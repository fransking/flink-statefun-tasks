from ._serialisation import DefaultSerialiser
from ._utils import _gen_id, _task_type_for, _is_tuple, _type_name, _annotated_protos_for
from ._types import RetryPolicy
from ._pipeline import _Pipeline, PipelineBuilder
from ._context import _TaskContext
from ._builtins import run_pipeline
from .messages_pb2 import TaskRequest, TaskResult, TaskException

from statefun.request_reply import BatchContext
from typing import Union
from functools import partial
import logging
import traceback as tb
import inspect
import asyncio

_log = logging.getLogger('FlinkTasks')


def _create_task_exception(task_request, ex, retry=False):
    task_exception = TaskException(
        id=_gen_id(),
        correlation_id=task_request.id,
        type=f'{task_request.type}.error',
        exception_type=_type_name(ex),
        exception_message=str(ex),
        stacktrace=tb.format_exc(),
        retry=retry)
    
    task_exception.state.CopyFrom(task_request.state)
    return task_exception


class FlinkTasks(object):
    def __init__(self, default_namespace: str = None, default_worker_name: str = None, egress_type_name: str = None, serialiser = None):
        self._default_namespace = default_namespace
        self._default_worker_name = default_worker_name
        self._egress_type_name = egress_type_name
        self._serialiser = serialiser if serialiser is not None else DefaultSerialiser()

        self._bindings = {}

        self.register_builtin('run_pipeline', partial(run_pipeline, serialiser=self._serialiser))

    def register_builtin(self, type_name, fun, **params):
        self._bindings[f'__builtins.{type_name}'] = _FlinkTask(fun, self._serialiser, **params)

    def register(self, fun, module_name=None, **params):
        if fun is None:
            raise ValueError("function instance must be provided")

        fun.type_name = _task_type_for(fun, module_name)
        self._bindings[fun.type_name] = _FlinkTask(fun, self._serialiser, **params)

    def bind(self, namespace: str = None, worker_name: str = None, retry_policy: RetryPolicy = None,  with_state: bool = False, is_fruitful: bool = True, module_name: str = None):
        """Binds a python function as a Flink Statefun Task

        Parameters:
        namespace (optional) (str): Statefun namespace to use in place of the default
        worker_name (optional) (str): Statefun worker to use in place of the default
        retry_policy (optional) (RetryPolicy): RetryPolicy to use should the task throw an exception
        with_state (optional defaults to false) (bool): Whether to pass a state object as the first parameter (an expect back a tuple of (state, result,)
        is_fruitful (optional defaults to true): Whether the function produces a fruitful result or simply returns None
        module_name (optional) (str): If specified then the task type will be module_name.function_name otherwise the Python module containing the function will be used 

        """
        def wrapper(function):
            def defaults():
                return {
                    'namespace': self._default_namespace if namespace is None else namespace,
                    'worker_name': self._default_worker_name if worker_name is None else worker_name,
                    'retry_policy': None if retry_policy is None else retry_policy.to_proto(),
                    'with_state': with_state,
                    'is_fruitful': is_fruitful,
                    'module_name': module_name
                }

            def send(*args, **kwargs):
                return PipelineBuilder().send(function, *args, **kwargs)

            function.defaults = defaults
            function.send = send

            self.register(function, **defaults())
            return function

        return wrapper

    def get_task(self, task_type):
        if task_type in self._bindings:
            return self._bindings[task_type]
        else:
            raise RuntimeError(f'{task_type} is not a registered FlinkTask')

    def is_async_required(self, task_input: Union[TaskRequest, TaskResult, TaskException]):
        try:
            return isinstance(task_input, TaskRequest) and self.get_task(task_input.type).is_async
        except:
            return False

    def run(self, context: BatchContext, task_input: Union[TaskRequest, TaskResult, TaskException]):
        with _TaskContext(context, task_input.type, self._egress_type_name, self._serialiser) as task_context:
            try:
                _log.info(f'Started {task_context}')

                # either we resume a pipeline (received TaskResult or TaskException) or we invoke a task (received TaskRequest)
                task_request_result = self._begin_operation(task_context, task_input)

                if task_request_result is not None:  # invoked a task
                    task_request, task_result = task_request_result

                    self._finalise_task_result(task_context, task_request, task_result)
            except Exception as ex:
                self._fail(task_context, task_input, ex)
                raise
            finally:
                _log.info(f'Finished {task_context}')

    async def run_async(self, context: BatchContext, task_input: Union[TaskRequest, TaskResult, TaskException]):
        with _TaskContext(context, task_input.type, self._egress_type_name, self._serialiser) as task_context:
            try:
                _log.info(f'Started {task_context}')

                # either we resume a pipeline (received TaskResult or TaskException) or we invoke a task (received TaskRequest)
                task_request_result = self._begin_operation(task_context, task_input)

                if task_request_result is not None:  # invoked a task which may be a coro
                    task_request, task_result = task_request_result

                    if asyncio.iscoroutine(task_result):
                        task_result = await task_result

                    self._finalise_task_result(task_context, task_request, task_result)
            except Exception as ex:
                self._fail(task_context, task_input, ex)
                raise
            finally:
                _log.info(f'Finished {task_context}')

    @staticmethod
    def send(func, *args, **kwargs) -> PipelineBuilder:
        try:
            send_func = func.send
        except AttributeError:
            raise AttributeError(
                'Expected function to have a send attribute. Make sure it is decorated with @tasks.bind()')
        return send_func(*args, **kwargs)

    def _begin_operation(self, task_context, task_request_or_result):
        if isinstance(task_request_or_result, (TaskResult, TaskException)):
            # we resume the pipeline passing this result into the next task
            task_result_or_exception = task_request_or_result
            self._resume_pipeline(task_context, task_result_or_exception)
        else:
            # otherwise we invoke the task
            task_request = task_request_or_result
            task_context.pack_and_save('task_request', task_request)

            flink_task = self.get_task(task_request.type)
            fn = flink_task.run_async if flink_task.is_async else flink_task.run

            return task_request, fn(task_request)

    def _get_pipeline(self, context):
        state = context.get_state()
        pipeline_protos = state.get('pipeline', None)

        if pipeline_protos is not None:
            return _Pipeline.from_proto(pipeline_protos, self._serialiser)
        else:
            raise ValueError(f'Missing pipleline for task_id - {context.get_task_id()}')

    def _resume_pipeline(self, context, task_result_or_exception: Union[TaskResult, TaskException]):
        pipeline = self._get_pipeline(context)

        if isinstance(task_result_or_exception, TaskException):
            task_exception = task_result_or_exception

            # retry if requested
            if task_exception.retry:

                # attempt retry - will return false if retry count exceeded
                if pipeline.attempt_retry(context, task_exception):
                    return

        pipeline.resume(context, task_result_or_exception)

    def _emit_result(self, context, task_request, task_result):
        # either send a message to egress if reply_topic was specified
        if task_request.HasField('reply_topic'):
            context.pack_and_send_egress(topic=task_request.reply_topic, value=task_result)

        # or call back to a particular flink function if reply_address was specified
        elif task_request.HasField('reply_address'):
            address, identifer = context.to_address_and_id(task_request.reply_address)
            context.pack_and_send(address, identifer, task_result)

        # or call back to our caller (if there is one)
        elif context.get_caller_id() is not None:
            context.pack_and_reply(task_result)

    def _finalise_task_result(self, context, task_request, task_result):

        task_result, task_exception, pipeline = task_result  # unpack

        if pipeline is not None:
            context.pack_and_save('task_result', task_result)
            pipeline.begin(context)
        else:

            if task_exception is not None:
                context.pack_and_save('task_exception', task_exception)
                return_value = task_exception
            else:
                context.pack_and_save('task_result', task_result)
                return_value = task_result

            # emit the result - either by replying to caller or sending some egress
            self._emit_result(context, task_request, return_value)

    def _fail(self, context, task_request, ex):
        task_exception = _create_task_exception(task_request, ex, retry=False)
        context.pack_and_save('task_exception', task_exception)

        # emit the error - either by replying to caller or sending some egress
        self._emit_result(context, task_request, task_exception)


class _FlinkTask(object):
    def __init__(self, fun, serialiser, retry_policy=None, with_state=False, is_fruitful=True, **kwargs):
        self._fun = fun
        self._serialiser = serialiser
        self._retry_policy = retry_policy
        self._with_state = with_state
        self._is_fruitful = is_fruitful

        full_arg_spec = inspect.getfullargspec(fun)
        self._args = full_arg_spec.args
        self._num_args = len(self._args)
        self._accepts_varargs = full_arg_spec.varargs is not None
        self.is_async = inspect.iscoroutinefunction(fun)

        # register any annotated proto types for fn
        proto_types = _annotated_protos_for(fun)
        self._serialiser.register_proto_types(proto_types)

    def run(self, task_request: TaskRequest):
        task_result, task_exception, pipeline = None, None, None

        try:
            # run the flink task
            task_args, kwargs, original_state = self._to_task_args_and_kwargs(task_request)

            fn_result = self._fun(*task_args, **kwargs)

            pipeline, task_result = self._to_pipeline_or_task_result(task_request, fn_result, original_state)
            
        # we errored so return a task_exception instead
        except Exception as e:
            task_exception = self._to_task_exception(task_request, e)

        return task_result, task_exception, pipeline

    async def run_async(self, task_request: TaskRequest):
        task_result, task_exception, pipeline = None, None, None

        try:
            # run the flink task
            task_args, kwargs, original_state = self._to_task_args_and_kwargs(task_request)

            fn_result = self._fun(*task_args, **kwargs)

            # await coro
            if asyncio.iscoroutine(fn_result):
                fn_result = await fn_result

            pipeline, task_result = self._to_pipeline_or_task_result(task_request, fn_result, original_state)

        # we errored so return a task_exception instead
        except Exception as e:
            task_exception = self._to_task_exception(task_request, e)

        return task_result, task_exception, pipeline

    def _to_pipeline_or_task_result(self, task_request, fn_result, original_state):
        pipeline, task_result, fn_state = None, None, original_state

        if not self._is_fruitful:
            fn_result = ()

        else:
            # if single result then wrap in tuple as this is the maximal case
            if not _is_tuple(fn_result):  
                fn_result = (fn_result,)

            # if this task accesses state then we expect the first element in the result tuple
            # to be the mutated state and the task results to be remainder
            if self._with_state:
                fn_state = fn_result[0]
                fn_result = fn_result[1:] if len(fn_result) > 1 else ()

            # if a single element tuple then unpack back to single value
            # so (8,) becomes 8 but (8,9) remains a tuple
            fn_result = fn_result[0] if len(fn_result) == 1 else fn_result

            # result of the task might be a Flink pipeline
            if isinstance(fn_result, PipelineBuilder):
                pipeline = fn_result.to_pipeline()
                fn_result = fn_result.to_proto(self._serialiser)


        task_result = TaskResult(
            id=_gen_id(),
            correlation_id=task_request.id,
            type=f'{task_request.type}.result')

        self._serialiser.serialise_result(task_result, fn_result, fn_state)

        return pipeline, task_result

    def _to_task_exception(self, task_request, ex):
        retry = False

        if self._retry_policy is not None:
            ex_class_hierarchy = [_type_name(ex) for ex in inspect.getmro(ex.__class__)]
            retry = any([ex_type for ex_type in self._retry_policy.retry_for if ex_type in ex_class_hierarchy])

        task_exception = _create_task_exception(task_request, ex, retry=retry)

        if retry:
            task_exception.retry_request.CopyFrom(task_request)

        return task_exception

    def _to_task_args_and_kwargs(self, task_request):
        args, kwargs, state = self._serialiser.deserialise_request(task_request)

        # listify
        if _is_tuple(args):
            args = [arg for arg in args]
        else:
            args = [args]

        # merge in args passed as kwargs e.g. fun1.continue_with(fun2, arg1=a, arg2=b)
        args_in_kwargs = [(idx, arg, kwargs[arg]) for idx, arg in enumerate(self._args) if arg in kwargs]
        for idx, arg, val in args_in_kwargs:
            args.insert(idx, val)
            del kwargs[arg]

        # add state as first argument if required by this task
        if self._with_state:
            args = [state] + args

        return args, kwargs, state


def in_parallel(entries: list):
    return PipelineBuilder().append_group(entries)
