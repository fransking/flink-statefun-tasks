from ._serialisation import DefaultSerialiser
from ._utils import _gen_id, _task_type_for, _is_tuple, _type_name, _annotated_protos_for
from ._types import RetryPolicy, TaskAlreadyExistsException
from ._pipeline import _Pipeline, PipelineBuilder
from ._context import TaskContext
from ._builtins import run_pipeline
from ._protobuf import _pack_any
from ._task_actions import _invoke_task_action
from .messages_pb2 import TaskRequest, TaskResult, TaskException, TaskActionRequest, TaskActionResult, TaskActionException, \
    TaskAction, TaskStatus

from statefun.request_reply import BatchContext
from datetime import timedelta
from typing import Union
import logging
import traceback as tb
import inspect
import asyncio


_log = logging.getLogger('FlinkTasks')


def _task_name(task_input):
    if isinstance(task_input, TaskActionRequest):
        return f'Action [{TaskAction.Name(task_input.action)}]'
    else:
        return task_input.type


def _create_task_exception(task_input, ex):
    if isinstance(task_input, TaskActionRequest):
        return TaskActionException(
            id=task_input.id,
            action = task_input.action,
            exception_type=_type_name(ex),
            exception_message=str(ex),
            stacktrace=tb.format_exc())
    else:
        task_exception = TaskException(
            id=task_input.id,
            type=f'{_task_name(task_input)}.error',
            exception_type=_type_name(ex),
            exception_message=str(ex),
            stacktrace=tb.format_exc())

        # if the task failed then ensure that exception retains the state from the task input (i.e. the TaskRequest)
        if isinstance(task_input, TaskRequest) and task_input.HasField('state'):
            task_exception.state.CopyFrom(task_input.state)

        return task_exception


def _create_task_result(task_input, result=None):
    if isinstance(task_input, TaskActionRequest):
        task_result = TaskActionResult(
            id=task_input.id,
            action = task_input.action
        )
    else:
        task_result = TaskResult(
            id=task_input.id,
            type=f'{_task_name(task_input)}.result')

    if result is not None:
        task_result.result.CopyFrom(_pack_any(result))

    return task_result



class FlinkTasks(object):
    """
    Flink Tasks implementation

    An instance should be instantiated at the top of your api file and used to decorate functions
    to be exposed as tasks.

    :param default_namespace: namespace to expose functions under. Maps to Flink Statefun function namespace in module.yaml
    :param default_worker_name: worker name to expose.  Maps to Flink Statefun function type in module.yaml
    :param egress_type_name: egress type name.  Maps to Flink Statefun egress in module.yaml
    :param optional serialiser: serialiser to use (will use DefaultSerialiser if not set)
    """
    def __init__(self, default_namespace: str = None, default_worker_name: str = None, egress_type_name: str = None, serialiser = None):
        self._default_namespace = default_namespace
        self._default_worker_name = default_worker_name
        self._egress_type_name = egress_type_name
        self._serialiser = serialiser if serialiser is not None else DefaultSerialiser()

        self._bindings = {}

        self.register_builtin('run_pipeline', run_pipeline)

    def register_builtin(self, type_name, fun, **params):
        """
        Registers a built in Flink Task e.g. __builtins.run_pipeline
        This should only be used if you want to provide your own built in (pre-defined) tasks

        :param type_name: task type to expose the built in as
        :param fun: a function, partial or lambda representing the built in
        :param params: any additional parameters to the built in
        """
        self._bindings[f'__builtins.{type_name}'] = _FlinkTask(fun, self._serialiser, **params)

    def register(self, fun, wrapper=None, module_name=None, **params):
        """
        Registers a Python function as a Flink Task.
        Normally you would attribute the function with @tasks.bind() instead

        :param fun: the python function
        :param optional wrapper: if wrapping a task function with e.g. functools.wraps then pass the wrapper here
        :param optional module_name: the module name to register the task under which by default is the Python module name containing the function
        :param params: any additional parameters to the Flink Task (such as a retry policy)
        """

        if fun is None:
            raise ValueError("function instance must be provided")

        fun.type_name = _task_type_for(fun, module_name)
        self._bindings[fun.type_name] = _FlinkTask(wrapper or fun, self._serialiser, **params)

    def bind(self, namespace: str = None, worker_name: str = None, retry_policy: RetryPolicy = None,  with_state: bool = False, is_fruitful: bool = True, module_name: str = None, with_context: bool = False):
        """
        Binds a function as a Flink Task

        :param namespace: namespace to use in place of the default
        :param worker_name: worker name to use in place of the default
        :param retry_policy: retry policy to use should the task throw an exception
        :param with_state: whether to pass a state object as the first (second if with_context is also set) parameter.  The return value should be a tuple of state, result (default False)
        :param is_fruitful: whether the function produces a fruitful result or simply returns None (default True)
        :param module_name: if specified then the task type used in addressing will be module_name.function_name
                            otherwise the Python module containing the function will be used
        :param with_context: whether to pass a Flink context object as the first parameter (default false)
        """
        def wrapper(function):
            def defaults():
                return {
                    'namespace': self._default_namespace if namespace is None else namespace,
                    'worker_name': self._default_worker_name if worker_name is None else worker_name,
                    'retry_policy': None if retry_policy is None else retry_policy.to_proto(),
                    'with_state': with_state,
                    'is_fruitful': is_fruitful,
                    'module_name': module_name,
                    'with_context': with_context
                }

            def send(*args, **kwargs):
                return PipelineBuilder().send(function, *args, **kwargs)

            function.defaults = defaults
            function.send = send

            self.register(function, **defaults())
            return function

        return wrapper

    def get_task(self, task_type):
        """
        Returns the Flink Task instance for a given task type name

        :param task_type: task type name e.g. examples.multiply
        :return: the Flink Task
        """
        if task_type in self._bindings:
            return self._bindings[task_type]
        else:
            raise RuntimeError(f'{task_type} is not a registered FlinkTask')

    def is_async_required(self, task_input: Union[TaskRequest, TaskResult, TaskException, TaskActionRequest]):
        """
        Checks if a task is declared as async or not

        :param task_input: the task input protobuf message
        :return: true if the task being called is declared as async otherwise false
        """

        try:
            return isinstance(task_input, TaskRequest) and self.get_task(task_input.type).is_async
        except:
            return False

    def run(self, context: BatchContext, task_input: Union[TaskRequest, TaskResult, TaskException, TaskActionRequest]):
        """
        Runs a non-async Flink Task

        :param context: context object provided by Flink
        :param task_input: the task input protobuf message
        """

        with TaskContext(context, _task_name(task_input), self._egress_type_name, self._serialiser) as task_context:
            try:
                _log.info(f'Started {task_context}')

                # we resume a pipeline (received TaskResult or TaskException) or we invoke a task (received TaskRequest)
                # or perform some action like return the status, pause, resume etc (received a TaskActionRequest)
                task_request_result = self._begin_operation(task_context, task_input)

                if task_request_result is not None:  # invoked a task
                    task_request, task_result = task_request_result

                    self._finalise_task_result(task_context, task_request, task_result)

                _log.info(f'Finished {task_context}')

            except Exception as ex:
                _log.error(f'Error invoking {task_context} - {ex}')
                self._fail(task_context, task_input, ex)

    async def run_async(self, context: BatchContext, task_input: Union[TaskRequest, TaskResult, TaskException, TaskActionRequest]):
        """
        Runs an async Flink Task

        :param context: context object provided by Flink
        :param task_input: the task input protobuf message
        """
        with TaskContext(context, _task_name(task_input), self._egress_type_name, self._serialiser) as task_context:
            try:
                _log.info(f'Started {task_context}')

                # we resume a pipeline (received TaskResult or TaskException) or we invoke a task (received TaskRequest)
                # or perform some action like return the status, pause, resume etc (received a TaskActionRequest)
                task_request_result = self._begin_operation(task_context, task_input)

                if task_request_result is not None:  # invoked a task which may be a coro
                    task_request, task_result = task_request_result

                    if asyncio.iscoroutine(task_result):
                        task_result = await task_result

                    self._finalise_task_result(task_context, task_request, task_result)

                _log.info(f'Finished {task_context}')

            except Exception as ex:
                _log.error(f'Error invoking {task_context} - {ex}')
                self._fail(task_context, task_input, ex)

    @staticmethod
    def send(func, *args, **kwargs) -> PipelineBuilder:
        """
        Returns a PipelineBuilder with this task as the first item in the pipeline

        :param func: a function decorated with @tasks.bind()
        :param args: task args
        :param kwargs: task kwargs
        :return: a pipeline builder
        """
        try:
            send_func = func.send
        except AttributeError:
            raise AttributeError(
                'Expected function to have a send attribute. Make sure it is decorated with @tasks.bind()')
        return send_func(*args, **kwargs)

    def _begin_operation(self, context, task_input):
        if isinstance(task_input, (TaskResult, TaskException)):
            # we resume the pipeline passing this result into the next task
            task_result_or_exception = task_input
            self._resume_pipeline(context, task_result_or_exception)

        elif isinstance(task_input, TaskActionRequest):
            # we are invoking some kind of action like pausing a pipeline or returning a task state
            task_action = task_input  # type: TaskAction
            self._invoke_action(context, task_action)

        elif isinstance(task_input, TaskRequest):
            # otherwise we invoke the task
            task_request = task_input  # type: TaskRequest
            context.apply_task_meta(task_request)
            return self._invoke_task(context, task_request)

        else:
            # we raise an error for an unsupport task_input type
            raise ValueError(f'Unexpected type for task_input: {type(task_input)}')

    def _get_pipeline(self, context):
        pipeline_protos = context.pipeline_state.pipeline

        if pipeline_protos is not None:
            return _Pipeline.from_proto(pipeline_protos, self._serialiser)
        else:
            raise ValueError(f'Missing pipleline for task_id - {context.get_task_id()}')

    def _resume_pipeline(self, context, task_result_or_exception: Union[TaskResult, TaskException]):
        pipeline = self._get_pipeline(context)
        pipeline.resume(context, task_result_or_exception)

    def _invoke_action(self, context, task_action):
        try:
            result = _invoke_task_action(context, task_action)
            if result is not None:
                self._emit_result(context, task_action, _create_task_result(task_action, result))

        except Exception as ex:
            self._emit_result(context, task_action, _create_task_exception(task_action, ex))

    def _invoke_task(self, context, task_request):
        if context.unpack('task_request', TaskRequest) is not None:
            # don't allow tasks to be overwritten
            raise TaskAlreadyExistsException(f'Task already exists: {task_request.id}')

        context.pack_and_save('task_request', task_request)

        flink_task = self.get_task(task_request.type)
        fn = flink_task.run_async if flink_task.is_async else flink_task.run

        return task_request, fn(context, task_request)

    def _emit_result(self, context, task_input, task_result):
        # either send a message to egress if reply_topic was specified
        if task_input.HasField('reply_topic'):
            context.pack_and_send_egress(topic=task_input.reply_topic, value=task_result)

        # or call back to a particular flink function if reply_address was specified
        elif task_input.HasField('reply_address'):
            address, identifer = context.to_address_and_id(task_input.reply_address)
            context.pack_and_send(address, identifer, task_result)

        elif isinstance(task_input, TaskRequest):
            # if this was a task request it could be part of a pipeline - i.e. called from another tasks so
            # either call back to original caller (e.g. if this is a result of a retry)
            if context.task_state.original_caller_id != '':
                context.pack_and_send(context.task_state.original_caller_address, context.task_state.original_caller_id, task_result)

            # or call back to our caller (if there is one)
            elif context.get_caller_id() is not None:
                context.pack_and_reply(task_result)

    def _finalise_task_result(self, context, task_request, result_tuple):
        task_result, task_exception, pipeline, task_state = result_tuple  # unpack

        if pipeline is not None and not pipeline.is_empty():
            pipeline.begin(context, task_request, task_state)
        else:
            if task_exception is not None:
                if self._attempt_retry(context, task_request, task_exception):
                    return  # we have triggered a retry so ignore the result of this invocation

                context.pack_and_save('task_exception', task_exception)
                return_value = task_exception
            else:
                context.pack_and_save('task_result', task_result)
                return_value = task_result

            # emit the result - either by replying to caller or sending some egress
            self._emit_result(context, task_request, return_value)

    def _attempt_retry(self, context, task_request, task_exception):
        if task_exception.maybe_retry and task_exception.retry_policy is not None:
            if context.task_state.retry_count >= task_exception.retry_policy.max_retries:
                return False

            # save the original caller address and id for this task_request prior to calling back to ourselves which would overwrite
            if context.task_state.original_caller_id == '':
                context.task_state.original_caller_id = context.get_caller_id()
                context.task_state.original_caller_address = context.get_caller_address()

            # remove previous task_request from state, increment retry count
            context.delete('task_request')
            context.task_state.retry_count += 1

            # send retry
            delay = timedelta(milliseconds=task_exception.retry_policy.delay_ms)

            if task_exception.retry_policy.exponential_back_off:
                delay = timedelta(milliseconds=task_exception.retry_policy.delay_ms ^ float(context.task_state.retry_count))

            if delay:
                context.pack_and_send_after(delay, context.get_address(), context.get_task_id(), task_request)
            else:
                context.pack_and_send(context.get_address(), context.get_task_id(), task_request)

            return True

        return False

    def _fail(self, context, task_input, ex):
        task_exception = _create_task_exception(task_input, ex)

        context.delete('task_result')
        context.pack_and_save('task_exception', task_exception)

        # emit the error - either by replying to caller or sending some egress
        self._emit_result(context, task_input, task_exception)


class _FlinkTask(object):
    def __init__(self, fun, serialiser, retry_policy=None, with_state=False, is_fruitful=True, with_context=False, **kwargs):
        self._fun = fun
        self._serialiser = serialiser
        self._retry_policy = retry_policy
        self._with_state = with_state
        self._is_fruitful = is_fruitful
        self._with_context = with_context

        full_arg_spec = inspect.getfullargspec(fun)
        self._args = full_arg_spec.args
        self._default_args = full_arg_spec.defaults
        self._num_args = len(self._args)
        self._accepts_varargs = full_arg_spec.varargs is not None
        self.is_async = inspect.iscoroutinefunction(fun)

        # register any annotated proto types for fn
        proto_types = _annotated_protos_for(fun)
        self._serialiser.register_proto_types(proto_types)

    def run(self, task_context: TaskContext, task_request: TaskRequest):
        task_result, task_exception, pipeline = None, None, None

        task_args, kwargs, fn_state = self._to_task_args_and_kwargs(task_context, task_request)

        try:
            # run the flink task
            fn_result = self._fun(*task_args, **kwargs)

            pipeline, task_result, fn_state = self._to_pipeline_or_task_result(task_request, fn_result, fn_state)

        # we errored so return a task_exception instead
        except Exception as e:
            task_exception = self._to_task_exception(task_request, e)

        return task_result, task_exception, pipeline, fn_state

    async def run_async(self, task_context: TaskContext, task_request: TaskRequest):
        task_result, task_exception, pipeline = None, None, None

        task_args, kwargs, fn_state = self._to_task_args_and_kwargs(task_context, task_request)

        try:
            # run the flink task
            fn_result = self._fun(*task_args, **kwargs)

            # await coro
            if asyncio.iscoroutine(fn_result):
                fn_result = await fn_result

            pipeline, task_result, fn_state = self._to_pipeline_or_task_result(task_request, fn_result, fn_state)

        # we errored so return a task_exception instead
        except Exception as e:
            task_exception = self._to_task_exception(task_request, e)

        return task_result, task_exception, pipeline, fn_state

    def _to_pipeline_or_task_result(self, task_request, fn_result, fn_state):
        pipeline, task_result = None, None

        # if single result then wrap in tuple as this is the maximal case
        if not _is_tuple(fn_result):
            fn_result = (fn_result,)

        # if this task accesses state then we expect the first element in the result tuple
        # to be the mutated state and the task results to be remainder
        if self._with_state:

            if len(fn_result) < 1:
                raise ValueError('Expecting a tuple with at least the state as the first element')

            fn_state = fn_result[0]
            fn_result = fn_result[1:] if len(fn_result) > 1 else ()

        # if a single element tuple remains then unpack back to single value
        # so (8,) becomes 8 but (8,9) remains a tuple
        fn_result = fn_result[0] if len(fn_result) == 1 else fn_result

        # result of the task might be a Flink pipeline
        if isinstance(fn_result, PipelineBuilder):
            # this new pipeline which once complete will yield the result of the whole pipeline
            # back to the caller as if it were a simple task
            pipeline = fn_result.to_pipeline(self._serialiser, is_fruitful=self._is_fruitful)
            fn_result = ()

        # drop the result if the task is marked as not fruitful
        if not self._is_fruitful:
            fn_result = ()

        task_result = _create_task_result(task_request)
        self._serialiser.serialise_result(task_result, fn_result, fn_state)

        return pipeline, task_result, fn_state

    def _to_task_exception(self, task_request, ex):
        # use retry policy on task request first then fallback to task definition
        task_retry_policy = task_request.retry_policy
        if not task_request.HasField('retry_policy'):
            task_retry_policy = self._retry_policy

        maybe_retry = False

        if task_retry_policy is not None:
            ex_class_hierarchy = [_type_name(ex) for ex in inspect.getmro(ex.__class__)]
            maybe_retry = any([ex_type for ex_type in task_retry_policy.retry_for if ex_type in ex_class_hierarchy])

        task_exception = _create_task_exception(task_request, ex)

        if maybe_retry:
            task_exception.maybe_retry = True
            task_exception.retry_policy.CopyFrom(task_retry_policy)

        return task_exception

    def _to_task_args_and_kwargs(self, task_context, task_request):
        args, kwargs, state = self._serialiser.deserialise_request(task_request)

        # listify
        if _is_tuple(args):
            args = [arg for arg in args]
        else:
            args = [args]

        # add state as first argument if required by this task
        if self._with_state:
            args.insert(0, state)
        # add context if required by this task
        if self._with_context:
            args.insert(0, task_context)

        resolved_args = []
        # merge in args passed as kwargs e.g. fun1.continue_with(fun2, arg1=a, arg2=b)
        for idx, arg in enumerate(self._args):
            if arg in kwargs:
                resolved_args.append(kwargs[arg])
                del kwargs[arg]
            elif self._default_args is not None and len(args) == 0:
                resolved_args.append(self._default_args[idx])
            else:
                resolved_args.append(args.pop(0))
        if self._accepts_varargs:
            resolved_args.extend(args)
        elif len(args) > 0:
            raise ValueError('Too many args supplied')

        return resolved_args, kwargs, state


def in_parallel(entries: list, max_parallelism=None):
    return PipelineBuilder().append_group(entries, max_parallelism)

