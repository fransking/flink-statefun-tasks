from ._serialisation import deserialise, serialise
from ._utils import _gen_id, _task_type_for, _to_args_and_kwargs
from ._types import _GroupEntry, TaskRetryPolicy
from ._pipeline import _Pipeline
from ._context import _TaskContext
from .messages_pb2 import TaskRequest, TaskResult, TaskException

from statefun.request_reply import BatchContext
from typing import Union, Tuple, Iterable
import logging
import traceback as tb
import inspect
import ast
import asyncio


_log = logging.getLogger('FlinkTasks')


class FlinkTasks(object):
    def __init__(self, default_namespace: str, default_worker_name: str, egress_type_name: str, default_content_type='application/json'):
        self._default_namespace = default_namespace
        self._default_worker_name = default_worker_name
        self._egress_type_name = egress_type_name
        self._default_content_type = default_content_type
        self._bindings = {}

    def register(self, fun, **params):
        if fun is None:
            raise ValueError("function instance must be provided")

        self._bindings[_task_type_for(fun)] = _FlinkTask(fun, **params)

    def bind(self, content_type:str=None, namespace:str=None, worker_name:str=None, retry_policy:TaskRetryPolicy=None):
        def wrapper(function):

            def defaults():
                return {
                    'content_type': self._default_content_type if content_type is None else content_type,
                    'namespace': self._default_namespace if namespace is None else namespace,
                    'worker_name': self._default_worker_name if worker_name is None else worker_name,
                    'retry_policy': retry_policy
                }

            def send(*args, **kwargs):
                return _Pipeline(fun=function).send(*args, **kwargs).set(**defaults())

            function.defaults = defaults
            function.send = send

            self.register(function, **defaults())
            return function

        return wrapper

    def get_task(self, task_type):
        if task_type in self._bindings:
            return self._bindings[task_type]
        else:
            raise ValueError(f'{task_type} is not a registered FlinkTask')

    def run(self, context: BatchContext, task_input: Union[TaskRequest, TaskResult, TaskException]):
        with _TaskContext(context, task_input.type, self._egress_type_name) as task_context:

            # either we resume a pipeline (received TaskResult or TaskException) or we invoke a task (received TaskRequest)
            task_request_result = self._begin_operation(task_context, task_input, is_async=False)

            if task_request_result is not None:  # invoked a task
                task_request, task_result = task_request_result

                self._finalise_task_result(task_context, task_request, task_result)

    async def run_async(self, context: BatchContext, task_input: Union[TaskRequest, TaskResult, TaskException]):
        with _TaskContext(context, task_input.type, self._egress_type_name) as task_context:

            # either we resume a pipeline (received TaskResult or TaskException) or we invoke a task (received TaskRequest)
            task_request_result = self._begin_operation(task_context, task_input, is_async=True)

            if task_request_result is not None:  # invoked a task which may be a coro
                task_request, task_result = task_request_result

                if asyncio.iscoroutine(task_result):
                    task_result = await task_result

                self._finalise_task_result(task_context, task_request, task_result)

    def _begin_operation(self, task_context, task_request_or_result, is_async):
        _log.info(f'Started {task_context}')

        if isinstance(task_request_or_result, (TaskResult, TaskException)):
            # we resume the pipeline passing this result into the next task
            task_result_or_exception = task_request_or_result
            self._resume_pipeline(task_context, task_result_or_exception)
        else:
            # otherwise we invoke the task
            task_request = task_request_or_result
            task_context.pack_and_save('task_request', task_request)

            flink_task = self.get_task(task_request.type)
            fn = flink_task.run_async if is_async else flink_task.run

            return task_request, fn(task_context, task_request)

    def _get_pipeline(self, context):
        state = context.get_state()
        pipeline = state.get('pipeline', None)

        if pipeline is not None:
            return _Pipeline(pipeline=pipeline)
        else:
            raise ValueError(f'Missing pipleline for task_id - {context.get_task_id()}')

    def _resume_pipeline(self, context, task_result_or_exception: Union[TaskResult, TaskException]):
        pipeline = self._get_pipeline(context)

        # either we resume the pipeline or terminate it if we received an exception instead of a result
        if isinstance(task_result_or_exception, TaskResult):
            task_result = task_result_or_exception
            pipeline.resume(context, task_result)
        else:
            task_exception = task_result_or_exception

            # retry if requested
            if task_exception.retry:
                
                # attempt retry - will return false if retry count exceeded
                if pipeline.attempt_retry(context, task_result_or_exception):
                    return

            pipeline.terminate(context, task_result_or_exception)

    def _finalise_task_result(self, context, task_request, task_result):

        task_result, task_exception, pipeline, extra_args = task_result  # unpack

        if pipeline is not None:
            context.pack_and_save('task_result', task_result)  # task result will be the pipeline in json format
            pipeline.resolve(context, extra_args)
        else:

            if task_exception is not None:
                context.pack_and_save('task_exception', task_exception)
                return_value = task_exception
            else:
                context.pack_and_save('task_result', task_result)
                return_value = task_result

            # then either we need to reply to a caller
            if context.get_caller_id() is not None:
                context.pack_and_reply(return_value)

            # or we need to send some egress
            elif task_request.reply_topic is not None and task_request.reply_topic != "":
                context.pack_and_send_egress(topic=task_request.reply_topic, value=return_value)



class _FlinkTask(object):
    def __init__(self, fun, content_type, retry_policy=None, **kwargs):
        self._fun = fun
        self._content_type = content_type
        self._retry_policy = retry_policy

        self._args = inspect.getfullargspec(fun).args
        self._num_args = len(self._args)
        self._explicit_return = any(isinstance(node, ast.Return) for node in ast.walk(ast.parse(inspect.getsource(fun))))
        

    def run(self, context: _TaskContext, task_request: TaskRequest):
        task_result, task_exception, pipeline, extra_args = None, None, None, None

        try:
            # run the flink task
            task_args, kwargs, pass_through_args = self._to_task_args_and_kwargs(task_request)

            result = self._fun(*task_args, **kwargs)
            
            fn_result = self._add_passthrough_args(result, pass_through_args)

            pipeline, task_result, extra_args = self._to_pipeline_or_task_result(task_request, fn_result, extra_args)

        # we errored so return a task_exception instead
        except Exception as e:
            task_exception = self._to_task_exception(task_request, e)

        return task_result, task_exception, pipeline, extra_args

    async def run_async(self, context: _TaskContext, task_request: TaskRequest):
        task_result, task_exception, pipeline, extra_args = None, None, None, None

        try:
            # run the flink task
            task_args, kwargs, pass_through_args = self._to_task_args_and_kwargs(task_request)

            result = self._fun(*task_args, **kwargs)

            # await coro
            if asyncio.iscoroutine(result):
                result = await result
            
            fn_result = self._add_passthrough_args(result, pass_through_args)

            pipeline, task_result, extra_args = self._to_pipeline_or_task_result(task_request, fn_result, extra_args)

        # we errored so return a task_exception instead
        except Exception as e:
            task_exception = self._to_task_exception(task_request, e)

        return task_result, task_exception, pipeline, extra_args


    def _to_pipeline_or_task_result(self, task_request, fn_result, extra_args):
        pipeline, task_result = None, None

        if not isinstance(fn_result, Tuple):  # if single result then wrap in tuple as this is the maximal case
            fn_result = (fn_result,)

        # result of the task might be a Flink pipeline or tuple of Flink pipline + extra args to be passed through
        # in which case return the pipeline and these extra args (if present).
        if isinstance(fn_result[0], _Pipeline):
            pipeline = fn_result[0]
            extra_args = fn_result[1:] if isinstance(fn_result, Tuple) and len(fn_result) > 1 else ()
            fn_result = (pipeline.to_json_dict(verbose=True), *extra_args)

        task_result = TaskResult(
            id=_gen_id(), 
            correlation_id=task_request.id,
            type=f'{task_request.type}.result')
        
        serialise(task_result, fn_result, content_type=self._content_type)

        return pipeline, task_result, extra_args

    def _to_task_exception(self, task_request, ex):
        retry = False

        if self._retry_policy is not None:
            retry = any([isinstance(ex, ex_type) for ex_type in self._retry_policy.retry_for])

        task_exception = TaskException(
                id=_gen_id(),
                correlation_id=task_request.id,
                type=f'{task_request.type}.error',
                exception_type=type(ex).__name__,
                exception_message=str(ex),
                stacktrace=tb.format_exc(),
                retry=retry)

        if retry:
            task_exception.original_request.CopyFrom(task_request)

        return task_exception

    def _to_task_args_and_kwargs(self, task_request):
        args, kwargs = _to_args_and_kwargs(deserialise(task_request))
        
        #listify
        args = [arg for arg in args]

        # merge in args passed as kwargs e.g. fun1.continue_with(fun2, arg1=a, arg2=b)
        args_in_kwargs = [(idx, arg, kwargs[arg]) for idx, arg in enumerate(self._args) if arg in kwargs]
        for idx, arg, val in args_in_kwargs:
            args.insert(idx, val)
            del kwargs[arg]

        # pass through any extra args we might have
        if len(args) > self._num_args:
            task_args = args[0: self._num_args]
            pass_through_args = args[len(task_args):]
        else:
            task_args = args
            pass_through_args = ()

        return task_args, kwargs, pass_through_args

    def _add_passthrough_args(self, result, pass_through_args):
        if len(pass_through_args) > 0:

            if self._explicit_return:
                return_value = result, *pass_through_args
            else:
                return_value = *pass_through_args,
            
            return return_value
        else:
            return result


def in_parallel(group: list):
    parallel_pipeline = _GroupEntry(_gen_id())

    for pipeline in group:
        pipeline.add_to_group(parallel_pipeline)

    return _Pipeline(pipeline=[parallel_pipeline])
