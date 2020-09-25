from ._serialisation import deserialise, serialise
from ._utils import _gen_id, _task_type_for
from ._types import _GroupEntry
from ._pipeline import _Pipeline
from ._context import _TaskContext
from .messages_pb2 import TaskRequest, TaskResult, TaskException

from statefun.request_reply import BatchContext
from typing import Union, Tuple, Iterable
import logging
import traceback as tb
import inspect
import ast


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

    def bind(self, content_type=None, namespace=None, worker_name=None):
        def wrapper(function):

            def defaults():
                return {
                    'content_type': self._default_content_type if content_type is None else content_type,
                    'namespace': self._default_namespace if namespace is None else namespace,
                    'worker_name': self._default_worker_name if worker_name is None else worker_name
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

    def _get_pipeline(self, task_id, state):
        pipeline = state.get('pipeline', None)

        if pipeline is not None:
            return _Pipeline(pipeline=pipeline)
        else:
            raise ValueError(f'Missing pipleline for task_id - {task_id}')


    def _resume_pipeline(self, context, task_id, caller_id, state, task_result):
        pipeline = self._get_pipeline(task_id, state)
        pipeline.resume(context, caller_id, state, task_result)

    def _terminate_pipeline(self, context, task_id, caller_id, state, task_exception):
        pipeline = self._get_pipeline(task_id, state)
        pipeline.terminate(context, caller_id, task_exception)


    def _run(self, context: _TaskContext, task_request_or_result: Union[TaskRequest, TaskResult, TaskException]):   

        task_id = context.get_task_id()
        caller_id = context.get_caller_id()
        task_type = task_request_or_result.type
        
        _log.info(f'Started {task_type}[{task_id}], caller: {caller_id})')

        # get existing state
        state = context.get_state()

        if isinstance(task_request_or_result, TaskResult):
            # we resume the pipeline passing this result into the next task
            task_result = task_request_or_result
            self._resume_pipeline(context, task_id, caller_id, state, task_result)

        elif isinstance(task_request_or_result, TaskException):
            # we terminate the pipeline returning the exception to the final task
            task_exception = task_request_or_result
            self._terminate_pipeline(context, task_id, caller_id, state, task_exception)

        else:
            task_request = task_request_or_result
            context.pack_and_save('task_request', task_request)

            # get the flink task to run
            flink_task = self.get_task(task_request.type)

            # run the flink task
            task_result, task_exception, pipeline, extra_args = flink_task.run(context, task_request)

            if pipeline is not None:
                pipeline.resolve(context, extra_args)
            else:

                if task_exception is not None:
                    context.pack_and_save('task_exception', task_exception)
                    return_value = task_exception
                else:
                    context.pack_and_save('task_result', task_result)
                    return_value = task_result
 
                # then either we need to reply to a caller
                if caller_id is not None:
                    context.pack_and_reply(return_value)

                # or we need to send some egress
                elif task_request.reply_topic is not None and task_request.reply_topic != "":
                    context.pack_and_send_egress(topic=task_request.reply_topic, value=return_value)

    def run(self, context: BatchContext, task_request_or_result: Union[TaskRequest, TaskResult, TaskException]):
        with _TaskContext(context, self._egress_type_name) as task_context:
            self._run(task_context, task_request_or_result)


class _FlinkTask(object):
    def __init__(self, fun, content_type, **kwargs):
        self._fun = fun
        self._content_type = content_type

        self._args = inspect.getfullargspec(fun).args
        self._num_args = len(self._args)
        self._explicit_return = any(isinstance(node, ast.Return) for node in ast.walk(ast.parse(inspect.getsource(fun))))
        
    def _is_args_and_kwargs(self, test_args):
        try:
            args, kwargs = test_args
            return isinstance(kwargs, dict)
        except:
            return False

    def _to_args_and_kwargs(self, task_request: TaskRequest):
            task_data = deserialise(task_request)

            if self._is_args_and_kwargs(task_data):
                return task_data
            
            elif task_data is None:
                return (), {}
            else:
                return (task_data), {}

    def run(self, context: _TaskContext, task_request: TaskRequest):
        task_result, task_exception, pipeline, extra_args = None, None, None, None

        try:
            # run the flink task
            fn_result = self._run(context, task_request)

            # result of the task might be a Flink pipeline or tuple of Flink pipline + extra args to be passed through
            possible_pipeline_result = fn_result[0] if isinstance(fn_result, Tuple) else fn_result

            # in which case return the pipeline and these extra args (if present).
            if isinstance(possible_pipeline_result, _Pipeline):
                pipeline = possible_pipeline_result
                extra_args = fn_result[1:] if isinstance(fn_result, Tuple) and len(fn_result) > 1 else () 

            # else return the result of the task
            else:

                if not isinstance(fn_result, Tuple):  # if single result then wrap in tuple as this is the maximal case
                    fn_result = (fn_result,)

                task_result = TaskResult(
                    id=_gen_id(), 
                    correlation_id=task_request.id,
                    type=f'{task_request.type}.result')
                
                serialise(task_result, fn_result, content_type=self._content_type)

        # we errored so return a task_exception instead
        except Exception as e:
            task_exception = TaskException(
                id=_gen_id(),
                correlation_id=task_request.id,
                type=f'{task_request.type}.error',
                exception_type=type(e).__name__,
                exception_message=str(e),
                stacktrace=tb.format_exc())

        return task_result, task_exception, pipeline, extra_args

    def _run(self, context: _TaskContext, task_request: TaskRequest):
        args, kwargs = self._to_args_and_kwargs(task_request)
        
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

        result = self._fun(*task_args, **kwargs)

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
