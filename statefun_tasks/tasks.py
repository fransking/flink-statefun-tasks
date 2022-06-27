from statefun_tasks.context import TaskContext
from statefun_tasks.pipeline_builder import PipelineBuilder
from statefun_tasks.utils import _is_tuple, _type_name, _annotated_protos_for
from statefun_tasks.messages_pb2 import TaskRequest
from statefun_tasks.type_helpers import _create_task_result, _create_task_exception
from statefun_tasks.events import EventHandlers
from statefun_tasks.types import YieldTaskInvocation

from typing import AsyncGenerator, Generator
import inspect
import asyncio


class FlinkTask(object):
    def __init__(self, fun, serialiser, events:EventHandlers, retry_policy=None, with_state=False, is_fruitful=True, with_context=False, **kwargs):
        self._fun = fun
        self._serialiser = serialiser
        self._retry_policy = retry_policy
        self._with_state = with_state
        self._is_fruitful = is_fruitful
        self._with_context = with_context
        self._events = events

        full_arg_spec = inspect.getfullargspec(fun)
        self._args = full_arg_spec.args
        self._default_args = full_arg_spec.defaults
        self._num_args = len(self._args)
        self._accepts_varargs = full_arg_spec.varargs is not None
        self.is_async = inspect.iscoroutinefunction(fun)

        # register any annotated proto types for fn
        proto_types = _annotated_protos_for(fun)
        self._serialiser.register_proto_types(proto_types)

    @property
    def function(self):
        """
        The Python function that this FlinkTask wraps
        """
        return self._fun

    async def run(self, task_context: TaskContext, task_request: TaskRequest):
        task_result, task_exception, pipeline = None, None, None

        task_args, kwargs, fn_state = self._to_task_args_and_kwargs(task_context, task_request)

        # run the flink task
        try:
            fn_result = self._fun(*task_args, **kwargs)
            
            # convert generators to lists
            if isinstance(fn_result, AsyncGenerator):
                fn_result = list([x async for x in fn_result])

            elif isinstance(fn_result, Generator):
                fn_result = list([x for x in fn_result])

            # await coros
            elif asyncio.iscoroutine(fn_result):
                fn_result = await fn_result

            pipeline, task_result, fn_state = self._to_pipeline_or_task_result(task_context, task_request, fn_result, fn_state)

        
        except YieldTaskInvocation:
            # task yielded so we don't output anything
            ()

        # we errored so return a task_exception instead
        except Exception as e:
            task_exception = self._to_task_exception(task_request, e)

        return task_result, task_exception, pipeline, fn_state

    def _to_pipeline_or_task_result(self, task_context, task_request, fn_result, fn_state):
        pipeline, task_result, is_fruitful = None, None, self._is_fruitful

        if is_fruitful and task_request.HasField('is_fruitful'):
            is_fruitful = task_request.is_fruitful

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
            pipeline = fn_result.set_task_defaults(
                default_namespace=task_context.get_namespace(), 
                default_worker_name=task_context.get_worker_name()).\
            to_pipeline(serialiser=self._serialiser, is_fruitful=is_fruitful, events=self._events)
            fn_result = ()

        # drop the result if the task is marked as not fruitful or caller has asked for the result to be dropped
        if not is_fruitful:
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
                default_arg_index = len(self._default_args) - len(self._args) + idx
                if default_arg_index < 0:
                    raise ValueError('Not enough args supplied')
                resolved_args.append(self._default_args[default_arg_index])
            else:
                if len(args) == 0:
                    raise ValueError('Not enough args supplied')
                resolved_args.append(args.pop(0))
        if self._accepts_varargs:
            resolved_args.extend(args)
        elif len(args) > 0:
            raise ValueError('Too many args supplied')

        return resolved_args, kwargs, state
