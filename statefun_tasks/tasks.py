from statefun_tasks.context import TaskContext
from statefun_tasks.pipeline_builder import PipelineBuilder
from statefun_tasks.utils import _is_tuple, _type_name, _annotated_protos_for
from statefun_tasks.messages_pb2 import TaskRequest
from statefun_tasks.type_helpers import _create_task_result, _create_task_exception

import inspect
import asyncio


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
        self._num_args = len(self._args)
        self._accepts_varargs = full_arg_spec.varargs is not None
        self.is_async = inspect.iscoroutinefunction(fun)

        # register any annotated proto types for fn
        proto_types = _annotated_protos_for(fun)
        self._serialiser.register_proto_types(proto_types)

    async def run(self, task_context: TaskContext, task_request: TaskRequest):
        task_result, task_exception, pipeline = None, None, None

        task_args, kwargs, fn_state = self._to_task_args_and_kwargs(task_context, task_request)

        # run the flink task
        try:
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

        # merge in args passed as kwargs e.g. fun1.continue_with(fun2, arg1=a, arg2=b)
        args_in_kwargs = [(idx, arg, kwargs[arg]) for idx, arg in enumerate(self._args) if arg in kwargs]
        for idx, arg, val in args_in_kwargs:
            args.insert(idx, val)
            del kwargs[arg]

        # add state as first argument if required by this task
        if self._with_state:
            args = [state] + args

        # add context if required by this task
        if self._with_context:
            args = [task_context] + args

        return args, kwargs, state
