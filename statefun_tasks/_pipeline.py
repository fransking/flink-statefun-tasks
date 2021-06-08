from ._utils import _gen_id, _task_type_for, _is_tuple
from ._serialisation import DefaultSerialiser
from ._types import Task, Group, RetryPolicy
from ._context import TaskContext
from ._pipeline_utils import _get_initial_tasks, _mark_task_complete, _get_next_step_in_pipeline, \
    _extend_args, _aggregate_group_results, _try_get_finally_task
from .messages_pb2 import TaskRequest, TaskResult, TaskException, Pipeline, TupleOfAny

from typing import Union, Iterable
from uuid import uuid4


class _Pipeline(object):
    def __init__(self, pipeline: list, serialiser=None):
        self._pipeline = pipeline
        self._serialiser = serialiser if serialiser is not None else DefaultSerialiser()

    def to_proto(self) -> Pipeline:
        pipeline = Pipeline(entries=[p.to_proto(self._serialiser) for p in self._pipeline])
        return pipeline

    @staticmethod
    def from_proto(pipeline_proto: Pipeline, serialiser):
        pipeline = []

        for proto in pipeline_proto.entries:
            if proto.HasField('task_entry'):
                pipeline.append(Task.from_proto(proto))
            elif proto.HasField('group_entry'):
                pipeline.append(Group.from_proto(proto))

        return _Pipeline(pipeline, serialiser)
        

    def begin(self, context: TaskContext, invoking_task: TaskRequest):
        caller_id = context.get_caller_id()

        # 1. record all the continuations into a pipeline and save into state with caller id and address
        context.pipeline_state.id = context.get_task_id()
        context.pipeline_state.address = context.get_address()
        context.pipeline_state.pipeline.CopyFrom(self.to_proto())

        if caller_id is not None:
            context.pipeline_state.caller_id = caller_id
            context.pipeline_state.caller_address = context.get_caller_address()

        # record the root pipeline details from the calling task into our pipeline state to aid in tracking nested pipelines
        context.pipeline_state.root_id = invoking_task.meta['root_pipeline_id'] or context.pipeline_state.id
        context.pipeline_state.root_address = invoking_task.meta['root_pipeline_address'] or context.pipeline_state.address

        # 2. get initial tasks(s) to call - might be single start of chain task or a group of tasks to call in parallel
        tasks = _get_initial_tasks(self._pipeline[0])

        # 3. call each task
        for task in tasks:

            request = TaskRequest(id=task.task_id, type=task.task_type)

            # set extra pipeline related parameters
            self._add_pipeline_meta(context, caller_id, request)

            self._serialiser.serialise_request(request, task.request, retry_policy=task.retry_policy)

            context.pack_and_send(task.get_destination(), task.task_id, request)

    def resume(self, context: TaskContext, task_result_or_exception: Union[TaskResult, TaskException]):
        caller_id = context.get_caller_id()
        task_results = context.pipeline_state.task_results

        # mark pipeline step as complete & record task result
        _mark_task_complete(caller_id, self._pipeline, task_result_or_exception, task_results)

        # get the next step of the pipeline to run (if any)
        _, next_step, group = _get_next_step_in_pipeline(caller_id, self._pipeline)

        # if the group is complete the create the aggregate results as a list
        if group is not None and group.is_complete():
            task_result_or_exception = _aggregate_group_results(group, task_results, self._serialiser)

        # if we got an exception then the next step is the finally_task if there is one (or none otherwise)
        if isinstance(task_result_or_exception, TaskException):
            next_step = _try_get_finally_task(caller_id, self._pipeline)

        # turn next step into remainder of tasks to call
        if isinstance(next_step, Task):
            remainder = [next_step]
            if next_step.is_finally:
                # record the result of the task prior to the finally task so we can return it once the finally task completes

                before_finally = context.pipeline_state.exception_before_finally \
                    if isinstance(task_result_or_exception, TaskException) \
                        else context.pipeline_state.result_before_finally

                before_finally.CopyFrom(task_result_or_exception)

        elif isinstance(next_step, Group):
            remainder = _get_initial_tasks(next_step)
        else:
            remainder = []

        # call next steps (if any)
        if any(remainder):
            task_result, task_state = self._serialiser.unpack_response(task_result_or_exception)

            for task in remainder:
                task_id = task.task_id
                task_args_and_kwargs = self._serialiser.to_args_and_kwargs(task.request)
                
                # extend task_result with any args & kwargs passed to the task explicitly
                # noting that args from previous tasks are not passed to finally
                task_request = self._serialiser.merge_args_and_kwargs(task_result if not task.is_finally else TupleOfAny(), task_args_and_kwargs)

                request = TaskRequest(id=task_id, type=task.task_type)

                # set extra pipeline related parameters
                self._add_pipeline_meta(context, caller_id, request)

                self._serialiser.serialise_request(request, task_request, state=task_state, retry_policy=task.retry_policy)
                
                context.pack_and_send(task.get_destination(), task_id, request)
        else:
            last_step = self._pipeline[-1]

            if last_step.is_complete():
                # if we are at the last step in the pipeline and it is complete then terminate and emit result
                self.terminate(context, task_result_or_exception)

            elif isinstance(task_result_or_exception, TaskException):
                if group is None or group.is_complete():
                    # else if have an exception then terminate but waiting for any parallel tasks in the group to complete first
                    self.terminate(context, task_result_or_exception)

    def terminate(self, context: TaskContext, task_result_or_exception: Union[TaskResult, TaskException]):
        task_request = context.unpack('task_request', TaskRequest)

        # if result_before_finally or exception_before_finally are set then we are in a finally block
        if context.pipeline_state.HasField('result_before_finally'):
            result_before_finally = context.pipeline_state.result_before_finally
        elif context.pipeline_state.HasField('exception_before_finally'):
            result_before_finally = context.pipeline_state.exception_before_finally
        else:
            result_before_finally = None

        if result_before_finally is not None and isinstance(task_result_or_exception, TaskResult):
            # finally ran successfully, so return the result of the previous task (rather than cleanup result from finally)
            task_result_or_exception = result_before_finally

        # set basic message properties
        task_result_or_exception.id = task_request.id
        task_result_or_exception.type = f'{task_request.type}.' + (
            'result' if isinstance(task_result_or_exception, TaskResult) else 'error')

        # pass back any state that we were given at the start of the pipeline
        task_result_or_exception.state.CopyFrom(task_request.state)

        # finally emit the result (to egress, destination address or caller address)
        self._emit_result(context, task_request, task_result_or_exception)

    @staticmethod
    def _emit_result(context, task_request, task_result_or_exception):
        # the result of this task is the result of the pipeline
        key = 'task_result' if isinstance(task_result_or_exception, TaskResult) else 'task_exception'
        context.pack_and_save(key, task_result_or_exception)

        # either send a message to egress if reply_topic was specified
        if task_request.HasField('reply_topic'):
            context.pack_and_send_egress(topic=task_request.reply_topic, value=task_result_or_exception)

        # or call back to a particular flink function if reply_address was specified
        elif task_request.HasField('reply_address'):
            address, identifer = context.to_address_and_id(task_request.reply_address)
            context.pack_and_send(address, identifer, task_result_or_exception)

        # or call back to our caller (if there is one)
        elif context.pipeline_state.caller_id is not None:
             if context.pipeline_state.caller_id != context.get_caller_id():  # don't call back to self
                context.pack_and_send(context.pipeline_state.caller_address, context.pipeline_state.caller_id, task_result_or_exception)

    @staticmethod
    def _add_pipeline_meta(context, caller_id, task_request: TaskRequest):
        task_request.meta['pipeline_address'] = context.pipeline_state.address
        task_request.meta['pipeline_id'] = context.pipeline_state.id
        task_request.meta['root_pipeline_id'] = context.pipeline_state.root_id
        task_request.meta['root_pipeline_address'] = context.pipeline_state.root_address

        if caller_id is not None:
            task_request.meta['parent_task_address'] = context.get_caller_address()
            task_request.meta['parent_task_id'] = caller_id





class PipelineBuilder(object):
    """
    Builder class for creating pipelines of Flink Tasks

    :param optional pipeline: list of initial pipeline entries e.g. from another builder
    """
    def __init__(self, pipeline: list = None):
        self._pipeline = [] if pipeline is None else pipeline
        self._builder_id = _gen_id()

    @property
    def id(self):
        """
        The ID of this pipeline
        """
        return self._builder_id

    def append_to(self, other: 'PipelineBuilder') -> 'PipelineBuilder':
        """
        Appends tasks from another pipeline builder into this one

        :param other: the other pipeline builder
        :return: the builder
        """
        other._pipeline.extend(self._pipeline)
        return self

    def append_group(self, pipelines: Iterable['PipelineBuilder']) -> 'PipelineBuilder':
        """
        Appends tasks from another pipeline builder into a new in_parallel group inside this one

        :param other: the other pipeline builder
        :return: the builder
        """
        group = Group(_gen_id())

        for pipeline in pipelines:
            pipeline._add_to_group(group)

        self._pipeline.append(group)
        return self

    def _add_to_group(self, group: Group):
        group.add_to_group(self._pipeline)

    @staticmethod
    def _unpack_single_tuple_args(args):
        # send a single argument by itself instead of wrapped inside a tuple
        if _is_tuple(args) and len(args) == 1:
            args = args[0]

        return args

    def send(self, fun, *args, **kwargs) -> 'PipelineBuilder':
        """
        Adds a task entry for the given Flink Task and arguments

        :param fun: the python function which should be decorated with @tasks.bind()
        :param args: the task args
        :param kwargs: the task kwargs
        :return: the builder
        """
        args = self._unpack_single_tuple_args(args)
        task_type, parameters = self._task_type_and_parameters_for(fun)
        self._pipeline.append(Task.from_fields(_gen_id(), task_type, args, kwargs, **parameters))
        return self

    def set(self, retry_policy: RetryPolicy = None, namespace: str = None,
            worker_name: str = None) -> 'PipelineBuilder':
        """
        Sets task properties on the last entry added to the builder

        :param option retry_policy: the task retry policy to use
        :param option namespace: the task namespace
        :param option worker_name: the task worker_name
        :return: the builder
        """

        if any(self._pipeline):
            entry = self._pipeline[-1]
            if retry_policy is not None:
                entry.retry_policy.CopyFrom(retry_policy.to_proto())
            if namespace is not None:
                entry.namespace = namespace
            if worker_name is not None:
                entry.worker_name = worker_name

        return self

    def continue_with(self, continuation, *args, **kwargs) -> 'PipelineBuilder':
        """
        Adds continuation to the pipeline

        :param continuation: the python function which should be decorated with @tasks.bind()
        :param args: the task args
        :param kwargs: the task kwargs
        :return: the builder
        """
        if isinstance(continuation, PipelineBuilder):
            continuation.append_to(self)
        else:
            args = self._unpack_single_tuple_args(args)
            task_type, parameters = self._task_type_and_parameters_for(continuation)
            self._pipeline.append(Task.from_fields(_gen_id(), task_type, args, kwargs, **parameters))
        return self

    def get_destination(self):
        """
        Returns the initial destination of the pipeline as None - i.e. use the default ingress
        """
        return None

    def to_task_request(self, serialiser) -> TaskRequest:
        """
        Serialises the pipeline as a TaskRequest with a task type of '__builtins.run_pipeline'

        :param serialiser: the serialiser to use such as DefaultSerialiser
        :return: TaskRequest protobuf message
        """
        task_id = self._builder_id
        task_type = '__builtins.run_pipeline'
        args = self.validate().to_proto(serialiser=serialiser)
        kwargs = {}
        parameters = {}    

        # send a single argument by itself instead of wrapped inside a tuple
        if _is_tuple(args) and len(args) == 1:
            args = args[0]

        task_request = TaskRequest(id=task_id, type=task_type)
        args_and_kwargs = serialiser.serialise_args_and_kwargs(args, kwargs)
        serialiser.serialise_request(task_request, args_and_kwargs)
        
        return task_request

    def finally_do(self, finally_action, *args, **kwargs) -> 'PipelineBuilder':
        """
        Adds finally to the pipeline

        :param continuation: the python function which should be decorated with @tasks.bind()
        :param args: the task args
        :param kwargs: the task kwargs
        :return: the builder
        """
        args = self._unpack_single_tuple_args(args)
        task_type, parameters = self._task_type_and_parameters_for(finally_action)
        combined_parameters = {**parameters, 'is_fruitful': False}
        task_entry = Task.from_fields(_gen_id(), task_type, args, kwargs, is_finally=True, **combined_parameters)
        self._pipeline.append(task_entry)
        return self

    def to_pipeline(self, serialiser=None):
        """
        Concretises the builder into a pipeline

        :param option serialiser: the serialiser to use such as DefaultSerialiser
        :return: a Flink Tasks pipeline
        """
        self.validate()
        return _Pipeline(self._pipeline, serialiser=serialiser)

    def validate(self) -> 'PipelineBuilder':
        """
        Validates the pipeline

        :return: the builder
        """
        errors = []

        finally_tasks = [task for task in self._pipeline if isinstance(task, Task) and task.is_finally]
        if len(finally_tasks) > 1:
            errors.append('Cannot have more than one "finally_do" method')
        if len(finally_tasks) == 1 and finally_tasks[0] != self._pipeline[-1]:
            errors.append('"finally_do" must be called at the end of a pipeline')
        
        if any(errors):
            error = ', '.join(errors)
            raise ValueError(f'Invalid pipeline: {error}')

        return self

    def to_proto(self, serialiser=None) -> Pipeline:
        """
        Serialises the pipeline to protobuf

        :param serialiser: the serialiser to use such as DefaultSerialiser
        :return: Pipeline protobuf message
        """
        return _Pipeline(self._pipeline, serialiser=serialiser).to_proto()

    @staticmethod
    def from_proto(pipeline_proto: Pipeline) -> 'PipelineBuilder':
        """
        Deserialises the pipeline from protobuf

        :param pipeline_proto: the pipeline as protobuf
        :return: Pipeline protobuf message
        """
        pipeline = []

        for proto in pipeline_proto.entries:
            if proto.HasField('task_entry'):
                pipeline.append(Task.from_proto(proto))
            elif proto.HasField('group_entry'):
                pipeline.append(Group.from_proto(proto))

        return PipelineBuilder(pipeline)

    def get_tasks(self) -> list:
        """
        Returns a list of all tasks identifiers (namespace, worker name, id) that make up this pipeline

        :return: list of tuples of namespace, worker name, id for each task
        """
        def yield_tasks(entry):
            for task_or_group in entry:
                if isinstance(task_or_group, Group):
                    for group_entry in task_or_group:
                        yield from yield_tasks(group_entry)
                else:
                    yield task_or_group.namespace, task_or_group.worker_name, task_or_group.id
        
        return list(yield_tasks(self))

    @staticmethod
    def _task_type_and_parameters_for(fun):
        try:
            parameters = fun.defaults()
            module_name = parameters.get('module_name', None)
            task_type = _task_type_for(fun, module_name)

            return task_type, parameters
        except AttributeError:
            raise AttributeError(f'Function {fun.__module__}.{fun.__name__} should be decorated with tasks.bind')

    def __iter__(self):
        return self._pipeline.__iter__()

    def __next__(self):
        return self._pipeline.__next__()
