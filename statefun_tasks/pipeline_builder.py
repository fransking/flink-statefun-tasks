from re import L
from statefun_tasks.types import Task, Group, RetryPolicy, ProtobufSerialisable
from statefun_tasks.utils import _gen_id
from statefun_tasks.messages_pb2 import TaskRequest, Pipeline
from statefun_tasks.builtin import builtin
from statefun_tasks.default_serialiser import pack_any, DefaultSerialiser
from typing import Iterable
import math


class PipelineBuilder(ProtobufSerialisable):
    """
    Builder class for creating pipelines

    :param optional pipeline: list of initial pipeline entries e.g. from another builder
    """
    def __init__(self, pipeline: list = None):
        self._pipeline = [] if pipeline is None else pipeline
        self._builder_id = _gen_id()
        self._inline = False
        self._initial_args = None
        self._initial_kwargs = None
        self._initial_state = None

    @property
    def id(self):
        """
        The ID of this pipeline
        """
        return self._builder_id

    @id.setter
    def id(self, value):
        self._builder_id = value

    @property
    def is_inline(self):
        """
        Returns true if the pipeline is inline or not
        """
        return self._inline

    @property
    def has_initial_parameters(self):
        """
        Returns true if the pipeline is has initial parameters (args, kwargs, state)
        """
        parameters = [self._initial_args, self._initial_kwargs, self._initial_state]
        return any([parameter is not None for parameter in parameters])

    def append(self, task: Task) -> 'PipelineBuilder':
        """
        Appends a single task onto this pipeline

        :param task: the task to append
        :return: the builder
        """
        self._pipeline.append(task)
        return self

    def append_to(self, other: 'PipelineBuilder') -> 'PipelineBuilder':
        """
        Appends tasks from another pipeline builder into this one

        :param other: the other pipeline builder
        :return: the builder
        """
        self._raise_if_initial_parameters_are_set()
        other._pipeline.extend(self._pipeline)
        return self

    def append_group(self, pipelines: Iterable['PipelineBuilder'], max_parallelism=None, return_exceptions=False, ordered=True) -> 'PipelineBuilder':
        """
        Appends tasks from another pipeline builders into a new in_parallel group inside this one

        :param pipelines: the other pipeline builders
        :param option max_parallelism: the maximum number of tasks to invoke in parallel for this group 
        :param option return_exceptions: if True then tasks that raise exceptions will not cause an aggregated exception to be thrown but instead will appear in the results
        :param option ordered: if False then the results of the group will come back unordered. Unordered groups are more efficiently aggregated by Flink.
        :return: the builder
        """
        group = Group(_gen_id(), max_parallelism=max_parallelism, return_exceptions=return_exceptions, is_unordered=not ordered)

        for pipeline in pipelines:
            self._raise_if_initial_parameters_are_set(pipeline)
            pipeline._add_to_group(group)

        self._pipeline.append(group)
        return self

    def _add_to_group(self, group: Group):
        group.add_to_group(self._pipeline)

    def _raise_if_initial_parameters_are_set(self, pipeline=None):
        pipeline = pipeline or self

        if pipeline.has_initial_parameters:
            raise ValueError('Ambiguous initial parameters')

    def send(self, fun, *args, **kwargs) -> 'PipelineBuilder':
        """
        Adds a task entry for the given Flink Task and arguments

        :param fun: the python function which should be decorated with @tasks.bind()
        :param args: the task args
        :param kwargs: the task kwargs
        :return: the builder
        """
        try:
            task = fun.to_task(args, kwargs)
            self._pipeline.append(task)
        except AttributeError:
            raise AttributeError(f'Function {fun.__module__}.{fun.__name__} should be decorated with tasks.bind')
        return self

    def set(self, retry_policy: RetryPolicy = None, namespace: str = None,
            worker_name: str = None, is_fruitful=None, display_name=None, task_id=None) -> 'PipelineBuilder':
        """
        Sets task properties on the last entry added to the builder

        :param option retry_policy: the task retry policy to use
        :param option namespace: the task namespace
        :param option worker_name: the task worker_name
        :param option is_fruitful: set to false to drop the results of tasks
        :param option display_name: optional friendly name for this task
        :param option task_id: optional task id for this task
        :return: the builder
        """

        if any(self._pipeline) and isinstance(self._pipeline[-1], Task):
            entry = self._pipeline[-1]
            if retry_policy is not None:
                entry.retry_policy.CopyFrom(retry_policy.to_proto())
            if namespace is not None:
                entry.namespace = namespace
            if worker_name is not None:
                entry.worker_name = worker_name
            if is_fruitful is not None:
                entry.is_fruitful = is_fruitful
            if display_name is not None:
                entry.display_name = display_name
            if task_id is not None:
                entry.task_id = task_id
        else:
            raise ValueError(f'set() must be applied to a task')

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
            try:
                task = continuation.to_task(args, kwargs)
                self._pipeline.append(task)
            except AttributeError:
                raise AttributeError(f'Function {continuation.__module__}.{continuation.__name__} should be decorated with tasks.bind')
        return self

    def continue_if(self, condition: bool, continuation, *args, **kwargs) -> 'PipelineBuilder':
        """
        Conditionally adds continuation to the pipeline

        :param condition: the condition
        :param continuation: the python function which should be decorated with @tasks.bind()
        :param args: the task args
        :param kwargs: the task kwargs
        :return: the builder
        """
        if condition:
            self.continue_with(continuation, *args, **kwargs)
        
        return self

    def wait(self) -> 'PipelineBuilder':
        """
        Causes the pipeline to automatically pause at this point

        :return: the builder
        """
        if any(self._pipeline):
            entry = self._pipeline[-1]
            entry.is_wait = True
        else:
            raise ValueError('wait() must be applied to a task or group not an empty pipeline')

        return self

    def get_destination(self):
        """
        Returns the initial destination of the pipeline as None - i.e. use the default ingress
        """
        return None

    def to_task_request(self, serialiser) -> TaskRequest:
        """
        Serialises the pipeline as a TaskRequest with a task type of builtin.run_pipeline

        :param serialiser: the serialiser to use such as DefaultSerialiser
        :return: TaskRequest protobuf message
        """
        task_id = self._builder_id
        task_type = builtin.run_pipeline.task_name
        args = self.validate().to_proto(serialiser)
        kwargs = {}

        task_request = TaskRequest(id=task_id, type=task_type, uid=_gen_id())
        args_and_kwargs = serialiser.serialise_args_and_kwargs(args, kwargs)
        serialiser.serialise_request(task_request, args_and_kwargs)

        return task_request

    def exceptionally(self, exception_task, *args, **kwargs) -> 'PipelineBuilder':
        """
        Adds exceptionally to the pipeline

        :param exception_task: the python function which should be decorated with @tasks.bind()
        :param args: the task args
        :param kwargs: the task kwargs
        :return: the builder
        """
        try:
            task = exception_task.to_task(args, kwargs, is_exceptionally=True)
            self._pipeline.append(task)
        except AttributeError:
            raise AttributeError(f'Function {exception_task.__module__}.{exception_task.__name__} should be decorated with tasks.bind')
        return self

    def finally_do(self, finally_action, *args, **kwargs) -> 'PipelineBuilder':
        """
        Adds finally to the pipeline

        :param finally_action: the python function which should be decorated with @tasks.bind()
        :param args: the task args
        :param kwargs: the task kwargs
        :return: the builder
        """
        try:
            task = finally_action.to_task(args, kwargs, is_finally=True)
            self._pipeline.append(task)
        except AttributeError:
            raise AttributeError(f'Function {finally_action.__module__}.{finally_action.__name__} should be decorated with tasks.bind')
        return self

    def set_task_defaults(self, default_namespace, default_worker_name) ->  'PipelineBuilder':
        """
        Sets defaults on task entries if they are not set

        :return: the builder
        """
        for task in self._get_tasks():
            if task.namespace == '':
                task.namespace = default_namespace
            if task.worker_name == '':
                task.worker_name = default_worker_name

        return self

    def with_initial(self, args=Ellipsis, kwargs=Ellipsis, state=Ellipsis) ->  'PipelineBuilder':
        """
        Optionally sets the initial args kwargs and state to be passed to the initial tasks(s) in this pipeline

        :param option args: arguments as tuple or TupleOfAny
        :param option state: state
        :param option kwargs: keyword arguments as dict or MapStringToAny
        :return: the builder
        """
        if args != Ellipsis:
            self._initial_args = args

        if kwargs != Ellipsis:
            self._initial_kwargs = kwargs

        if state != Ellipsis:
            self._initial_state = state
            
        return self

    def inline(self, is_inline=True) ->  'PipelineBuilder':
        """
        Marks the pipeline as being inline (or not).  
        
        By default pipelines are not inline. Inline pipelines accept inputs from and share state with their parent task.

        :return: the builder
        """
        self._inline = is_inline
        return self

    def validate(self) -> 'PipelineBuilder':
        """
        Validates the pipeline raising a ValueError if the pipeline is invalid

        :return: the builder
        """
        errors = []

        all_tasks = self._get_tasks()
        all_groups = self._get_groups()

        task_uids = [task.uid for task in all_tasks]
        if len(task_uids) != len(set(task_uids)):
            errors.append('Task uids must be unique')
        
        group_ids = [group.group_id for group in all_groups]
        if len(group_ids) != len(set(group_ids)):
            errors.append('Group ids must be unique')

        finally_tasks = [task for task in all_tasks if task.is_finally]
        if len(finally_tasks) > 1:
            errors.append('Cannot have more than one "finally_do" task')
        if len(finally_tasks) == 1 and finally_tasks[0] != self._pipeline[-1]:
            errors.append('"finally_do" must be called at the end of the pipeline')

        exceptionally_tasks = [task for task in all_tasks if task.is_exceptionally]                
        if any(list(set(exceptionally_tasks) & set(finally_tasks))):
            errors.append('Tasks can be either finally_do or exceptionally but not both')

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
        self.validate()

        serialiser = serialiser or DefaultSerialiser()

        pipeline = Pipeline(
            entries=[p.to_proto(serialiser) for p in self._pipeline], 
            inline=self._inline)

        if self._initial_args is not None:
            pipeline.initial_args.CopyFrom(pack_any(serialiser.to_proto(self._initial_args)))
        
        if self._initial_kwargs is not None:
            pipeline.initial_kwargs.CopyFrom(serialiser.to_proto(self._initial_kwargs))

        if self._initial_state is not None:
            pipeline.initial_state.CopyFrom(pack_any(serialiser.to_proto(self._initial_state)))
        
        return pipeline


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

        builder = PipelineBuilder(pipeline)

        if pipeline_proto.inline:
            builder = builder.inline()

        if pipeline_proto.HasField('initial_args'):
            builder.with_initial(args=pipeline_proto.initial_args)

        if pipeline_proto.HasField('initial_kwargs'):
            builder.with_initial(kwargs=pipeline_proto.initial_kwargs)

        if pipeline_proto.HasField('initial_state'):
            builder.with_initial(state=pipeline_proto.initial_state)

        return builder

    def get_tasks(self) -> list:
        """
        Returns a list of all tasks identifiers (namespace, worker name, id) that make up this pipeline

        :return: list of tuples of namespace, worker name, id for each task
        """
        return [(t.namespace, t.worker_name, t.id) for t in self._get_tasks()]

    def is_empty(self):
        """
        Tests if the pipeline contains any tasks

        :return: true if empty, false otherwise
        """
        return not any(self.get_tasks())

    def _get_tasks(self) -> list:
        def yield_tasks(entry):
            for task_or_group in entry:
                if isinstance(task_or_group, Group):
                    for group_entry in task_or_group:
                        yield from yield_tasks(group_entry)
                else:
                    yield task_or_group
        
        return list(yield_tasks(self))

    def _get_groups(self) -> list:
        def yield_groups(entry):
            for task_or_group in entry:
                if isinstance(task_or_group, Group):
                    yield task_or_group
                    for group_entry in task_or_group:
                        yield from yield_groups(group_entry)
        
        return list(yield_groups(self))

    def __iter__(self):
        return self._pipeline.__iter__()

    def __next__(self):
        return self._pipeline.__next__()


def in_parallel(entries: list, max_parallelism=None, num_stages:int = 1, return_exceptions=False, ordered=True):
    """
    Creates a parallism of tasks to be run concurrently

    pipeline = in_parallel([a.send(i) for i in range(100)]) 

    :param entries: the tasks to run
    :param optional max_parallelism: the maximum number of tasks to run in parallel at a time
    :param optional num_stages: the number of sub-pipelines to break this parallelism into.  Used to scale larger parallelisms.
    :param optional return_exceptions: if True then tasks that raise exceptions will not cause an aggregated exception to be thrown but instead will appear in the results
    :param option ordered: if False then the results of the group will come back unordered.  Unordered groups are more efficiently aggregated by Flink.
    :return: a pipeline
    """

    if num_stages > 1:
        # split up a group such [[1,2,3,4,5,6]] into inline pipelines each with a subset of the group
        # i.e. [[p[1,2], p[3,4], p[5,6]]] followed by a flatten to allow for better distribution
        # of a parallelism over multiple workers
        chunk_size = max(math.ceil(len(entries) / num_stages), 1)
        per_stage_max_parallelism = None if max_parallelism is None else max(int(max_parallelism / num_stages), 1)
        stages = [entries[i:i + chunk_size] for i in range(0, len(entries), chunk_size)]

        if len(stages) > 1:
            per_stage_pipeline = [
                PipelineBuilder().append_group(stage, 
                                               max_parallelism=per_stage_max_parallelism, 
                                               return_exceptions=return_exceptions,
                                               ordered=ordered) 
                for stage in stages
            ]
            
            group = [PipelineBuilder().append(builtin.run_pipeline.to_task(args=pipeline.inline())) for pipeline in per_stage_pipeline] 

            return PipelineBuilder().append_group(group, 
                                                  max_parallelism=max_parallelism, 
                                                  return_exceptions=return_exceptions,
                                                  ordered=ordered).continue_with(builtin.flatten_results)

    return PipelineBuilder().append_group(entries, max_parallelism, return_exceptions=return_exceptions, ordered=ordered)
