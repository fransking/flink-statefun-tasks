from statefun_tasks.types import Task, Group, RetryPolicy
from statefun_tasks.utils import _gen_id, _is_tuple
from statefun_tasks.pipeline import _Pipeline
from statefun_tasks.messages_pb2 import TaskRequest, Pipeline

from typing import Iterable


def in_parallel(entries: list, max_parallelism=None):
    return PipelineBuilder().append_group(entries, max_parallelism)


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

    def append_group(self, pipelines: Iterable['PipelineBuilder'], max_parallelism=None) -> 'PipelineBuilder':
        """
        Appends tasks from another pipeline builder into a new in_parallel group inside this one

        :param other: the other pipeline builder
        :param option max_parallelism: the maximum number of tasks to invoke in parallel for this group 
        :return: the builder
        """
        group = Group(_gen_id(), max_parallelism=max_parallelism)

        for pipeline in pipelines:
            pipeline._add_to_group(group)

        self._pipeline.append(group)
        return self

    def _add_to_group(self, group: Group):
        group.add_to_group(self._pipeline)

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
            worker_name: str = None, is_fruitful=None, display_name=None) -> 'PipelineBuilder':
        """
        Sets task properties on the last entry added to the builder

        :param option retry_policy: the task retry policy to use
        :param option namespace: the task namespace
        :param option worker_name: the task worker_name
        :param option is_fruitful: set to false to drop the results of tasks
        :param option display_name: optional friendly name for this task
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
        Serialises the pipeline as a TaskRequest with a task type of '__builtins.run_pipeline'

        :param serialiser: the serialiser to use such as DefaultSerialiser
        :return: TaskRequest protobuf message
        """
        task_id = self._builder_id
        task_type = '__builtins.run_pipeline'
        args = self.validate().to_proto(serialiser)
        kwargs = {}

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

    def to_pipeline(self, serialiser=None, is_fruitful=True, events=None):
        """
        Concretises the builder into a pipeline

        :param option serialiser: the serialiser to use such as DefaultSerialiser
        :param option is_fruitful: whether this pipeline is fruitful (i.e. returns a result). Default is True
        :param option events: event handler instance if this pipeline should fire events

        :return: a Flink Tasks pipeline
        """
        self.validate()
        return _Pipeline(self._pipeline, events=events, serialiser=serialiser, is_fruitful=is_fruitful)

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

    def is_empty(self):
        """
        Tests if the pipeline contains any tasks

        :return: true if empty, false otherwise
        """
        return not any(self.get_tasks())

    def __iter__(self):
        return self._pipeline.__iter__()

    def __next__(self):
        return self._pipeline.__next__()
