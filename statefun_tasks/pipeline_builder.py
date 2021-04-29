from statefun_tasks.types import Task, Group, RetryPolicy
from statefun_tasks.utils import _gen_id, _is_tuple, _task_type_for
from statefun_tasks.pipeline import _Pipeline
from statefun_tasks.messages_pb2 import TaskRequest, Pipeline

from typing import Union, Iterable


def in_parallel(entries: list):
    return PipelineBuilder().append_group(entries)


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
        self._pipeline.append(Task(_gen_id(), task_type, args, kwargs, parameters=parameters))
        return self

    def set(self, retry_policy:RetryPolicy = None, **params) -> 'PipelineBuilder':
        """
        Sets task properties on the last entry added to the builder

        :param option retry_policy: the task retry policy to use
        :param params: any other parameters to the task
        :return: the builder
        """
        if retry_policy is not None:
            params['retry_policy'] = retry_policy.to_proto()

        if any(self._pipeline):
            entry = self._pipeline[-1]
            entry.set_parameters(params)

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
            self._pipeline.append(Task(_gen_id(), task_type, args, kwargs, parameters=parameters))
        return self

    def get_destination(self):
        """
        Returns the initial destination of the pipeline - i.e. where the first task should be sent 
        or None if it should be set to the default namespace/worker

        :return: the initial destination (e.g. example/worker) or None if it should use the default
        """
        return None if not any(self._pipeline) else self._pipeline[0].get_destination()

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

        task_request = TaskRequest(id=task_id, type=task_type, parameters=serialiser.to_proto(parameters))
        serialiser.serialise_request(task_request, args, kwargs)
        
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
        task_entry = Task(_gen_id(), task_type, args, kwargs, parameters=parameters, is_finally=True)
        task_entry.set_parameters({'is_fruitful': False})
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

        for entry in self._pipeline:
            entry.validate(errors)

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
    def from_proto(pipeline_proto: Pipeline, serialiser) -> 'PipelineBuilder':
        """
        Deserialises the pipeline from protobuf

        :param pipeline_proto: the pipeline as protobuf
        :param serialiser: the serialiser to use such as DefaultSerialiser
        :return: Pipeline protobuf message
        """
        pipeline = []

        for proto in pipeline_proto.entries:
            if proto.HasField('task_entry'):
                pipeline.append(Task.from_proto(proto, serialiser))
            elif proto.HasField('group_entry'):
                pipeline.append(Group.from_proto(proto, serialiser))

        return PipelineBuilder(pipeline)

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
