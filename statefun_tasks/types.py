from statefun_tasks.utils import _type_name
from statefun_tasks.messages_pb2 import TaskState, TaskRequest, TaskResult, TaskException, \
    TaskActionRequest, TaskActionResult, TaskActionException, TaskEntry, GroupEntry, PipelineEntry, TaskRetryPolicy, Pipeline

from statefun import make_protobuf_type
from google.protobuf.message import Message

from dataclasses import dataclass, field
from datetime import timedelta
import json


# Protobuf type registrations required by Flink Statefun API
TASK_STATE_TYPE = make_protobuf_type(TaskState, namespace='io.statefun_tasks.types')
TASK_REQUEST_TYPE = make_protobuf_type(TaskRequest, namespace='io.statefun_tasks.types')
TASK_RESULT_TYPE = make_protobuf_type(TaskResult, namespace='io.statefun_tasks.types')
TASK_EXCEPTION_TYPE = make_protobuf_type(TaskException, namespace='io.statefun_tasks.types')
TASK_ACTION_REQUEST_TYPE = make_protobuf_type(TaskActionRequest, namespace='io.statefun_tasks.types')
TASK_ACTION_RESULT_TYPE = make_protobuf_type(TaskActionResult, namespace='io.statefun_tasks.types')
TASK_ACTION_EXCEPTION_TYPE = make_protobuf_type(TaskActionException, namespace='io.statefun_tasks.types')


_VALUE_TYPE_MAP = {
    TaskState: TASK_STATE_TYPE,
    TaskRequest: TASK_REQUEST_TYPE,
    TaskResult: TASK_RESULT_TYPE,
    TaskException: TASK_EXCEPTION_TYPE,
    TaskActionRequest: TASK_ACTION_REQUEST_TYPE,
    TaskActionResult: TASK_ACTION_RESULT_TYPE,
    TaskActionException: TASK_ACTION_EXCEPTION_TYPE
}


class Task:
    def __init__(self, task_id, task_type, args, kwargs, parameters=None, is_finally=False):
        self.task_id = task_id
        self.task_type = task_type
        self.args = args
        self.kwargs = kwargs
        self.complete = False
        self.parameters = {} if parameters is None else parameters
        self.is_finally = is_finally

    @property
    def id(self):
        """
        The ID of this task
        """
        return self.task_id

    def set_parameters(self, parameters):
        self.parameters.update(parameters)

    def get_parameter(self, parameter_name):
        if parameter_name in self.parameters:
            return self.parameters[parameter_name]
        else:
            return None

    def get_destination(self):
        # task destination of form 'namespace/worker_name'
        return f'{self.get_parameter("namespace")}/{self.get_parameter("worker_name")}'

    def to_tuple(self):
        return self.task_id, self.task_type, self.args, self.kwargs, self.parameters

    def mark_complete(self):
        self.complete = True

    def is_complete(self):
        return self.complete

    def validate(self, errors):
        if self.task_id is None:
            errors.append(f'task {self.task_type} is missing a task_id')

        if self.get_parameter('namespace') is None:
            errors.append(f'task {self.task_type} [{self.task_id}] is missing "namespace"')

        if self.get_parameter('worker_name') is None:
            errors.append(f'task {self.task_type} [{self.task_id}] is missing "worker_name"')

    def to_proto(self, serialiser) -> PipelineEntry:
        proto = TaskEntry(
            task_id=self.task_id, 
            task_type=self.task_type, 
            complete=self.complete, 
            is_finally=self.is_finally)
        
        request = serialiser.serialise_args_and_kwargs(self.args, self.kwargs)
        proto.request.CopyFrom(request)

        if self.parameters is not None:
            proto.parameters.CopyFrom(serialiser.to_proto(self.parameters))
        
        return PipelineEntry(task_entry=proto)

    @staticmethod 
    def from_proto(proto: PipelineEntry, serialiser):
        args, kwargs = serialiser.deserialise_args_and_kwargs(proto.task_entry.request)

        entry = Task(
            task_id=proto.task_entry.task_id, 
            task_type=proto.task_entry.task_type, 
            args=args, 
            kwargs=kwargs, 
            parameters=serialiser.from_proto(proto.task_entry.parameters), 
            is_finally=proto.task_entry.is_finally)

        entry.complete = proto.task_entry.complete

        return entry

    def __repr__(self):
        return self.task_id


class Group:
    def __init__(self, group_id):
        self.group_id = group_id
        self._group = []

    def set_parameters(self, parameters):
        pass  # do nothing for now

    def get_parameter(self, parameter_name):
        return None

    def get_destination(self):
        return None  # _GroupEntries don't have a single destination

    def add_to_group(self, tasks):
        self._group.append(tasks)

    def __iter__(self):
        return self._group.__iter__()

    def __next__(self):
        return self._group.__next__()

    def is_complete(self):
        return all(entry.is_complete() for entries in self._group for entry in entries)

    def validate(self, errors):
        for entries in self._group:
            for entry in entries:
                entry.validate(errors)

    def to_proto(self, serialiser) -> PipelineEntry:
        proto = GroupEntry(group_id=self.group_id)

        for entries in self._group:
            pipeline = Pipeline()

            for entry in entries:
                entry_proto = entry.to_proto(serialiser)
                pipeline.entries.append(entry_proto)

            proto.group.append(pipeline)

        return PipelineEntry(group_entry=proto)

    @staticmethod 
    def from_proto(proto: PipelineEntry, serialiser):
        entry = Group(group_id=proto.group_entry.group_id)

        group = []
        for pipeline in proto.group_entry.group:
            entries = []
            
            for proto in pipeline.entries:
                if proto.HasField('task_entry'):
                    entries.append(Task.from_proto(proto, serialiser))
                elif proto.HasField('group_entry'):
                    entries.append(Group.from_proto(proto, serialiser))

            group.append(entries)

        entry._group = group
        return entry

    def __repr__(self):
        return self._group.__repr__()


@dataclass
class RetryPolicy:
    retry_for: list = field(default_factory=lambda: [Exception])
    max_retries: int = 1
    delay: timedelta = timedelta()
    exponential_back_off: bool = False

    def to_proto(self):
        return TaskRetryPolicy(
            retry_for=[_type_name(ex) for ex in self.retry_for],
            max_retries=self.max_retries, 
            delay_ms=self.delay.total_seconds() * 1000, 
            exponential_back_off=self.exponential_back_off)


class TaskAlreadyExistsException(Exception):
    def __init__(self, message):
        super().__init__(message)
