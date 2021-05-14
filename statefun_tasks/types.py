from statefun_tasks.utils import _type_name
from statefun_tasks.messages_pb2 import PipelineState, TaskState, TaskRequest, TaskResult, TaskException, \
    TaskActionRequest, TaskActionResult, TaskActionException, TaskEntry, GroupEntry, PipelineEntry, TaskRetryPolicy, Pipeline

from statefun import make_protobuf_type

from dataclasses import dataclass, field
from datetime import timedelta

# Protobuf type registrations required by Flink Statefun API
PIPELINE_STATE_TYPE = make_protobuf_type(PipelineState, namespace='io.statefun_tasks.types')
TASK_STATE_TYPE = make_protobuf_type(TaskState, namespace='io.statefun_tasks.types')
TASK_REQUEST_TYPE = make_protobuf_type(TaskRequest, namespace='io.statefun_tasks.types')
TASK_RESULT_TYPE = make_protobuf_type(TaskResult, namespace='io.statefun_tasks.types')
TASK_EXCEPTION_TYPE = make_protobuf_type(TaskException, namespace='io.statefun_tasks.types')
TASK_ACTION_REQUEST_TYPE = make_protobuf_type(TaskActionRequest, namespace='io.statefun_tasks.types')
TASK_ACTION_RESULT_TYPE = make_protobuf_type(TaskActionResult, namespace='io.statefun_tasks.types')
TASK_ACTION_EXCEPTION_TYPE = make_protobuf_type(TaskActionException, namespace='io.statefun_tasks.types')


_VALUE_TYPE_MAP = {
    TaskState: PIPELINE_STATE_TYPE,
    TaskState: TASK_STATE_TYPE,
    TaskRequest: TASK_REQUEST_TYPE,
    TaskResult: TASK_RESULT_TYPE,
    TaskException: TASK_EXCEPTION_TYPE,
    TaskActionRequest: TASK_ACTION_REQUEST_TYPE,
    TaskActionResult: TASK_ACTION_RESULT_TYPE,
    TaskActionException: TASK_ACTION_EXCEPTION_TYPE
}


class Task:
    def __init__(self, task_id=None, task_type=None, args=None, kwargs=None, parameters=None, is_finally=False, proto=None):

        if proto is None:
            proto = TaskEntry(
                task_id=task_id, 
                task_type=task_type, 
                complete=False, 
                is_finally=is_finally)

            self._proto_backed = False
            self._unpacked = True
            self._args = args
            self._kwargs = kwargs
        else:
            self._proto_backed = True
            self._unpacked = False
            self._args = None
            self._kwargs = None

        self._proto = proto
        self._parameters = {} if parameters is None else parameters

    @property
    def id(self):
        """
        The ID of this task
        """
        return self._proto.task_id

    @property
    def task_id(self):
        """
        The ID of this task
        """
        return self._proto.task_id

    @property
    def task_type(self):
        """
        The type of this task
        """
        return self._proto.task_type

    @property
    def complete(self):
        """
        Whether this task is considered complete or not
        """
        return self._proto.complete

    @property
    def is_finally(self):
        """
        Whether this task is a finally task
        """
        return self._proto.is_finally 

    def unpack(self, serialiser):
        if self._unpacked or not self._proto_backed:
            return self

        # unpack args and kwargs
        self._args, self._kwargs = serialiser.deserialise_args_and_kwargs(self._proto.request)

        # unpack parameters - those already set via set_parameters() take precedence
        parameters = serialiser.from_proto(self._proto.parameters)
        parameters.update(self._parameters)
        self._parameters = parameters 

        self._unpacked = True

        return self

    def set_parameters(self, parameters):
        if not self._unpacked:
            raise ValueError('Task not fully unpacked from protobuf. Call task.unpack(seraliser) first')

        self._parameters.update(parameters)

    def get_parameter(self, parameter_name):
        if not self._unpacked:
            raise ValueError('Task not fully unpacked from protobuf. Call task.unpack(seraliser) first')

        if parameter_name in self._parameters:
            return self._parameters[parameter_name]
        else:
            return None

    def get_destination(self):
        # task destination of form 'namespace/worker_name'
        return f'{self.get_parameter("namespace")}/{self.get_parameter("worker_name")}'

    def to_tuple(self):
        if not self._unpacked:
            raise ValueError('Task not fully unpacked from protobuf. Call task.unpack(seraliser) first')

        return self.task_id, self.task_type, self._args, self._kwargs, self._parameters

    def mark_complete(self):
        self._proto.complete = True

    def is_complete(self):
        return self._proto.complete

    def to_proto(self, serialiser) -> PipelineEntry:
        if not self._proto_backed:
            request = serialiser.serialise_args_and_kwargs(self._args, self._kwargs)
            self._proto.request.CopyFrom(request)

        if any(self._parameters):
            self._proto.parameters.CopyFrom(serialiser.to_proto(self._parameters))
        
        return PipelineEntry(task_entry=self._proto)

    @staticmethod 
    def from_proto(proto: PipelineEntry):
        return Task(proto=proto.task_entry)

    def __repr__(self):
        return self.task_id


class Group:
    def __init__(self, group_id):
        self.group_id = group_id
        self._group = []

    def add_to_group(self, tasks):
        self._group.append(tasks)

    def __iter__(self):
        return self._group.__iter__()

    def __next__(self):
        return self._group.__next__()

    def is_complete(self):
        return all(entry.is_complete() for entries in self._group for entry in entries)

    def get_destination(self):
        return None  # _GroupEntries don't have a single destination

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
    def from_proto(proto: PipelineEntry):
        entry = Group(group_id=proto.group_entry.group_id)

        group = []
        for pipeline in proto.group_entry.group:
            entries = []
            
            for proto in pipeline.entries:
                if proto.HasField('task_entry'):
                    entries.append(Task.from_proto(proto))
                elif proto.HasField('group_entry'):
                    entries.append(Group.from_proto(proto))

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
