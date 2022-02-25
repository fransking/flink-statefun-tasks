from statefun_tasks.utils import _type_name, _gen_id
from statefun_tasks.messages_pb2 import PipelineState, TaskState, TaskRequest, TaskResult, TaskException, \
    TaskActionRequest, TaskActionResult, TaskActionException, TaskEntry, GroupEntry, PipelineEntry, TaskRetryPolicy, \
    Pipeline, ChildPipeline

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
CHILD_PIPELINE_TYPE = make_protobuf_type(ChildPipeline, namespace='io.statefun_tasks.types')

_VALUE_TYPE_MAP = {
    TaskState: PIPELINE_STATE_TYPE,
    TaskState: TASK_STATE_TYPE,
    TaskRequest: TASK_REQUEST_TYPE,
    TaskResult: TASK_RESULT_TYPE,
    TaskException: TASK_EXCEPTION_TYPE,
    TaskActionRequest: TASK_ACTION_REQUEST_TYPE,
    TaskActionResult: TASK_ACTION_RESULT_TYPE,
    TaskActionException: TASK_ACTION_EXCEPTION_TYPE,
    ChildPipeline: CHILD_PIPELINE_TYPE,
}


class Task:
    def __init__(self, proto: TaskEntry, task_args=None, task_kwargs=None):

        if task_args is None and task_kwargs is None:
            self._proto_backed = True
            self._unpacked = False
            self._args = None
            self._kwargs = None
        else:
            self._proto_backed = False
            self._unpacked = True
            self._args = task_args
            self._kwargs = task_kwargs

        self._proto = proto

    @staticmethod
    def from_id(task_id, namespace=None, worker_name=None):
        return Task(TaskEntry(task_id=task_id, namespace=namespace, worker_name=worker_name))

    @staticmethod
    def from_fields(task_id, task_type, task_args, task_kwargs, is_finally=None, namespace=None, worker_name=None,
                    is_fruitful=None, retry_policy=None, display_name=None, is_wait=None, uid=None, **kwargs):
        proto = TaskEntry(
            task_id=task_id,
            task_type=task_type,
            complete=False,
            is_finally=is_finally,
            namespace=namespace,
            worker_name=worker_name,
            is_fruitful=is_fruitful,
            retry_policy=retry_policy,
            display_name=display_name,
            is_wait=is_wait,
            uid=uid if uid is not None else _gen_id()
        )
        return Task(proto, task_args, task_kwargs)

    @property
    def uid(self):
        """
        The unique ID of this task
        """
        return self._proto.uid

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

    @task_id.setter
    def task_id(self, value):
        self._proto.task_id = value

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

    @property
    def is_fruitful(self):
        return self._proto.is_fruitful

    @is_fruitful.setter
    def is_fruitful(self, value):
        self._proto.is_fruitful = value

    @property
    def retry_policy(self):
        return self._proto.retry_policy

    @property
    def pipeline_address(self):
        return self._proto.pipeline_address

    @pipeline_address.setter
    def pipeline_address(self, value):
        self._proto.pipeline_address = value

    @property
    def pipeline_id(self):
        return self._proto.pipeline_id

    @pipeline_id.setter
    def pipeline_id(self, value):
        self._proto.pipeline_id = value

    @property
    def namespace(self):
        return self._proto.namespace

    @namespace.setter
    def namespace(self, value):
        self._proto.namespace = value

    @property
    def worker_name(self):
        return self._proto.worker_name

    @worker_name.setter
    def worker_name(self, value):
        self._proto.worker_name = value

    @property
    def request(self):
        return self._proto.request

    @request.setter
    def request(self, value):
        return self._proto.request.CopyFrom(value)

    @property
    def display_name(self):
        return self._proto.display_name if self._proto.HasField('display_name') else None

    @display_name.setter
    def display_name(self, value):
        if value is None:
            self._proto.ClearField('display_name')
        else:
            self._proto.display_name = value

    @property
    def is_wait(self):
        return self._proto.is_wait

    @is_wait.setter
    def is_wait(self, value):
        self._proto.is_wait = value

    def unpack(self, serialiser):
        if self._unpacked or not self._proto_backed:
            return self

        # unpack args and kwargs
        self._args, self._kwargs = serialiser.deserialise_args_and_kwargs(self._proto.request)

        self._unpacked = True

        return self

    def get_destination(self):
        # task destination of form 'namespace/worker_name'
        return f'{self._proto.namespace}/{self._proto.worker_name}'

    def to_tuple(self):
        if not self._unpacked:
            raise ValueError('Task not fully unpacked from protobuf. Call task.unpack(seraliser) first')

        return self.task_id, self.task_type, self._args, self._kwargs

    def mark_complete(self):
        self._proto.complete = True

    def is_complete(self):
        return self._proto.complete

    def to_proto(self, serialiser) -> PipelineEntry:
        if not self._proto_backed:
            request = serialiser.serialise_args_and_kwargs(self._args, self._kwargs)
            self._proto.request.CopyFrom(request)

        return PipelineEntry(task_entry=self._proto)

    @staticmethod
    def from_proto(proto: PipelineEntry):
        return Task(proto=proto.task_entry)

    def __repr__(self):
        return self.task_id


class Group:
    def __init__(self, group_id, max_parallelism=None, is_wait=None):
        self.group_id = group_id
        self.max_parallelism = max_parallelism
        self.is_wait = is_wait
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
        proto = GroupEntry(group_id=self.group_id, max_parallelism=self.max_parallelism, is_wait=self.is_wait)

        for entries in self._group:
            pipeline = Pipeline()

            for entry in entries:
                entry_proto = entry.to_proto(serialiser)
                pipeline.entries.append(entry_proto)

            proto.group.append(pipeline)

        return PipelineEntry(group_entry=proto)

    @staticmethod
    def from_proto(proto: PipelineEntry):
        entry = Group(group_id=proto.group_entry.group_id, max_parallelism=proto.group_entry.max_parallelism, is_wait=proto.group_entry.is_wait)

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


class TasksException(Exception):
    def __init__(self, message):
        super().__init__(message)


class TaskAlreadyExistsException(TasksException):
    def __init__(self, message):
        super().__init__(message)


class TaskCancelledException(TasksException):
    def __init__(self, message):
        super().__init__(message)


class PipelineInProgress(TasksException):
    def __init__(self, message):
        super().__init__(message)


class YieldTaskInvocation(Exception):
    def __init__(self):
        super().__init__()
