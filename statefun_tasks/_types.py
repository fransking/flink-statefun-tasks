from typing import NamedTuple
from datetime import timedelta
from google.protobuf.message import Message
from .messages_pb2 import TaskEntry, GroupEntry, PipelineEntry, TaskRetryPolicy, Pipeline
from ._utils import _type_name


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
                    is_fruitful=None, retry_policy=None, module_name=None, with_state=None,
                    with_context=None):
        proto = TaskEntry(
            task_id=task_id,
            task_type=task_type,
            complete=False,
            is_finally=is_finally,
            namespace=namespace,
            worker_name=worker_name,
            is_fruitful=is_fruitful,
            retry_policy=retry_policy,
            module_name=module_name,
            with_state=with_state,
            with_context=with_context
        )
        return Task(proto, task_args, task_kwargs)

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

    @property
    def is_fruitful(self):
        return self._proto.is_fruitful

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
    def __init__(self, group_id, max_parallelism=None):
        self.group_id = group_id
        self.max_parallelism = max_parallelism
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
        proto = GroupEntry(group_id=self.group_id, max_parallelism=self.max_parallelism)

        for entries in self._group:
            pipeline = Pipeline()

            for entry in entries:
                entry_proto = entry.to_proto(serialiser)
                pipeline.entries.append(entry_proto)

            proto.group.append(pipeline)

        return PipelineEntry(group_entry=proto)

    @staticmethod
    def from_proto(proto: PipelineEntry):
        entry = Group(group_id=proto.group_entry.group_id, max_parallelism=proto.group_entry.max_parallelism)

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


class RetryPolicy(NamedTuple):
    retry_for: list = [Exception]
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
