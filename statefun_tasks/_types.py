from typing import NamedTuple
from datetime import timedelta
from .messages_pb2 import TaskEntry, GroupEntry, PipelineEntry, TaskRetryPolicy, Pipeline
from ._utils import _type_name
import json


class _TaskEntry(object):
    def __init__(self, task_id, task_type, args, kwargs, parameters=None, is_finally=False):
        self.task_id = task_id
        self.task_type = task_type
        self.args = args
        self.kwargs = kwargs
        self.complete = False
        self.parameters = {} if parameters is None else parameters
        self.is_finally = is_finally

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
        return self.task_id, self.task_type, self.args, self.kwargs

    def mark_complete(self):
        self.complete = True

    def is_complete(self):
        return self.complete

    def validate(self, errors):
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
        
        if self.args is not None:
            proto.args.CopyFrom(serialiser.to_proto(self.args))

        if self.kwargs is not None: 
            proto.kwargs.CopyFrom(serialiser.to_proto(self.kwargs))

        if self.parameters is not None:
            proto.parameters.CopyFrom(serialiser.to_proto(self.parameters))
        
        return PipelineEntry(task_entry=proto)

    @staticmethod 
    def from_proto(proto: PipelineEntry, serialiser):
        entry = _TaskEntry(
            task_id=proto.task_entry.task_id, 
            task_type=proto.task_entry.task_type, 
            args=serialiser.from_proto(proto.task_entry.args), 
            kwargs=serialiser.from_proto(proto.task_entry.kwargs), 
            parameters=serialiser.from_proto(proto.task_entry.parameters), 
            is_finally=proto.task_entry.is_finally)

        entry.complete = proto.task_entry.complete

        return entry

    def __repr__(self):
        return self.task_id


class _GroupEntry(object):
    def __init__(self, group_id):
        self.group_id = group_id
        self._group = []

    def set_parameters(self, parameters):
        pass  # do nothing for now

    def get_parameter(self, parameter_name):
        return None

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
        entry = _GroupEntry(group_id=proto.group_entry.group_id)

        group = []
        for pipeline in proto.group_entry.group:
            entries = []
            
            for proto in pipeline.entries:
                if proto.HasField('task_entry'):
                    entries.append(_TaskEntry.from_proto(proto, serialiser))
                elif proto.HasField('group_entry'):
                    entries.append(_GroupEntry.from_proto(proto, serialiser))

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
