from typing import NamedTuple
from datetime import timedelta
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

    def to_tuple(self):
        return self.task_id, self.task_type, self.args, self.kwargs

    def mark_complete(self):
        self.complete = True

    def is_complete(self):
        return self.complete

    def to_json_dict(self, verbose=False):
        return json.loads(self.to_json(verbose))

    def to_json(self, verbose=False):
        if verbose:
            return json.dumps(self.__dict__, default=lambda o: str(o))
        else:
            return json.dumps({
                'task_id': self.task_id,
                'task_type': self.task_type
            })

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

    def to_json_dict(self, verbose=False):
        return {
            'group_id': self.group_id,
            'group': [[entry.to_json_dict(verbose) for entry in entries] for entries in self._group]
        }

    def to_json(self, verbose=False):
        return json.dumps(self.to_json_dict(verbose=verbose))

    def __repr__(self):
        return self._group.__repr__()


class _GroupResult(object):
    def __init__(self, data, content_type):
        self.data = data
        self.content_type = content_type


class TaskRetryPolicy(NamedTuple):
    retry_for: list = [Exception]
    max_retries: int = 1
    delay: timedelta = timedelta()
    exponential_back_off: bool = False
