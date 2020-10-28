from .messages_pb2 import TaskRequest, TaskResult, TaskException
from ._types import _GroupResult

import json
import pickle
from typing import Union


def _dumps(data):
    return pickle.dumps(data)


def _loads(data):
    return pickle.loads(data)


def deserialise(task_object: Union[TaskRequest, TaskResult, _GroupResult]):
    content_type = task_object.content_type

    if task_object.data is None or len(task_object.data) == 0:
        return None

    if content_type is None:
        raise ValueError('Missing content_type')

    if content_type not in [
        'application/json',
        'application/python-pickle',
        'application/octect-stream'
        ]:
        raise ValueError(f'Unsupported content_type {content_type}')

    if content_type.lower() == 'application/json':
        return json.loads(task_object.data.decode('utf-8'))

    if content_type.lower() == 'application/python-pickle':
        return pickle.loads(task_object.data)

    if content_type.lower() == 'application/octect-stream':
        return task_object.data

    raise ValueError(f'Unhandled content_type {content_type}')


def deserialise_result(task_result: TaskResult):
    data = deserialise(task_result)
    if isinstance(data, (list, tuple)) and len(data) == 1:
        # single results are still returned as single element list/tuple and are thus unpacked
        return data[0]
    else:
        return data


def serialise(task_object: Union[TaskRequest, TaskResult], data, content_type: str):
    if content_type is None:
        raise ValueError('Missing content_type')

    if content_type not in [
        'application/json',
        'application/python-pickle',
        'application/octect-stream'
        ]:
        raise ValueError(f'Unsupported content_type {content_type}')

    task_object.content_type = content_type

    if task_object.data is None or len(task_object.data) == 0:
        task_object.data = bytes()

    if content_type.lower() == 'application/json':
        task_object.data = json.dumps(data).encode('utf-8')

    elif content_type.lower() == 'application/python-pickle':
        task_object.data = pickle.dumps(data)

    elif content_type.lower() == 'application/octect-stream':
        if isinstance(data, bytes):
            task_object.data = data
        else:
            raise ValueError('Expected bytes')
    else:
        raise ValueError(f'Unhandled content_type {content_type}')


def try_serialise_json_then_pickle(task_object: Union[TaskRequest, TaskResult], data):
    try:
        serialise(task_object, data, content_type='application/json')
    except:
        serialise(task_object, data, content_type='application/python-pickle')
