from statefun_tasks.types import _VALUE_TYPE_MAP
from statefun_tasks.utils import _type_name
from statefun_tasks.protobuf import pack_any
from statefun_tasks.messages_pb2 import TaskRequest, TaskResult, TaskException, TaskActionRequest, TaskActionResult, TaskActionException

import traceback as tb


def flink_value_type_for(proto):
    proto_type = type(proto)
    value_type = _VALUE_TYPE_MAP.get(proto_type, None)

    if value_type is not None:
        return value_type
    
    raise ValueError(f'No Flink value type found for proto type {proto_type}')


def _create_task_exception(task_input, ex):
    if isinstance(task_input, TaskActionRequest):
        return TaskActionException(
            id=task_input.id,
            action = task_input.action,
            exception_type=_type_name(ex),
            exception_message=str(ex),
            stacktrace=tb.format_exc())
    else:
        task_exception = TaskException(
            id=task_input.id,
            type=f'{task_input.type}.error',
            exception_type=_type_name(ex),
            exception_message=str(ex),
            stacktrace=tb.format_exc())

        if isinstance(task_input, TaskRequest) and task_input.HasField('state'):
            task_exception.state.CopyFrom(task_input.state)

        return task_exception


def _create_task_result(task_input, result=None):
    if isinstance(task_input, TaskActionRequest):
        task_result = TaskActionResult(
            id=task_input.id,
            action = task_input.action)
    else:
        task_result = TaskResult(
            id=task_input.id,
            type=f'{task_input.type}.result')

        if task_input.HasField('state'):
            task_result.state.CopyFrom(task_input.state)

    if result is not None:
        task_result.result.CopyFrom(pack_any(result))

    return task_result
