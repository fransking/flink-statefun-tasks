from statefun_tasks.utils import _type_name
from statefun_tasks.protobuf import pack_any
from statefun_tasks.messages_pb2 import TaskRequest, TaskResult, TaskException, TaskActionRequest, TaskActionResult, TaskActionException

import traceback as tb


def _create_task_exception(task_input, ex, state=None):
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

        # if the task failed then ensure that exception retains the state from the task input (i.e. the TaskRequest)
        if state is not None:
            task_exception.state.CopyFrom(state)
        elif isinstance(task_input, TaskRequest) and task_input.HasField('state'):
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

    if result is not None:
        task_result.result.CopyFrom(pack_any(result))

    return task_result
