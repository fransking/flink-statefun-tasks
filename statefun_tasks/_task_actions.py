from .messages_pb2 import TaskRequest, TaskResult, TaskException, TaskActionResult, TaskAction, TaskStatus


def _get_task_status(context):
    if context.unpack('task_exception', TaskException) is not None:
        return TaskStatus(status=TaskStatus.Status.FAILED)
    elif context.unpack('task_result', TaskResult) is not None:
        return TaskStatus(status=TaskStatus.Status.COMPLETED)
    else:
        return TaskStatus(status=TaskStatus.Status.PENDING)


def _get_task_request(context):
    task_request = context.unpack('task_request', TaskRequest)
    if task_request is not None:
        return task_request
    raise ValueError(f'Task request not found')


def _get_task_result(context):
    task_result = context.unpack('task_result', TaskResult)
    if task_result is not None:
        return task_result

    task_exception = context.unpack('task_exception', TaskException)
    if task_exception is not None:
        return task_exception

    raise ValueError(f'Task result not found')
    

def _invoke_task_action(context, task_action):
    if task_action.action == TaskAction.GET_STATUS:
        return _get_task_status(context)

    elif task_action.action == TaskAction.GET_REQUEST:
        return _get_task_request(context)

    elif task_action.action == TaskAction.GET_RESULT:
        return _get_task_result(context)

    else:    
        raise ValueError(f'Unsupported task action {TaskAction.Name(task_action.action)}')
