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
    

def _invoke_task_action(context, task_action):
    if task_action.action == TaskAction.GET_STATUS:
        return _get_task_status(context)

    elif task_action.action == TaskAction.GET_REQUEST:
        return _get_task_request(context)

    else:    
        raise ValueError(f'Unsupported task action {TaskAction.Name(task_action.action)}')
