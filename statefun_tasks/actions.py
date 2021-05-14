from statefun_tasks.messages_pb2 import TaskAction, TaskStatus


class _FlinkAction(object):
    def __init__(self, context):
        self._context = context
        
    def run(self, action_request):
        if action_request.action == TaskAction.GET_STATUS:
            return self._get_task_status()

        elif action_request.action == TaskAction.GET_REQUEST:
            return self._get_task_request()

        elif action_request.action == TaskAction.GET_RESULT:
            return self._get_task_result()

        else:    
            raise ValueError(f'Unsupported task action {TaskAction.Name(action_request.action)}')

    def _get_task_status(self):

        if self._context.storage.task_exception is not None:
            return TaskStatus(status=TaskStatus.Status.FAILED)

        if self._context.storage.task_result is not None:
            return TaskStatus(status=TaskStatus.Status.COMPLETED)

        return TaskStatus(status=TaskStatus.Status.PENDING)

    def _get_task_request(self):
        task_request = self._context.storage.task_request or None
    
        if task_request is not None:
            return task_request

        raise ValueError(f'Task request not found')


    def _get_task_result(self):
        task_result = self._context.storage.task_result or None
        if task_result is not None:
            return task_result

        task_exception = self._context.storage.task_exception or None
        if task_exception is not None:
            return task_exception

        raise ValueError(f'Task result not found')
