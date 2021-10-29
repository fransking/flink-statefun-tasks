from statefun_tasks.messages_pb2 import TaskAction, TaskStatus
from statefun_tasks.pipeline import Pipeline

class _FlinkAction(object):
    def __init__(self, context, pipeline:Pipeline=None):
        self._context = context
        self._pipeline = pipeline
        
    def run(self, action_request):

        # TODO refactor into handlers
        if action_request.action == TaskAction.GET_STATUS:
            return self._get_task_status()

        elif action_request.action == TaskAction.GET_REQUEST:
            return self._get_task_request()

        elif action_request.action == TaskAction.GET_RESULT:
            return self._get_task_result()

        elif action_request.action == TaskAction.PAUSE_PIPELINE:
            return self._pause_pipeline()

        elif action_request.action == TaskAction.UNPAUSE_PIPELINE:
            return self._unpause_pipeline()

        elif action_request.action == TaskAction.CANCEL_PIPELINE:
            return self._cancel_pipeline()

        else:    
            raise ValueError(f'Unsupported task action {TaskAction.Name(action_request.action)}')

    def _get_task_status(self):
        if self._pipeline is not None:
            return self._pipeline.status(self._context)

        if self._context.storage.task_exception is not None:
            return TaskStatus(value=TaskStatus.Status.FAILED)

        if self._context.storage.task_result is not None:
            return TaskStatus(value=TaskStatus.Status.COMPLETED)

        return TaskStatus(value=TaskStatus.Status.PENDING)

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

    def _pause_pipeline(self):
        if self._pipeline is None:
            raise ValueError('Task is not a pipeline')
       
        self._pipeline.pause(self._context)

    def _unpause_pipeline(self):
        if self._pipeline is None:
            raise ValueError('Task is not a pipeline')
       
        self._pipeline.unpause(self._context)

    def _cancel_pipeline(self):
        if self._pipeline is None:
            raise ValueError('Task is not a pipeline')
       
        self._pipeline.cancel(self._context)
