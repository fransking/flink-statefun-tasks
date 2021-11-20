from statefun_tasks.context import TaskContext
from statefun_tasks.task_impl.handlers import MessageHandler
from statefun_tasks.messages_pb2 import TaskActionRequest, TaskAction, TaskRequest, TaskStatus, TaskResult, TaskException
from statefun_tasks.type_helpers import _create_task_result, _create_task_exception


class TaskActionHandler(MessageHandler):
    def __init__(self):
        pass

    def can_handle(self, context: TaskContext, message):
        if isinstance(message, TaskActionRequest):
            context.task_name = f'Action [{TaskAction.Name(message.action)}]'
            return True

        return False

    async def handle_message(self, tasks: 'FlinkTasks', context: TaskContext, action_request):
        try:
            
            result = await self._handle(context, tasks, action_request)
            tasks.emit_result(context, action_request, _create_task_result(action_request, result))

        except Exception as ex:
            tasks.emit_result(context, action_request, _create_task_exception(action_request, ex))

    async def _handle(self, context, tasks, action_request):

        if action_request.action == TaskAction.GET_STATUS:
            return self._get_task_status(context, tasks)

        elif action_request.action == TaskAction.GET_REQUEST:
            return self._get_task_request(context)

        elif action_request.action == TaskAction.GET_RESULT:
            return self._get_task_result(context)

        elif action_request.action == TaskAction.PAUSE_PIPELINE:
            return await self._pause_pipeline(context, tasks)

        elif action_request.action == TaskAction.UNPAUSE_PIPELINE:
            return await self._unpause_pipeline(context, tasks)

        elif action_request.action == TaskAction.CANCEL_PIPELINE:
            return await self._cancel_pipeline(context, tasks)

        else:    
            raise ValueError(f'Unsupported task action {TaskAction.Name(action_request.action)}')

    def _get_task_status(self, context, tasks):
        pipeline = tasks.try_get_pipeline(context)

        if pipeline is not None:
            return pipeline.status(context)

        if context.unpack('task_exception', TaskException) is not None:
            return TaskStatus(value=TaskStatus.Status.FAILED)

        if context.unpack('task_result', TaskResult) is not None:
            return TaskStatus(value=TaskStatus.Status.COMPLETED)

        return TaskStatus(value=TaskStatus.Status.PENDING)

    def _get_task_request(self, context):
        task_request = context.unpack('task_request', TaskRequest)  or None
    
        if task_request is not None:
            return task_request

        raise ValueError(f'Task request not found')

    def _get_task_result(self, context):

        task_result = context.unpack('task_result', TaskResult) or None
        if task_result is not None:
            return task_result

        task_exception = context.unpack('task_exception', TaskException) or None
        if task_exception is not None:
            return task_exception

        raise ValueError(f'Task result not found')

    async def _pause_pipeline(self, context, tasks):
        pipeline = tasks.try_get_pipeline(context)

        if pipeline is None:
            raise ValueError('Task is not a pipeline')
       
        await pipeline.pause(context)

    async def _unpause_pipeline(self, context, tasks):
        pipeline = tasks.try_get_pipeline(context)

        if pipeline is None:
            raise ValueError('Task is not a pipeline')
       
        await pipeline.unpause(context)

    async def _cancel_pipeline(self, context, tasks):
        pipeline = tasks.try_get_pipeline(context)

        if pipeline is None:
            raise ValueError('Task is not a pipeline')
       
        await pipeline.cancel(context)
