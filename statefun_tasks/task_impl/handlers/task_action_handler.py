from statefun_tasks.context import TaskContext
from statefun_tasks.task_impl.handlers import MessageHandler
from statefun_tasks.types import TASK_ACTION_REQUEST_TYPE
from statefun_tasks.messages_pb2 import TaskAction, TaskStatus
from statefun_tasks.type_helpers import _create_task_result, _create_task_exception
from statefun import Message


class TaskActionHandler(MessageHandler):
    def __init__(self):
        pass

    def unpack(self, context: TaskContext, message: Message):
        if message.is_type(TASK_ACTION_REQUEST_TYPE):
            task_input = message.as_type(TASK_ACTION_REQUEST_TYPE)
            context.task_name = f'Action [{TaskAction.Name(task_input.action)}]'

            return task_input
        
        return None

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

        if context.storage.task_exception is not None:
            return TaskStatus(value=TaskStatus.Status.FAILED)

        if context.storage.task_result is not None:
            return TaskStatus(value=TaskStatus.Status.COMPLETED)

        return TaskStatus(value=TaskStatus.Status.PENDING)

    def _get_task_request(self, context):
        task_request = context.storage.task_request or None
    
        if task_request is not None:
            return task_request

        raise ValueError(f'Task request not found')

    def _get_task_result(self, context):

        task_result = context.storage.task_result or None
        if task_result is not None:
            return task_result

        task_exception = context.storage.task_exception or None
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
