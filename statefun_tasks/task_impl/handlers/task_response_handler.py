from statefun_tasks.context import TaskContext
from statefun_tasks.task_impl.handlers import MessageHandler
from statefun_tasks.types import TASK_RESULT_TYPE, TASK_EXCEPTION_TYPE
from statefun import Message


class TaskResponseHandler(MessageHandler):
    def __init__(self):
        pass

    def unpack(self, context: TaskContext, message: Message):
        task_input = None
        
        if message.is_type(TASK_RESULT_TYPE):
            task_input = message.as_type(TASK_RESULT_TYPE)
        
        elif message.is_type(TASK_EXCEPTION_TYPE):
            task_input = message.as_type(TASK_EXCEPTION_TYPE)
        
        if task_input is not None:
            context.task_name = task_input.type

        task_request = context.storage.task_request
        if task_request is not None:
            context.contextualise_from(task_request)

        return task_input

    async def handle_message(self, tasks: 'FlinkTasks', context: TaskContext, task_result_or_exception):
        pipeline = tasks.get_pipeline(context)
        await pipeline.handle_message(context, task_result_or_exception)
