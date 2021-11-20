from statefun_tasks.context import TaskContext
from statefun_tasks.task_impl.handlers import MessageHandler
from statefun_tasks.messages_pb2 import TaskResult, TaskException


class TaskResponseHandler(MessageHandler):
    def __init__(self):
        pass

    def can_handle(self, context: TaskContext, message):
        if isinstance(message, (TaskResult, TaskException)):
            context.task_name = message.type
            return True

        return False

    async def handle_message(self, tasks: 'FlinkTasks', context: TaskContext, task_result_or_exception):
        pipeline = tasks.get_pipeline(context)
        await pipeline.handle_message(context, task_result_or_exception)
