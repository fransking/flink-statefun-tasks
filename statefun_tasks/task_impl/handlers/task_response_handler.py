from statefun_tasks.context import TaskContext
from statefun_tasks.task_impl.handlers import MessageHandler
from statefun_tasks.messages_pb2 import TaskResult, TaskException, TaskRequest


class TaskResponseHandler(MessageHandler):
    def __init__(self):
        pass

    def can_handle(self, context: TaskContext, message):
        if isinstance(message, (TaskResult, TaskException)):
            context.task_name = message.type

            task_request = context.unpack('task_request', TaskRequest)
            if task_request is not None:
                context.contextualise_from(task_request)

            return True

        return False

    async def handle_message(self, tasks: 'FlinkTasks', context: TaskContext, task_result_or_exception):
        pipeline = tasks.get_pipeline(context)
        await pipeline.handle_message(context, task_result_or_exception)
