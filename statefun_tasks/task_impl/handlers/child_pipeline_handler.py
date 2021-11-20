from statefun_tasks.context import TaskContext
from statefun_tasks.task_impl.handlers import MessageHandler
from statefun_tasks.messages_pb2 import ChildPipeline


class ChildPipelineHandler(MessageHandler):
    def __init__(self):
        pass

    def can_handle(self, context: TaskContext, message):
        if isinstance(message, ChildPipeline):
            context.task_name = f'New child pipeline: {message.id}'
            return True

        return False

    async def handle_message(self, tasks: 'FlinkTasks', context: TaskContext, child_pipeline):
        pipeline = tasks.try_get_pipeline(context)
        
        if pipeline is not None:
            pipeline.add_child(context, child_pipeline)
