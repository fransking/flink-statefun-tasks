from statefun_tasks.context import TaskContext
from statefun_tasks.task_impl.handlers import MessageHandler
from statefun_tasks.types import CHILD_PIPELINE_TYPE
from statefun import Message


class ChildPipelineHandler(MessageHandler):
    def __init__(self):
        pass

    def unpack(self, context: TaskContext, message: Message):       
        if message.is_type(CHILD_PIPELINE_TYPE):
            task_input = message.as_type(CHILD_PIPELINE_TYPE)
            context.task_name = f'New child pipeline: {task_input.id}'
            
            return task_input

        return None

    async def handle_message(self, tasks: 'FlinkTasks', context: TaskContext, child_pipeline):
        pipeline = tasks.try_get_pipeline(context)
        
        if pipeline is not None:
            pipeline.add_child(context, child_pipeline)
