from statefun_tasks.context import TaskContext
from statefun_tasks.pipeline_impl.handlers import PipelineMessageHandler
from statefun_tasks.messages_pb2 import TaskRequest, TaskResult, TaskException, TaskStatus
from typing import Union


class CancelPipelineHandler(PipelineMessageHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
    
    def can_handle_message(self, context: TaskContext, message: Union[TaskRequest, TaskResult, TaskException]) -> bool:        
        return context.pipeline_state is not None \
            and (context.pipeline_state.status.value == TaskStatus.CANCELLING or context.pipeline_state.status.value == TaskStatus.CANCELLED) \
                and isinstance(message, (TaskResult, TaskException))

    def handle_message(self, context: TaskContext, message: Union[TaskRequest, TaskResult, TaskException], **kwargs):
        task_result_or_exception = message
        caller_id = context.get_caller_id()

        # mark pipeline step as complete
        self.graph.mark_task_complete(caller_id, task_result_or_exception)
                
        # wait for the finally task or our own task cancellation message to reach us
        if context.pipeline_state.status.value == TaskStatus.CANCELLING:
            if caller_id == context.pipeline_state.id or self.graph.is_finally_task(caller_id):
                context.pipeline_state.status.value = TaskStatus.CANCELLED

                # continue (into EndPipelineHandler)
                return True, task_result_or_exception

        # break otherwise
        return False, message