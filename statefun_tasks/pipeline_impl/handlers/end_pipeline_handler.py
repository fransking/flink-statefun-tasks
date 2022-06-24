from statefun_tasks.context import TaskContext
from statefun_tasks.pipeline_impl.handlers import PipelineMessageHandler
from statefun_tasks.messages_pb2 import TaskRequest, TaskResult, TaskException, TaskStatus
from statefun_tasks.types import TasksException
from typing import Union


class EndPipelineHandler(PipelineMessageHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
    
    def can_handle_message(self, context: TaskContext, message: Union[TaskRequest, TaskResult, TaskException]) -> bool:        
        return context.pipeline_state is not None \
            and context.pipeline_state.invocation_id == message.invocation_id \
                and context.pipeline_state.status.value in [TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED] \
                    and isinstance(message, (TaskResult, TaskException))

    async def handle_message(self, context: TaskContext, message: Union[TaskRequest, TaskResult, TaskException], pipeline, **kwargs):
        task_result_or_exception = message
                
        task_request = context.storage.task_request or TaskRequest()

        # if result_before_finally or exception_before_finally are set then we are in a finally block
        result_before_finally = pipeline.get_result_before_finally(context)

        if result_before_finally is not None and isinstance(task_result_or_exception, TaskResult):
            # finally ran successfully, so return the result of the previous task (rather than cleanup result from finally)
            task_result_or_exception = result_before_finally

        # notify event handler (with option to cancel)
        try:
            await pipeline.events.notify_pipeline_finished(context, context.pipeline_state.pipeline, task_result_or_exception)
        except TasksException as ex:
            context.pipeline_state.status.value = TaskStatus.Status.RUNNING  # reset to running so we can cancel
            await pipeline.cancel(context, ex)
            return False, task_result_or_exception

        # set basic message properties
        task_result_or_exception.id = task_request.id
        task_result_or_exception.uid = task_request.uid
        task_result_or_exception.invocation_id = task_request.invocation_id
        
        task_result_or_exception.type = f'{task_request.type}.' + (
            'result' if isinstance(task_result_or_exception, TaskResult) else 'error')

        # pass back any state that we were given at the start of the pipeline
        if not context.pipeline_state.pipeline.inline:
            task_result_or_exception.state.CopyFrom(context.pipeline_state.task_state)

        # finally emit the result (to egress, destination address or caller address)
        self.result_emitter.emit_result(context, task_request, task_result_or_exception)
        
        # break
        return False, task_result_or_exception
