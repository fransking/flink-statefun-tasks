from unittest.case import skip
from statefun_tasks.context import TaskContext
from statefun_tasks.pipeline_impl.handlers import PipelineMessageHandler
from statefun_tasks.types import Task, Group, TasksException
from statefun_tasks.messages_pb2 import TaskRequest, TaskResult, TaskException, TaskStatus
from typing import Union


class ContinuePipelineHandler(PipelineMessageHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
    
    def can_handle_message(self, context: TaskContext, message: Union[TaskRequest, TaskResult, TaskException]) -> bool:
        return context.pipeline_state is not None \
            and context.pipeline_state.status.value in [TaskStatus.RUNNING, TaskStatus.PAUSED] \
                and isinstance(message, (TaskResult, TaskException))

    async def handle_message(self, context: TaskContext, message: Union[TaskRequest, TaskResult, TaskException], pipeline, **kwargs):
        task_result_or_exception = message
        caller_id = context.get_caller_id()

        # mark pipeline step as complete
        self.graph.mark_task_complete(caller_id, task_result_or_exception)

        # notify event handler (with option to cancel)
        try:
            pipeline.events.notify_pipeline_task_finished(context, task_result_or_exception)
        except TasksException as ex:
            await pipeline.cancel(context, ex)
            return False, task_result_or_exception

        # release deferred tasks if they can now run
        await self.submitter.release_tasks(context, caller_id, task_result_or_exception)

        # get the next step of the pipeline to run (if any)
        current_step, next_step, group, empty_group = self.graph.get_next_step_in_pipeline(caller_id)

        # if this task is part group then we need to record the results so we can aggregate later
        if group is not None:
            await self.result_aggregator.add_result(context, group, caller_id, task_result_or_exception)

            # once the group is complete aggregate the results
            if group.is_complete():
                task_result_or_exception = await self.result_aggregator.aggregate(context, group)

                # pause the pipeline if this completed group is a wait
                if group.is_wait:
                    await pipeline.pause(context)
        else:
            # pause the pipeline if this task is a wait
            if current_step.is_wait:  
                await pipeline.pause(context)

        # if we got an exception then the next step is the finally_task if there is one (or none otherwise)
        if isinstance(task_result_or_exception, TaskException):
            next_step = self.graph.try_get_finally_task(caller_id)

        # else if we came across an empty group between this task and next_entry then our result must be an empty array []
        # as we cannot call an empty group but can synthesise the result (remembering to pass through state)
        elif empty_group:
            _, state = self._serialiser.deserialise_result(task_result_or_exception)
            self._serialiser.serialise_result(task_result_or_exception, ([]), state)

        # turn next step into remainder of tasks to call
        if isinstance(next_step, Task):
            tasks = [next_step]
            max_parallelism = 1

            if next_step.is_finally:
                # record the result of the task prior to the finally task so we can return it once the finally task completes
                pipeline.save_result_before_finally(context, task_result_or_exception)
            
        elif isinstance(next_step, Group):
            tasks, max_parallelism, _ = self.graph.get_initial_tasks(group=next_step)
        else:
            tasks = []

        if any(tasks):
            # split into tasks to call now and those to defer if max parallelism is exceeded
            await self.submitter.submit_tasks(context, tasks, task_result_or_exception=task_result_or_exception, max_parallelism=max_parallelism)

        else:
            last_step = self._pipeline[-1]

            if last_step.is_complete():
                # if we are at the last step in the pipeline then we are complete
                context.pipeline_state.status.value = TaskStatus.COMPLETED if isinstance(task_result_or_exception, TaskResult) else TaskStatus.FAILED
                pipeline.events.notify_pipeline_status_changed(context, context.pipeline_state.pipeline, context.pipeline_state.status.value)


            elif isinstance(task_result_or_exception, TaskException):
                if group is None or group.is_complete():
                    # else if have an exception then we failed but waiting for any parallel tasks in the group to complete first
                    context.pipeline_state.status.value = TaskStatus.FAILED
                    pipeline.events.notify_pipeline_status_changed(context, context.pipeline_state.pipeline, context.pipeline_state.status.value)

        # continue
        return True, task_result_or_exception
