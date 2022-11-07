from statefun_tasks.context import TaskContext
from statefun_tasks.pipeline_impl.handlers import PipelineMessageHandler
from statefun_tasks.types import Task, Group, TasksException
from statefun_tasks.type_helpers import _create_task_result
from statefun_tasks.messages_pb2 import TaskRequest, TaskResult, TaskException, TaskStatus
from typing import Union


class ContinuePipelineHandler(PipelineMessageHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
    
    def can_handle_message(self, context: TaskContext, message: Union[TaskRequest, TaskResult, TaskException]) -> bool:
        return context.pipeline_state is not None \
            and context.pipeline_state.invocation_id == message.invocation_id \
                and context.pipeline_state.status.value in [TaskStatus.RUNNING, TaskStatus.PAUSED] \
                    and isinstance(message, (TaskResult, TaskException))

    async def handle_message(self, context: TaskContext, message: Union[TaskRequest, TaskResult, TaskException], pipeline, **kwargs):
        task_result_or_exception = message
        caller_uid = message.uid

        # update last known task state in case we need to cancel later and call a finally task passing through the current state
        context.pipeline_state.last_task_state.CopyFrom(task_result_or_exception.state)          

        # mark pipeline step as complete
        if not self.graph.mark_task_complete(caller_uid, task_result_or_exception):
            return False, task_result_or_exception

        # notify event handler (with option to cancel)
        try:
            await pipeline.events.notify_pipeline_task_finished(context, task_result_or_exception)
        except TasksException as ex:
            await pipeline.cancel(context, ex)
            return False, task_result_or_exception

        # release deferred tasks if they can now run
        self.submitter.release_tasks(context, caller_uid, task_result_or_exception)

        # get the next step of the pipeline to run (if any)
        current_step, next_step, group, empty_group, skipped_tasks = self.graph.get_next_step_in_pipeline(caller_uid, task_result_or_exception)

        # if this task is part group then we need to record the results so we can aggregate later
        if group is not None:
            self.result_aggregator.add_result(context, caller_uid, task_result_or_exception)

            # once the group is complete aggregate the results
            if group.is_complete():
                task_result_or_exception = self.result_aggregator.aggregate(context, group)

                # we may now have a TaskResult instead of TaskException so we remark the graph and get the next
                # steps based on our overall group result or exception
                self.graph.mark_task_complete(caller_uid, task_result_or_exception, remark=True)
                current_step, next_step, group, empty_group, skipped_tasks = self.graph.get_next_step_in_pipeline(caller_uid, task_result_or_exception)

                # update last known task state in case we need to cancel later and call a finally task passing through the current state
                context.pipeline_state.last_task_state.CopyFrom(task_result_or_exception.state,)   

                # pause the pipeline if this completed group is a wait
                if group.is_wait:
                    await pipeline.pause(context)
        else:
            # pause the pipeline if this task is a wait
            if current_step.is_wait:  
                await pipeline.pause(context)

        # notify event handlers of any skipped tasks such as exceptionally tasks that are passed over if we don't have a TaskException
        await pipeline.events.notify_pipeline_tasks_skipped(context, skipped_tasks)

        # if we got an exception then if we have an exceptionally, pass the exception as a result to this task
        if isinstance(task_result_or_exception, TaskException):

            if next_step is not None and next_step.is_exceptionally:
                task_exception = task_result_or_exception
                task_result_or_exception = _create_task_result(task_exception)
                self._serialiser.serialise_result(task_result_or_exception, task_exception, task_result_or_exception.state)

        else:
            # if we came across an empty group between this task and next_entry then our result must be an empty array []
            # as we cannot call an empty group but can synthesise the result (remembering to pass through state)
            if empty_group:
                _, state = self._serialiser.deserialise_result(task_result_or_exception)
                self._serialiser.serialise_result(task_result_or_exception, ([]), state)

        # turn next step into remainder of tasks to call
        if isinstance(next_step, Task):
            tasks, max_parallelism = [next_step], 1

            if next_step.is_finally:
                # record the result of the task prior to the finally task so we can return it once the finally task completes
                pipeline.save_result_before_finally(context, task_result_or_exception)
            
        elif isinstance(next_step, Group):
            tasks, max_parallelism, _ = self.graph.get_initial_tasks(group=next_step)
        else:
            tasks = []

        if any(tasks):
            # split into tasks to call now and those to defer if max parallelism is exceeded
            self.submitter.submit_tasks(context, tasks, task_result_or_exception=task_result_or_exception, max_parallelism=max_parallelism)

        else:
            last_step = self._pipeline[-1]

            if last_step.is_complete():
                # if we are at the last step in the pipeline then we are complete
                context.pipeline_state.status.value = TaskStatus.COMPLETED if isinstance(task_result_or_exception, TaskResult) else TaskStatus.FAILED
                await pipeline.events.notify_pipeline_status_changed(context, context.pipeline_state.pipeline, context.pipeline_state.status.value)

            elif isinstance(task_result_or_exception, TaskException):
                if group is None or group.is_complete():
                    # else if have an exception then we failed but waiting for any parallel tasks in the group to complete first
                    context.pipeline_state.status.value = TaskStatus.FAILED
                    await pipeline.events.notify_pipeline_status_changed(context, context.pipeline_state.pipeline, context.pipeline_state.status.value)

        # continue
        return True, task_result_or_exception
