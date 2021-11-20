from statefun_tasks import events
from statefun_tasks.context import TaskContext
from statefun_tasks.task_impl.handlers import MessageHandler
from statefun_tasks.types import TASK_REQUEST_TYPE, TaskAlreadyExistsException
from statefun import Message
from datetime import timedelta


class TaskRequestHandler(MessageHandler):
    def __init__(self):
        pass

    def unpack(self, context: TaskContext, message: Message):
        if message.is_type(TASK_REQUEST_TYPE):
            task_input = message.as_type(TASK_REQUEST_TYPE)
            
            context.task_name = task_input.type
            context.apply_task_meta(task_input)

            return task_input
        
        return None

    async def handle_message(self, tasks: 'FlinkTasks', context: TaskContext, task_request):
        if context.storage.task_request is not None:
            # don't allow tasks to be overwritten
            raise TaskAlreadyExistsException(f'Task already exists: {task_request.id}')

        context.storage.task_request = task_request

        flink_task = tasks.get_task(task_request.type)

        # notify started
        tasks.events.notify_task_started(context, task_request)
        
        task_result, task_exception, pipeline, task_state = await flink_task.run(context, task_request)
        
        # notify finished
        tasks.events.notify_task_finished(context, task_result, task_exception, is_pipeline=pipeline is not None)

        # if task returns a pipeline then start it if we can
        if pipeline is not None and await pipeline.handle_message(context, task_request, task_state):
            ()

        # else if we have a task result return it
        elif task_result is not None:
            context.storage.task_result = task_result
            tasks.emit_result(context, task_request, task_result)

        # else if we have an task exception, attempt retry or return the error
        elif task_exception is not None:
            if self._attempt_retry(context, tasks, task_request, task_exception):
                return  # we have triggered a retry so ignore the result of this invocation

            context.storage.task_exception = task_exception
            tasks.emit_result(context, task_request, task_exception)

    def _attempt_retry(self, context, tasks, task_request, task_exception):
        if task_exception.maybe_retry and task_exception.retry_policy is not None:           
            if context.task_state.retry_count >= task_exception.retry_policy.max_retries:
                return False

            # save the original caller address and id for this task_request prior to calling back to ourselves which would overwrite
            if context.task_state.original_caller_id == '':
                context.task_state.original_caller_id = context.get_caller_id()
                context.task_state.original_caller_address = context.get_caller_address()

            # remove previous task_request from state, increment retry count
            del context.storage.task_request
            context.task_state.retry_count += 1

            # send retry
            delay = timedelta(milliseconds=task_exception.retry_policy.delay_ms)

            if task_exception.retry_policy.exponential_back_off:
                delay = timedelta(milliseconds=task_exception.retry_policy.delay_ms * (2 ** context.task_state.retry_count))

            if delay:
                context.send_message_after(delay, context.get_address(), context.get_task_id(), task_request)
            else:
                context.send_message(context.get_address(), context.get_task_id(), task_request)

            # notify retry
            tasks.events.notify_task_retry(context, task_request, context.task_state.retry_count)

            return True

        return False
