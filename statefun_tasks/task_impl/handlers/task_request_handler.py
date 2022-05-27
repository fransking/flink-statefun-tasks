from statefun_tasks.context import TaskContext
from statefun_tasks.task_impl.handlers import MessageHandler
from statefun_tasks.types import TASK_REQUEST_TYPE, PipelineInProgress
from statefun_tasks.type_helpers import _create_task_exception
from statefun_tasks.messages_pb2 import TaskRequest
from statefun import Message
from datetime import timedelta


class TaskRequestHandler(MessageHandler):
    def __init__(self):
        pass

    def unpack(self, context: TaskContext, message: Message):
        if message.is_type(TASK_REQUEST_TYPE):
            task_input = message.as_type(TASK_REQUEST_TYPE)
            
            context.task_name = task_input.type
            context.contextualise_from(task_input)

            return task_input
        
        return None

    async def handle_message(self, tasks: 'FlinkTasks', context: TaskContext, task_request: TaskRequest):
        context.storage.task_request = task_request

        # delete any previous result / exception from state
        del context.storage.task_result
        del context.storage.task_exception

        flink_task = tasks.get_task(task_request.type)

        # notify started
        await tasks.events.notify_task_started(context, task_request)
        
        # run task code
        task_result, task_exception, pipeline, state = await flink_task.run(context, task_request)

        # if a pipeline is retured then try to reset its state in case it already exists
        if pipeline is not None:
            try:
                pipeline.reset(context)
            except PipelineInProgress as ex:  # can't re-run a running pipeline.  Must wait for it to finish
                task_exception, pipeline = _create_task_exception(task_request, ex), None

        # notify finished
        await tasks.events.notify_task_finished(context, task_result, task_exception, is_pipeline=pipeline is not None)

        # if task returns a pipeline then start it if we can
        if pipeline is not None and await pipeline.handle_message(context, task_request, state):
            ()

        # else if we have an task exception, attempt retry or return the error
        elif task_exception is not None:
            if await self._attempt_retry(context, tasks, task_request, task_exception):
                return  # we have triggered a retry so ignore the result of this invocation

            context.storage.task_result = task_exception
            tasks.emit_result(context, task_request, task_exception)

        # else if we have a task result return it
        elif task_result is not None:
            context.storage.task_exception = task_result
            tasks.emit_result(context, task_request, task_result)

    async def _attempt_retry(self, context, tasks, task_request, task_exception):
        task_state = context.task_state.by_uid[task_request.uid]

        if task_exception.maybe_retry and task_exception.retry_policy is not None:           
            if task_state.retry_count >= task_exception.retry_policy.max_retries:
                return False

            # increment retry count
            task_state.retry_count += 1

            # save the original caller address and id for this task_request prior to calling back to ourselves which would overwrite
            if task_state.original_caller_id == '':
                task_state.original_caller_id = context.get_caller_id()
                task_state.original_caller_address = context.get_caller_address()

            # remove previous task_request from state
            del context.storage.task_request

            # calculate delay
            if task_exception.retry_policy.exponential_back_off:
                delay = timedelta(milliseconds=task_exception.retry_policy.delay_ms * (2 ** task_state.retry_count))
            else:
                delay = timedelta(milliseconds=task_exception.retry_policy.delay_ms)

            # send retry
            context.send_message(context.get_address(), context.get_task_id(), task_request, delay)

            # notify retry
            await tasks.events.notify_task_retry(context, task_request, task_state.retry_count)

            return True

        return False
