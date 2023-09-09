from statefun_tasks.task_context import TaskContext
from statefun_tasks.handlers import MessageHandler
from statefun_tasks.types import TASK_REQUEST_TYPE
from statefun_tasks.messages_pb2 import TaskRequest, Address
from statefun import Message
from datetime import timedelta


class TaskRequestHandler(MessageHandler):
    def __init__(self, embedded_pipeline_namespace: str=None, embedded_pipeline_type: str=None, keep_task_state=True):
        self._embedded_pipeline_namespace = embedded_pipeline_namespace
        self._embedded_pipeline_type = embedded_pipeline_type
        self._keep_task_state = keep_task_state
        pass

    def unpack(self, context: TaskContext, message: Message):
        if message.is_type(TASK_REQUEST_TYPE):
            task_input = message.as_type(TASK_REQUEST_TYPE)
            
            context.task_name = task_input.type
            context.contextualise_from(task_input)

            return task_input
        
        return None

    async def handle_message(self, tasks: 'FlinkTasks', context: TaskContext, task_request: TaskRequest):
        await tasks.events.notify_task_received(context, task_request)
        
        context.storage.task_request = task_request

        # delete any previous result / exception from state
        del context.storage.task_result
        del context.storage.task_exception

        flink_task = tasks.get_task(task_request.type)

        # notify started
        await tasks.events.notify_task_started(context, task_request)
        
        # run task code
        task_result, task_exception, pipeline, is_fruitful = await flink_task.run(context, task_request)

        # notify finished
        await tasks.events.notify_task_finished(context, task_result, task_exception, is_pipeline=pipeline is not None)

        # if task returns a pipeline then forward to pipeline function
        if pipeline is not None:
            self._send_pipeline_request(context, task_request, task_result, is_fruitful)

        # if we have an task exception, attempt retry or return the error
        if task_exception is not None:
            if await self._attempt_retry(context, tasks, task_request, task_exception):
                return  # we have triggered a retry so ignore the result of this invocation

            context.storage.task_exception = task_exception
            await tasks.emit_result(context, task_request, task_exception)

        # else if we have a task result return it unless the result is a pipeline
        elif task_result is not None:
            context.storage.task_result = task_result

            if pipeline is None:
                await tasks.emit_result(context, task_request, task_result)

        if not self._keep_task_state:
            # clear state from intermediate tasks
            del context.storage.task_request
            del context.storage.task_result
            del context.storage.task_exception

    @staticmethod
    async def _attempt_retry(context, tasks, task_request, task_exception):
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
    
    def _send_pipeline_request(self, context, task_request, task_result, is_fruitful):
        if self._embedded_pipeline_namespace is None or self._embedded_pipeline_type is None:
            raise ValueError('Unable to forward pipeline to embedded pipeline function. \
                             Missing configuration in FlinkTasks(embedded_pipeline_namespace=..., embedded_pipeline_type=...)')

        pipeline_request = TaskRequest(
                id=task_request.id,
                uid=task_request.uid,
                invocation_id=task_request.invocation_id,
                type="__builtins.run_pipeline",
                is_fruitful=is_fruitful,
                meta=task_request.meta
        )

        pipeline_request.request.CopyFrom(task_result.result)
        pipeline_request.state.CopyFrom(task_result.state)

        # send the result of the pipeline to egress if reply_topic was specified
        if task_request.HasField('reply_topic'):
            pipeline_request.reply_topic = task_request.reply_topic

        # or call back to a particular flink function if reply_address was specified
        elif task_request.HasField('reply_address'):
            pipeline_request.reply_address.CopyFrom(task_request.reply_address)

        # otherwise call back to the caller (if there is one)
        else:
            address = context.get_original_caller_address()
            caller_id = context.get_original_caller_id()

            if address is not None and '/' in address and caller_id is not None:
                namespace, type = address.split('/')
                address = Address(namespace=namespace, type=type, id=caller_id)
                pipeline_request.reply_address.CopyFrom(address)
            
        context.send_message(f'{self._embedded_pipeline_namespace}/{self._embedded_pipeline_type}', pipeline_request.uid, pipeline_request)
