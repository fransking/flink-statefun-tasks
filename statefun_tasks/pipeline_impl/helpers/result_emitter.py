from statefun_tasks.context import TaskContext
from statefun_tasks.messages_pb2 import TaskResult, TupleOfAny
from statefun_tasks.serialisation import pack_any
from statefun_tasks.types import MessageSizeExceeded
from statefun_tasks.type_helpers import _create_task_exception
import logging

_log = logging.getLogger('FlinkTasks')


class ResultEmitter(object):
    __slots__ = ()

    def __init__(self):
        pass

    async def emit_result(self, context: TaskContext, task_request, task_result_or_exception, pipeline: '_Pipeline'):
        await pipeline.events.notify_emit_result(context, task_result_or_exception)

        # the result of this task is the result of the pipeline
        if isinstance(task_result_or_exception, TaskResult):

            # if we are not a fruitful pipeline then zero out the result
            if not context.pipeline_state.is_fruitful:
                task_result_or_exception.result.CopyFrom(pack_any(TupleOfAny()))

            context.pack_and_save('task_result', task_result_or_exception)
        else:
            # task failed or was cancelled
            context.pack_and_save('task_exception', task_result_or_exception)

        # either send a message to egress if reply_topic was specified
        if task_request.HasField('reply_topic'):
            self._send_egress_message(context, task_request, task_result_or_exception)

        # or call back to a particular flink function if reply_address was specified
        elif task_request.HasField('reply_address'):
            address, identifer = context.to_address_and_id(task_request.reply_address)
            context.pack_and_send(address, identifer, task_result_or_exception)

        # or call back to our caller (if there is one)
        elif context.pipeline_state.caller_id is not None:
            context.pack_and_send(context.pipeline_state.caller_address, context.pipeline_state.caller_id, task_result_or_exception)

    def _send_egress_message(self, context, task_request, task_result_or_exception):
        try:
            context.pack_and_send_egress(topic=task_request.reply_topic, value=task_result_or_exception)
        except MessageSizeExceeded as e:
            _log.warning(f'Unable to send egress message - {e}')
            context.pack_and_send_egress(topic=task_request.reply_topic, value=_create_task_exception(task_request, e))
