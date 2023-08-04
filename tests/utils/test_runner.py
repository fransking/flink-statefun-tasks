from unittest.mock import MagicMock
from statefun_tasks import FlinkTasks, TaskRequest
from statefun_tasks.utils import _gen_id, _task_type_for
from statefun_tasks.handlers import TaskRequestHandler, TaskActionHandler
from statefun_tasks.messages_pb2 import TaskSpecificState, TaskRetryPolicy, TaskActionRequest


class TaskRunner():
    def __init__(self, tasks: FlinkTasks):
        self._tasks = tasks
        self._serialiser = tasks._serialiser

    def create_task_request(self, 
                            task_type, 
                            *args, 
                            reply_topic=None, 
                            reply_address=None, 
                            task_id=None, 
                            task_uid=None, 
                            retry_policy: TaskRetryPolicy=None, 
                            state=None,
                            **kwargs):
        
        request = TaskRequest(id=task_id or _gen_id(), uid=task_uid or _gen_id(), invocation_id=_gen_id(), type=task_type)

        if reply_topic is not None:
            request.reply_topic = reply_topic
        elif reply_address is not None:
            request.reply_address.CopyFrom(reply_address)
        
        args_and_kwargs = self._serialiser.serialise_args_and_kwargs(args, kwargs)
        self._serialiser.serialise_request(request, args_and_kwargs, state=state, retry_policy=retry_policy)
        return request

    async def run_task(self, 
                       task_or_task_type, 
                       *args, 
                       context=None, 
                       reply_topic=None, 
                       reply_address=None, 
                       caller_id=None,
                       caller_address="test/runner",
                       task_id=None,
                       task_uid=None,
                       task_state: TaskSpecificState=None,
                       retry_policy: TaskRetryPolicy=None,
                       state=None,
                       **kwargs):
        
        task_type = task_or_task_type if isinstance(task_or_task_type, str) else _task_type_for(task_or_task_type)

        task_request = self.create_task_request(task_type, 
                                                *args, 
                                                reply_topic=reply_topic, 
                                                reply_address=reply_address, 
                                                task_id=task_id, 
                                                task_uid=task_uid,
                                                retry_policy=retry_policy,
                                                state=state,
                                                **kwargs)

        caller_id = caller_id or _gen_id()

        context = context or MagicMock()

        context.to_address_and_id = lambda address: (f'{address.namespace}/{address.type}', address.id)
        context.get_caller_address = lambda: caller_address
        context.get_caller_id = lambda: caller_id
        context.get_original_caller_address = lambda: caller_address
        context.get_original_caller_id = lambda: caller_id
        context.task_state.by_uid = {task_request.uid: task_state or TaskSpecificState()}

        handler = TaskRequestHandler(embedded_pipeline_namespace="test", embedded_pipeline_type="embedded_pipeline")
        await handler.handle_message(self._tasks, context, task_request)

        try:
            result, state = self._serialiser.deserialise_result(context.storage.task_result)
            return result, state
        except AttributeError:
            try:
                task_exception = context.storage.task_exception
                raise ValueError(task_exception.exception_message)
            except AttributeError:
                return None

    async def run_action(self, task_id, action, context=None):
        task_action_request = TaskActionRequest(id=task_id, action=action, reply_topic='reply_topic')

        messages = []
        context = context or MagicMock()
        context.safe_send_egress_message = lambda a,b,c: messages.append(b)
        
        handler = TaskActionHandler()
        await handler.handle_message(self._tasks, context, task_action_request)

        return messages[0]
