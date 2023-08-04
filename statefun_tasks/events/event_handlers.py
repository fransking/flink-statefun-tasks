from statefun_tasks.messages_pb2 import TaskRequest, TaskResult, TaskException
from statefun_tasks.types import TasksException
from asyncio import iscoroutine


class EventHandlers(object):
    def __init__(self):
        self._on_task_received_handlers = []
        self._on_task_started_handlers = []
        self._on_task_finished_handlers = []
        self._on_task_retry_handlers = []
        self._on_emit_result_handlers = []

    def on_task_received(self, handler):
        """
        @tasks.events.on_task_received decorator
        """
        self._on_task_received_handlers.append(handler)

    def on_task_started(self, handler):
        """
        @tasks.events.on_task_started decorator
        """
        self._on_task_started_handlers.append(handler)

    def on_task_finished(self, handler):
        """
        @tasks.events.on_task_finished decorator
        """
        self._on_task_finished_handlers.append(handler)

    def on_task_retry(self, handler):
        """
        @tasks.events.on_task_retry decorator
        """
        self._on_task_retry_handlers.append(handler)

    def on_emit_result(self, handler):
        """
        @tasks.events.on_emit_result decorator
        """
        self._on_emit_result_handlers.append(handler)

    async def notify_task_received(self, context, task_request: TaskRequest):
        """
        Calls all on_task_received event handlers
        """
        await self._notify_all(self._on_task_received_handlers, context, task_request)

    async def notify_task_started(self, context, task_request: TaskRequest):
        """
        Calls all on_task_started event handlers
        """
        await self._notify_all(self._on_task_started_handlers, context, task_request)

    async def notify_task_finished(self, context, task_result: TaskResult, task_exception: TaskException, is_pipeline: bool):
        """
        Calls all notify_task_finished event handlers
        """
        await self._notify_all(self._on_task_finished_handlers, context, task_result=task_result, task_exception=task_exception, is_pipeline=is_pipeline)

    async def notify_task_retry(self, context, task_request: TaskRequest, retry_count):
        """
        Calls all notify_task_retry event handlers
        """
        await self._notify_all(self._on_task_retry_handlers, context, task_request, retry_count)

    async def notify_emit_result(self, context, task_result_or_exception):
        """
        Calls all notify_emit_result event handlers
        """
        if isinstance(task_result_or_exception, TaskResult):
            task_result, task_exception = task_result_or_exception, None
        else:
            task_result, task_exception = None, task_result_or_exception

        await self._notify_all(self._on_emit_result_handlers, context, raise_tasks_exceptions=False, task_result=task_result, task_exception=task_exception)

    @staticmethod
    async def _notify_all(handlers, *args, raise_tasks_exceptions=True, **kwargs):
        for handler in handlers:
            try:
                res = handler(*args, **kwargs)

                if iscoroutine(res):
                    await res
                    
            except TasksException:
                if raise_tasks_exceptions:
                    raise
            except:
                pass
