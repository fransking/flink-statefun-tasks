from statefun_tasks.messages_pb2 import TaskRequest, TaskResult, TaskException, Pipeline, TaskStatus
from statefun_tasks.types import TasksException


class EventHandlers(object):
    def __init__(self):
        self._on_task_started_handlers = []
        self._on_task_finished_handlers = []
        self._on_task_retry_handlers = []
        self._on_pipeline_created_handlers = []
        self._on_pipeline_status_changed = []
        self._on_pipeline_task_finished_handlers = []
        self._on_pipeline_finished_handlers = []

    def on_task_started(self, handler):
        self._on_task_started_handlers.append(handler)

    def on_task_finished(self, handler):
        self._on_task_finished_handlers.append(handler)

    def on_task_retry(self, handler):
        self._on_task_retry_handlers.append(handler)

    def on_pipeline_created(self, handler):
        self._on_pipeline_created_handlers.append(handler)

    def on_pipeline_status_changed(self, handler):
        self._on_pipeline_status_changed.append(handler)

    def on_pipeline_task_finished(self, handler):
        self._on_pipeline_task_finished_handlers.append(handler)

    def on_pipeline_finished(self, handler):
        self._on_pipeline_finished_handlers.append(handler)

    def notify_task_started(self, context, task_request: TaskRequest):
        self._notify_all(self._on_task_started_handlers, context, task_request)

    def notify_task_finished(self, context, task_result: TaskResult, task_exception: TaskException, is_pipeline: bool):
        self._notify_all(self._on_task_finished_handlers, context, task_result=task_result, task_exception=task_exception, is_pipeline=is_pipeline)

    def notify_task_retry(self, context, task_request: TaskRequest, retry_count):
        self._notify_all(self._on_task_retry_handlers, context, task_request, retry_count)

    def notify_pipeline_created(self, context, pipeline: Pipeline):
        self._notify_all(self._on_pipeline_created_handlers, context, pipeline)

    def notify_pipeline_status_changed(self, context, pipeline: Pipeline, status: TaskStatus):
        self._notify_all(self._on_pipeline_status_changed, context, pipeline, status)

    def notify_pipeline_task_finished(self, context, task_result_or_exception):
        if isinstance(task_result_or_exception, TaskResult):
            task_result, task_exception = task_result_or_exception, None
        else:
            task_result, task_exception = None, task_result_or_exception

        self._notify_all(self._on_pipeline_task_finished_handlers, context, task_result=task_result, task_exception=task_exception)

    def notify_pipeline_finished(self, context, pipeline, task_result_or_exception):
        if isinstance(task_result_or_exception, TaskResult):
            task_result, task_exception = task_result_or_exception, None
        else:
            task_result, task_exception = None, task_result_or_exception

        self._notify_all(self._on_pipeline_finished_handlers, context, pipeline, task_result=task_result, task_exception=task_exception)


    @staticmethod
    def _notify_all(handlers, *args, **kwargs):
        for handler in handlers:
            try:
                handler(*args, **kwargs)
            except TasksException:
                raise
            except:
                pass
