from statefun_tasks import TaskException, TaskActionResult, TaskStatus as TaskStatusProto
from dataclasses import dataclass
from enum import Enum

class TaskError(Exception):
    def __init__(self, ex: TaskException, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.type = ex.exception_type
        self.message = ex.exception_message
        self.stacktrace = ex.stacktrace

    def __str__(self):
        return f'type: {self.type} message: {self.message} stacktrace: {self.stacktrace}'


class TaskStatus(Enum):
    PENDING = TaskStatusProto.Status.PENDING
    RUNNING = TaskStatusProto.Status.RUNNING
    COMPLETED = TaskStatusProto.Status.COMPLETED
    FAILED = TaskStatusProto.Status.FAILED
