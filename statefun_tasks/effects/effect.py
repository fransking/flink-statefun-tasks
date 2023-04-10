from abc import ABC, abstractmethod
from statefun_tasks.messages_pb2 import TaskResult


class Effect(ABC):
    def __init__(self, fn_result):
        self._fn_result = fn_result

    @property
    def fn_result(self):
        return self._fn_result

    @abstractmethod
    def apply(task_result: TaskResult):
        pass
