from abc import ABC, abstractmethod
from statefun_tasks.context import TaskContext
from statefun_tasks.serialisation import DefaultSerialiser


class MessageHandler(ABC):
    def __init__(self, serialiser: DefaultSerialiser):
        self._serialiser = serialiser

    @abstractmethod
    def can_handle(self, context: TaskContext, message):
        pass

    @abstractmethod
    async def handle_message(self, tasks: 'FlinkTasks', context: TaskContext, message):
        pass
