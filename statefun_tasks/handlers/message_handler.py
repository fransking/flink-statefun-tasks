from abc import ABC, abstractmethod
from statefun_tasks.task_context import TaskContext
from statefun_tasks.default_serialiser import DefaultSerialiser
from statefun import Message


class MessageHandler(ABC):
    def __init__(self, serialiser: DefaultSerialiser):
        self._serialiser = serialiser

    @abstractmethod
    def unpack(self, context: TaskContext, message: Message):
        pass

    @abstractmethod
    async def handle_message(self, tasks: 'FlinkTasks', context: TaskContext, unpacked_message):
        pass
