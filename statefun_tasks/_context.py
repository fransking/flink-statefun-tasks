from ._serialisation import _dumps, _loads
from .messages_pb2 import TaskState, TaskResult

from google.protobuf.any_pb2 import Any
from statefun import kafka_egress_record
from statefun.request_reply import BatchContext
from statefun.request_reply_pb2 import Address
from typing import Callable


class _TaskContext(object):
    def __init__(self, context: BatchContext, task_name: str, egress_type_name: str):
        self._context = context
        self._task_name = task_name
        self._egress_type_name = egress_type_name

        try:
            task_state = self.unpack('task_state', TaskState)
            self._state = _loads(task_state.data)
        except:
            self._state = {}

    def get_address(self):
        return f'{self._context.address.namespace}/{self._context.address.type}'

    def get_task_id(self):
        return self._context.address.identity

    def get_caller_address(self):
        return f'{self._context.caller.namespace}/{self._context.caller.type}'

    def get_caller_id(self):
        return None if self._context.caller.identity == "" else self._context.caller.identity

    @staticmethod
    def to_address_and_id(address: Address):
        return f'{address.namespace}/{address.type}', address.id

    def unpack(self, key:str, object_type):
        return self._context.state(key).unpack(object_type)

    def pack_and_reply(self, message):
        self._context.pack_and_reply(message)

    def pack_and_save(self, key:str, value):
        self._context.state(key).pack(value)

    def pack_and_send(self, destination, task_id, request):
        self._context.pack_and_send(destination, task_id, request)

    def pack_and_send_after(self, delay, destination, task_id, request):
        self._context.pack_and_send_after(delay, destination, task_id, request)

    def pack_and_send_egress(self, topic, value):
        any = Any()
        any.Pack(value)
        egress_message = kafka_egress_record(topic=topic, value=any)
        self._context.pack_and_send_egress(self._egress_type_name, egress_message)

    def set_state(self, data:dict):
        self._state = data

    def get_state(self) -> dict:
        return self._state

    def update_state(self, updates:dict):
        self._state.update(updates)

    def pack_and_update_state(self, key, value):
        any = Any()
        any.Pack(value)
        self._state.update({key: any})

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        task_state = TaskState(type='statefun_tasks.task_state', data=_dumps(self._state))
        self.pack_and_save('task_state', task_state)

    def __str__(self):
        return f'{self._task_name} [{self.get_task_id()}], caller: {self.get_caller_id()}]'
