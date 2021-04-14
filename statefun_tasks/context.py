from statefun_tasks.serialisation import DefaultSerialiser
from statefun_tasks.type_helpers import flink_value_type_for
from statefun_tasks.messages_pb2 import TaskState
from statefun_tasks.protobuf import pack_any

from statefun import kafka_egress_message, message_builder, Context, SdkAddress
from google.protobuf.any_pb2 import Any

from typing import Callable


class _TaskContext(object):
    def __init__(self, context: Context, egress_type_name: str, serialiser=None):
        self._context = context
        self._egress_type_name = egress_type_name
        self._serialiser = serialiser if serialiser is not None else DefaultSerialiser()

        self.storage = context.storage

        task_state = self.storage.task_state or TaskState()
        self._state = self._serialiser.from_proto(task_state.data)

    def get_address(self):
        return f'{self._context.address.typename}'

    def get_task_id(self):
        return self._context.address.id

    def get_caller_address(self):
        return f'{self._context.caller.typename}'

    def get_caller_id(self):
        return None if self._context.caller.id == "" else self._context.caller.id

    @staticmethod
    def to_address_and_id(address: SdkAddress):
        return f'{address.namespace}/{address.type}', address.id

    def send_message_after(self, delay, destination, task_id, value):
        value_type = flink_value_type_for(value)
        message = message_builder(target_typename=destination, target_id=task_id, value=value, value_type=value_type)
        self._context.send_after(delay, message)

    def send_message(self, destination, task_id, value):
        value_type = flink_value_type_for(value)
        message = message_builder(target_typename=destination, target_id=task_id, value=value, value_type=value_type)
        self._context.send(message)

    def send_egress_message(self, topic, value):
        proto_bytes = pack_any(value).SerializeToString()
        message = kafka_egress_message(typename=self._egress_type_name, topic=topic, value=proto_bytes)
        self._context.send_egress(message)

    def delete(self, key):
        del self._context[key]

    def set_state(self, data:dict):
        self._state = data

    def get_state(self) -> dict:
        return self._state

    def update_state(self, updates:dict):
        self._state.update(updates)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        task_state = TaskState(data=self._serialiser.to_proto(self._state))
        self._context.storage.task_state = task_state

    def __str__(self):
        return f'task_id: {self.get_task_id()}, caller: {self.get_caller_id()}'
