from statefun_tasks.serialisation import DefaultSerialiser
from statefun_tasks.type_helpers import flink_value_type_for
from statefun_tasks.messages_pb2 import TaskState, PipelineState
from statefun_tasks.protobuf import pack_any

from statefun import kafka_egress_message, message_builder, Context, SdkAddress
from datetime import timedelta


class TaskContext(object):
    """
    Task context wrapper around Flink context

    :param context: Flink context
    :param egress_type_name: egress type name to use when calling send_egress_message()
    :param optional serialiser: serialiser to use (will use DefaultSerialiser if not set)
    """
    def __init__(self, context: Context, egress_type_name: str, serialiser=None):
        self._context = context
        self._egress_type_name = egress_type_name
        self._serialiser = serialiser if serialiser is not None else DefaultSerialiser()

        self.storage = context.storage
        self.task_state = self.storage.task_state or TaskState()
        self.pipeline_state = self.storage.pipeline_state or PipelineState()

    def get_address(self):
        """
        Own address in the form of context.address.typename

        :return: address
        """
        return f'{self._context.address.typename}'

    def get_task_id(self):
        """
        Own task Id in the form of context.address.id

        :return: task Id
        """
        return self._context.address.id

    def get_caller_address(self):
        """
        Caller address in the form of context.caller.typename

        :return: address
        """
        return f'{self._context.caller.typename}'

    def get_caller_id(self):
        """
        Caller task Id in the form of context.caller.id

        :return: task Id
        """
        return None if self._context.caller.id == "" else self._context.caller.id

    @staticmethod
    def to_address_and_id(address: SdkAddress):
        """
        Converts SdkAddress into a string representation

        :param address: SDK address
        :return: address and id in the format namespace/type/id
        """
        return f'{address.namespace}/{address.type}', address.id

    def send_message_after(self, delay: timedelta, destination, target_id, value):
        """
        Sends a message to another Flink Task worker after some delay

        :param delay: the delay
        :param destination: the destination to send the message to (e.g. example/worker)
        :param target_id: the target Id
        :param value: the message to send
        """
        value_type = flink_value_type_for(value)
        message = message_builder(target_typename=destination, target_id=target_id, value=value, value_type=value_type)
        self._context.send_after(delay, message)

    def send_message(self, destination, target_id, value):
        """
        Sends a message to another Flink Task worker

        :param destination: the destination to send the message to (e.g. example/worker)
        :param target_id: the target Id
        :param value: the message to send
        """
        value_type = flink_value_type_for(value)
        message = message_builder(target_typename=destination, target_id=target_id, value=value, value_type=value_type)
        self._context.send(message)

    def send_egress_message(self, topic, value):
        """
        Sends a message to an egress topic

        :param topic: the topic name
        :param value: the message to send
        """
        proto_bytes = pack_any(value).SerializeToString()
        message = kafka_egress_message(typename=self._egress_type_name, topic=topic, value=proto_bytes)
        self._context.send_egress(message)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._context.storage.task_state = self.task_state
        self._context.storage.pipeline_state = self.pipeline_state

    def __str__(self):
        return f'task_id: {self.get_task_id()}, caller: {self.get_caller_id()}'
