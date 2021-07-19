from ._serialisation import DefaultSerialiser
from .messages_pb2 import TaskState, PipelineState, TaskRequest

from google.protobuf.any_pb2 import Any
from statefun import kafka_egress_record
from statefun.request_reply import BatchContext
from statefun.request_reply_pb2 import Address


class TaskContext(object):
    def __init__(self, context: BatchContext, task_name: str, egress_type_name: str, serialiser=None):
        self._context = context
        self._task_name = task_name
        self._egress_type_name = egress_type_name
        self._serialiser = serialiser if serialiser is not None else DefaultSerialiser()

        self.task_state = self.unpack('task_state', TaskState) or TaskState()
        self.pipeline_state = self.unpack('pipeline_state', PipelineState) or PipelineState()
        self._task_meta = {}

    @property
    def task_name(self):
        """
        The name of this task
        """
        return self._task_name

    @task_name.setter
    def task_name(self, value):
        self._task_name = value

    def apply_task_meta(self, task_request: TaskRequest):
        """
        Applies the task meta from the given TaskRequest to this context

        :param task_request: the task request
        """
        self._task_meta = task_request.meta or {}

    def get_root_pipeline_id(self):
        """
        ID of the top most pipeline if this task is called as part of a pipeline else None.  This will be different from 
        get_pipeline_id() if the pipeline is nested

        :return: root pipeline ID
        """
        return self._task_meta.get('root_pipeline_id', None)

    def get_root_pipeline_address(self):
        """
        Address of the top most pipeline if this task is called as part of a pipeline else None.

        :return: root pipeline address
        """
        return self._task_meta.get('root_pipeline_address', None)

    def get_pipeline_id(self):
        """
        ID of the pipeline if this task is called as part of a pipeline else None

        :return: pipeline ID
        """
        return self._task_meta.get('pipeline_id', None)

    def get_parent_task_id(self):
        """
        ID of the parent task if this task has one else None

        :return: parent task ID
        """
        return self._task_meta.get('parent_task_id', None)

    def get_parent_task_address(self):
        """
        ID of the parent task address if this task has one else None

        :return: parent task address
        """
        return self._task_meta.get('parent_task_address', None)

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

    def delete(self, key):
        del self._context[key]

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.pack_and_save('task_state', self.task_state)
        self.pack_and_save('pipeline_state', self.pipeline_state)

    def __str__(self):
        return f'task_name: {self.task_name}, task_id: {self.get_task_id()}, caller: {self.get_caller_id()}'
