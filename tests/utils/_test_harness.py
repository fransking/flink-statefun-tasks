import asyncio
from typing import Union, Optional, List, NamedTuple

from google.protobuf.any_pb2 import Any
from statefun import StatefulFunctions, AsyncRequestReplyHandler
from statefun.kafka_egress_pb2 import KafkaProducerRecord
from statefun.request_reply_pb2 import FromFunction, ToFunction, Address

from statefun_tasks import TaskResult, TaskException, PipelineBuilder, TaskRequest, FlinkTasks, deserialise_result
from statefun_tasks.client import TaskError
from ._test_utils import update_address, update_state, unpack_any

default_namespace = 'test'
default_worker_name = 'worker'
tasks = FlinkTasks(default_namespace=default_namespace, default_worker_name=default_worker_name,
                   egress_type_name=f'{default_namespace}/kafka-generic-egress')

functions = StatefulFunctions()


@functions.bind('test/worker')
async def worker(context, task_data: Union[TaskRequest, TaskResult, TaskException]):
    if tasks.is_async_required(task_data):
        await tasks.run_async(context, task_data)
    else:
        tasks.run(context, task_data)


async_handler = AsyncRequestReplyHandler(functions)


class _InvocationResult(NamedTuple):
    egress_message: Optional[Union[TaskResult, TaskException]]
    outgoing_messages: List[Any]


class TestHarness:
    """
    Provides a simplified implementation of Flink stateful functions, suitable for testing pipeline execution.

    Tasks are executed within the same process.
    """

    def __init__(self):
        self.__initial_state_keys = ['task_request',
                                     'task_state',
                                     'task_result',
                                     'task_exception']
        self.__states = {}
        self.__topic = 'statefun-test.requests'
        self.__initial_target_type = 'worker'
        self.__reply_topic = 'my_reply_topic'

    def run_pipeline(self, pipeline: PipelineBuilder, initial_target_type='worker'):
        task_request = pipeline.to_task_request()
        task_request.reply_topic = self.__reply_topic

        target = Address()
        target.namespace = default_namespace
        target.type = initial_target_type
        target.id = task_request.id

        return self._run_flink_loop(task_request, target)

    def _update_state(self, namespace, target_type, target_id, state_mutations):
        item_id = (namespace, target_type, target_id)
        state = self.__states[item_id]
        for state_mutation in state_mutations:
            if state_mutation.mutation_type == state_mutation.MODIFY:
                next(filter(lambda x: x.state_name == state_mutation.state_name,
                            state)).state_value = state_mutation.state_value
            else:
                raise ValueError('Only state modifications are currently supported')
        self.__states[item_id] = state

    def _copy_state_to_invocation(self, namespace, target_type, target_id, to_function):
        item_id = (namespace, target_type, target_id)
        if item_id not in self.__states:
            for state_key in self.__initial_state_keys:
                update_state(to_function.invocation.state, state_key, None)
            self.__states[item_id] = to_function.invocation.state
        state = self.__states[item_id]

        for state_item in list(state):
            update_state(to_function.invocation.state, state_item.state_name, state_item.state_value)

    def _try_extract_egress(self, invocation_result):
        if len(invocation_result.outgoing_egresses) == 0:
            return None
        elif len(invocation_result.outgoing_egresses) > 1:
            raise ValueError(f'Expected at most 1 egress message. Found {len(invocation_result.outgoing_egresses)}')
        kafka_producer_record = KafkaProducerRecord()
        invocation_result.outgoing_egresses[0].argument.Unpack(kafka_producer_record)
        if kafka_producer_record.topic != self.__reply_topic:
            raise ValueError(f'Unexpected topic for egress: {kafka_producer_record.topic}')
        result_any = Any.FromString(kafka_producer_record.value_bytes)
        task_result_or_exception = unpack_any(result_any, [TaskResult, TaskException])
        if isinstance(task_result_or_exception, TaskResult):
            return deserialise_result(task_result_or_exception)
        else:
            raise TaskErrorException(TaskError(task_result_or_exception))

    def _run_flink_loop(self, message_arg: Union[TaskRequest, TaskResult, TaskException], target: Address, caller=None):
        to_function = ToFunction()
        update_address(to_function.invocation.target, target.namespace, target.type, target.id)
        invocation = to_function.invocation.invocations.add()
        if caller:
            update_address(invocation.caller, caller.namespace, caller.type, caller.id)
        invocation.argument.Pack(message_arg)

        self._copy_state_to_invocation(target.namespace, target.type, target.id, to_function)
        result_bytes = asyncio.get_event_loop().run_until_complete(async_handler(to_function.SerializeToString()))

        result = self._process_result(to_function, result_bytes)
        if result.egress_message is not None:
            return result.egress_message
        else:
            outgoing_messages = result.outgoing_messages
            for outgoing_message in outgoing_messages:
                message_arg = unpack_any(outgoing_message.argument, [TaskRequest, TaskResult, TaskException])
                egress_value = self._run_flink_loop(message_arg=message_arg, target=outgoing_message.target, caller=target)
                if egress_value:
                    return egress_value

    def _process_result(self, to_function: ToFunction, result_bytes) -> _InvocationResult:
        result = self._parse_result_bytes(result_bytes)
        invocation_result = result.invocation_result
        egress_message = self._try_extract_egress(invocation_result)
        outgoing_messages = invocation_result.outgoing_messages
        target = to_function.invocation.target
        self._update_state(target.namespace, target.type, target.id, invocation_result.state_mutations)
        return _InvocationResult(egress_message, outgoing_messages)

    @staticmethod
    def _parse_result_bytes(result_bytes):
        f = FromFunction()
        f.ParseFromString(result_bytes)
        return f


class TaskErrorException(Exception):
    def __init__(self, task_error: TaskError):
        super().__init__(task_error)
        self.__task_error = task_error

    @property
    def task_error(self):
        return self.__task_error
