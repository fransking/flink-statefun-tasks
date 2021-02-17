import asyncio
import unittest

from statefun_tasks import DefaultSerialiser, TaskAction, TaskStatus, TaskRequest, TaskResult

from google.protobuf.any_pb2 import Any
from tests.test_messages_pb2 import TestPerson, TestGreetingRequest, TestGreetingResponse
from tests.utils import TestHarness, tasks


@tasks.bind()
def hello_workflow(first_name, last_name):
    return tasks.send(_say_hello, first_name, last_name)

@tasks.bind()
def _say_hello(first_name, last_name):
    return f'Hello {first_name} {last_name}'


class TaskActionsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.test_harness = TestHarness()

    @staticmethod
    def _unpack(any_proto: Any, proto_type):
        proto = proto_type()
        any_proto.Unpack(proto)
        return proto

    def test_get_status_for_pending_workflow(self):
        pipeline = tasks.send(hello_workflow, 'Jane', 'Doe')
        action_result = self.test_harness.run_action(pipeline, TaskAction.GET_STATUS)
        self.assertEqual(self._unpack(action_result.result, TaskStatus).status, TaskStatus.PENDING)

    def test_get_status_for_pending_task(self):
        pipeline = tasks.send(_say_hello, 'Jane', 'Doe')
        action_result = self.test_harness.run_action(pipeline, TaskAction.GET_STATUS)
        self.assertEqual(self._unpack(action_result.result, TaskStatus).status, TaskStatus.PENDING)

    def test_get_status_for_completed_workflow(self):
        pipeline = tasks.send(hello_workflow, 'Jane', 'Doe')
        self.test_harness.run_pipeline(pipeline)
        action_result = self.test_harness.run_action(pipeline, TaskAction.GET_STATUS)
        self.assertEqual(self._unpack(action_result.result, TaskStatus).status, TaskStatus.COMPLETED)

    def test_get_status_for_completed_task(self):
        pipeline = tasks.send(_say_hello, 'Jane', 'Doe')
        self.test_harness.run_pipeline(pipeline)
        action_result = self.test_harness.run_action(pipeline, TaskAction.GET_STATUS)
        self.assertEqual(self._unpack(action_result.result, TaskStatus).status, TaskStatus.COMPLETED)

    def test_get_status_for_failed_workflow(self):
        pipeline = tasks.send(hello_workflow, 'Jane')
        try:
            self.test_harness.run_pipeline(pipeline)
        except:
            pass

        action_result = self.test_harness.run_action(pipeline, TaskAction.GET_STATUS)
        self.assertEqual(self._unpack(action_result.result, TaskStatus).status, TaskStatus.FAILED)

    def test_get_status_for_failed_task(self):
        pipeline = tasks.send(_say_hello, 'Jane')
        try:
            self.test_harness.run_pipeline(pipeline)
        except:
            pass

        action_result = self.test_harness.run_action(pipeline, TaskAction.GET_STATUS)
        self.assertEqual(self._unpack(action_result.result, TaskStatus).status, TaskStatus.FAILED)



if __name__ == '__main__':
    unittest.main()
