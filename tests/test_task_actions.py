import asyncio
import unittest

from statefun_tasks import DefaultSerialiser, TaskAction, TaskStatus, TaskRequest, TaskResult, TaskException

from google.protobuf.any_pb2 import Any
from tests.test_messages_pb2 import TestPerson, TestGreetingRequest, TestGreetingResponse
from tests.utils import TestHarness, tasks, TaskErrorException


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

    def test_get_status_for_pending_pipeline(self):
        pipeline = tasks.send(_say_hello, 'Jane', 'Doe')
        action_result = self.test_harness.run_action(pipeline, TaskAction.GET_STATUS)
        self.assertEqual(self._unpack(action_result.result, TaskStatus).status, TaskStatus.PENDING)

    def test_get_status_for_completed_pipeline(self):
        pipeline = tasks.send(_say_hello, 'Jane', 'Doe')
        self.test_harness.run_pipeline(pipeline)
        action_result = self.test_harness.run_action(pipeline, TaskAction.GET_STATUS)
        self.assertEqual(self._unpack(action_result.result, TaskStatus).status, TaskStatus.COMPLETED)

    def test_get_status_for_failed_pipeline(self):
        pipeline = tasks.send(_say_hello, 'Jane')
        try:
            self.test_harness.run_pipeline(pipeline)
        except:
            pass

        action_result = self.test_harness.run_action(pipeline, TaskAction.GET_STATUS)
        self.assertEqual(self._unpack(action_result.result, TaskStatus).status, TaskStatus.FAILED)

    def test_get_task_request_for_an_existing_pipeline(self):
        pipeline = tasks.send(_say_hello, 'Jane', 'Doe')
        self.test_harness.run_pipeline(pipeline)
        action_result = self.test_harness.run_action(pipeline, TaskAction.GET_REQUEST)
        task_request = self._unpack(action_result.result, TaskRequest)
        self.assertEqual(task_request.id, pipeline.id)

    def test_get_task_request_for_a_non_existing_pipeline(self):
        pipeline = tasks.send(_say_hello, 'Jane', 'Doe')
        try:
            self.test_harness.run_action(pipeline, TaskAction.GET_REQUEST)
        except TaskErrorException as ex:
            self.assertEqual(ex.task_error.message, 'Task request not found')
        else:
            self.fail('Expected an exception')

    def test_get_task_result_for_a_completed_pipeline(self):
        pipeline = tasks.send(_say_hello, 'Jane', 'Doe')
        self.test_harness.run_pipeline(pipeline)
        action_result = self.test_harness.run_action(pipeline, TaskAction.GET_RESULT)
        task_result = self._unpack(action_result.result, TaskResult)
        self.assertEqual(task_result.id, pipeline.id)

    def test_get_task_result_for_failed_pipeline(self):
        pipeline = tasks.send(_say_hello, 'Jane')
        try:
            self.test_harness.run_pipeline(pipeline)
        except:
            pass

        action_result = self.test_harness.run_action(pipeline, TaskAction.GET_RESULT)
        task_exception = self._unpack(action_result.result, TaskException)
        self.assertEqual(task_exception.id, pipeline.id)


if __name__ == '__main__':
    unittest.main()
