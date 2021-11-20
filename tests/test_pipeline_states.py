import asyncio
import unittest

from statefun_tasks import in_parallel
from statefun_tasks.messages_pb2 import TaskStatus, TaskAction, TaskException
from tests.utils import TestHarness, tasks
from google.protobuf.any_pb2 import Any

_started = []
_finished = []
_status = {}


@tasks.events.on_task_started
def on_task_started(context, task_request):
    _started.append(task_request.id)


@tasks.events.on_task_finished
def on_task_finished(context, task_result=None, task_exception=None, is_pipeline=False):
    if task_result is not None:
        _finished.append(task_result.id)
    else:
        _finished.append(task_result.id)


@tasks.events.on_pipeline_status_changed
def on_pipeline_status_changes(context, pipeline, status):
    _status[context.get_task_id()] = status


@tasks.bind()
def _say_hello(first_name, last_name):
    return f'Hello {first_name} {last_name}'


@tasks.bind()
async def _say_goodbye(greeting, goodbye_message):
    await asyncio.sleep(0)
    return f'{greeting}.  So now I will say {goodbye_message}'


class PipelineStateTests(unittest.TestCase):
    def setUp(self) -> None:
        self.test_harness = TestHarness()

    @staticmethod
    def _unpack(any_proto: Any, proto_type):
        proto = proto_type()
        any_proto.Unpack(proto)
        return proto

    def test_pipeline_with_task_that_waits(self):
        pipeline = tasks.send(_say_hello, 'Jane', 'Doe').wait().continue_with(_say_goodbye, goodbye_message="see you later!")
        self.test_harness.run_pipeline(pipeline)

        entries = pipeline.get_tasks()
        self.assertIn(entries[0][2], _started)  # first task
        self.assertNotIn(entries[1][2], _started)  # second task after wait

        self.assertIn(pipeline.id, _status)
        self.assertEqual(_status[pipeline.id], TaskStatus.PAUSED)

    def test_pipeline_with_group_that_waits(self):
        pipeline = in_parallel([
            tasks.send(_say_hello, 'Jane', 'Doe'),
            tasks.send(_say_hello, 'Joe', 'Blogs')
            ]).wait().continue_with(_say_goodbye, goodbye_message="see you later!")
        self.test_harness.run_pipeline(pipeline)

        entries = pipeline.get_tasks()
        self.assertIn(entries[0][2], _started)  # first task in group
        self.assertIn(entries[1][2], _started)  # second task in group
        self.assertNotIn(entries[2][2], _started)  # task after group
        
        self.assertIn(pipeline.id, _status)
        self.assertEqual(_status[pipeline.id], TaskStatus.PAUSED)

    def test_pipeline_with_group_that_waits_and_then_is_unpaused_and_completes(self):
        pipeline = in_parallel([
            tasks.send(_say_hello, 'Jane', 'Doe'),
            tasks.send(_say_hello, 'Joe', 'Blogs')
            ]).wait().continue_with(_say_goodbye, goodbye_message="see you later!")
        self.test_harness.run_pipeline(pipeline)

        entries = pipeline.get_tasks()
        self.assertIn(entries[0][2], _started)  # first task in group
        self.assertIn(entries[1][2], _started)  # second task in group
        self.assertNotIn(entries[2][2], _started)  # task after group
        
        self.assertIn(pipeline.id, _status)
        self.assertEqual(_status[pipeline.id], TaskStatus.PAUSED)

        self.test_harness.run_action(pipeline, TaskAction.UNPAUSE_PIPELINE)
        self.assertEqual(_status[pipeline.id], TaskStatus.COMPLETED)
        self.assertIn(entries[2][2], _started)  # task after group

    def test_pipeline_with_group_that_waits_and_then_is_cancelled(self):
        pipeline = in_parallel([
            tasks.send(_say_hello, 'Jane', 'Doe'),
            tasks.send(_say_hello, 'Joe', 'Blogs')
            ]).wait().continue_with(_say_goodbye, goodbye_message="see you later!")
        self.test_harness.run_pipeline(pipeline)

        entries = pipeline.get_tasks()
        self.assertIn(entries[0][2], _started)  # first task in group
        self.assertIn(entries[1][2], _started)  # second task in group
        self.assertNotIn(entries[2][2], _started)  # task after group
        
        self.assertIn(pipeline.id, _status)
        self.assertEqual(_status[pipeline.id], TaskStatus.PAUSED)
        self.test_harness.run_action(pipeline, TaskAction.CANCEL_PIPELINE)

        self.assertEqual(_status[pipeline.id], TaskStatus.CANCELLED)
        self.assertNotIn(entries[2][2], _started)  # task after group

        # pipeline result is an exception
        action_result = self.test_harness.run_action(pipeline, TaskAction.GET_RESULT)
        task_exception = self._unpack(action_result.result, TaskException)
        self.assertEqual(task_exception.id, pipeline.id)


if __name__ == '__main__':
    unittest.main()