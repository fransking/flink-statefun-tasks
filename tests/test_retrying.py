import unittest
from unittest.mock import MagicMock
from statefun_tasks import FlinkTasks, RetryPolicy
from statefun_tasks.utils import _task_type_for
from statefun_tasks.messages_pb2 import TaskSpecificState, TaskRetryPolicy
from tests.utils import TaskRunner


tasks = FlinkTasks()


@tasks.bind(retry_policy=RetryPolicy(retry_for=[ValueError], max_retries=1))
def _fail():
    raise ValueError('Fail')


class RetryTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.runner = TaskRunner(tasks)

    async def test_task_failing_retries_if_failure_count_is_less_than_max_retries(self):
        task_id = '123'
        task_uid = 'u123'
        caller_address = 'test/test_task_failing_retries_if_failure_count_is_less_than_max_retries'
        caller_id = '456'
        
        messages = []
        context = MagicMock()
        context.send_message = lambda *args: messages.append(args[0:3])
        
        await self.runner.run_task(_fail, context=context, task_id=task_id, task_uid=task_uid, caller_address=caller_address, caller_id=caller_id)
        _, _, retry_request = [m for m in messages if  m[2].uid == task_uid][0]
        
        self.assertEqual(context.task_state.by_uid[task_uid].original_caller_address, caller_address)
        self.assertEqual(context.task_state.by_uid[task_uid].original_caller_id, caller_id)
        self.assertEqual(context.task_state.by_uid[task_uid].retry_count, 1)

        self.assertEqual(retry_request.id, task_id)
        self.assertEqual(retry_request.uid, task_uid)
        self.assertEqual(retry_request.type, _task_type_for(_fail))

    async def test_task_fails_if_the_failure_count_exceeds_max_retries(self):
        with self.assertRaises(ValueError):
            await self.runner.run_task(_fail, task_state=TaskSpecificState(retry_count=1))

    async def test_task_retry_policy_can_be_overriden(self):
        task_uid = 'u123'
        context = MagicMock()
        retry_policy = TaskRetryPolicy(retry_for=["builtins.ValueError"], max_retries=2)
        task_state = TaskSpecificState(retry_count=1)

        await self.runner.run_task(_fail, context=context, task_uid=task_uid, retry_policy=retry_policy, task_state=task_state)
        self.assertEqual(context.task_state.by_uid[task_uid].retry_count, 2)
