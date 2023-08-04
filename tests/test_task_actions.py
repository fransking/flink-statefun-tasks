import unittest
from unittest.mock import MagicMock
from statefun_tasks import FlinkTasks, TaskAction, unpack_any, TaskStatus, TaskActionException, TaskResult, TaskException
from tests.utils import TaskRunner


tasks = FlinkTasks()


class TaskActionsTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.runner = TaskRunner(tasks)

    async def test_get_status_for_pending_task(self):
        context = MagicMock()
        context.storage.task_result = None
        context.storage.task_exception = None

        action_result = await self.runner.run_action('123', TaskAction.GET_STATUS, context=context)
        self.assertEqual(unpack_any(action_result.result, [TaskStatus]).value, TaskStatus.PENDING)

    async def test_get_status_for_failed_task(self):
        context = MagicMock()
        context.storage.task_result = None
        context.storage.task_exception = True

        action_result = await self.runner.run_action('123', TaskAction.GET_STATUS, context=context)
        self.assertEqual(unpack_any(action_result.result, [TaskStatus]).value, TaskStatus.FAILED)

    async def test_get_status_for_completed_task(self):
        context = MagicMock()
        context.storage.task_result = True
        context.storage.task_exception = None

        action_result = await self.runner.run_action('123', TaskAction.GET_STATUS, context=context)
        self.assertEqual(unpack_any(action_result.result, [TaskStatus]).value, TaskStatus.COMPLETED)

    async def test_get_task_request_for_pending_task(self):
        context = MagicMock()
        context.storage.task_request = None

        action_result = await self.runner.run_action('123', TaskAction.GET_REQUEST, context=context)
        self.assertIsInstance(action_result, TaskActionException)
        self.assertEqual(action_result.exception_message, 'Task request not found')

    async def test_get_task_request_for_task(self):
        context = MagicMock()
        context.storage.task_request = 'Mock task request'

        action_result = await self.runner.run_action('123', TaskAction.GET_REQUEST, context=context)
        self.assertTrue(action_result, 'Mock task request')

    async def test_get_task_result_for_pending_task(self):
        context = MagicMock()
        context.storage.task_result = None
        context.storage.task_exception = None

        action_result = await self.runner.run_action('123', TaskAction.GET_RESULT, context=context)
        self.assertIsInstance(action_result, TaskActionException)
        self.assertEqual(action_result.exception_message, 'Task result not found')

    async def test_get_task_result_for_completed_task(self):
        context = MagicMock()
        context.storage.task_result = TaskResult(id='123')
        context.storage.task_exception = None

        action_result = await self.runner.run_action('123', TaskAction.GET_RESULT, context=context)
        self.assertEqual(action_result.id, '123')

    async def test_get_task_result_for_failed_task(self):
        context = MagicMock()
        context.storage.task_result = None
        context.storage.task_exception = TaskException(id='123')

        action_result = await self.runner.run_action('123', TaskAction.GET_RESULT, context=context)
        self.assertEqual(action_result.id, '123')
