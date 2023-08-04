import unittest
from unittest.mock import MagicMock
from statefun_tasks import FlinkTasks, DefaultSerialiser, RetryPolicy
from tests.utils import TaskRunner


tasks = FlinkTasks()


@tasks.bind(with_state=True)
def _task_with_state(initial_state):
    new_state = 'new'
    return new_state, initial_state


@tasks.bind()
def _task_without_state():
    return True


@tasks.bind(with_state=True)
def _task_with_state_that_fails(initial_state):
    raise ValueError('fail')


@tasks.bind()
def _task_without_state_that_fails():
    raise ValueError('fail')


@tasks.bind(retry_policy=RetryPolicy(retry_for=[ValueError], max_retries=1))
def _task_that_retries():
    raise ValueError('fail')


class WithStateTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.runner = TaskRunner(tasks)

    async def test_task_with_state(self):
        result, state = await self.runner.run_task(_task_with_state, state='initial')
        self.assertEqual(result, 'initial')
        self.assertEqual(state, 'new')

    async def test_passing_state_through_task(self):
        result, state = await self.runner.run_task(_task_without_state, state='initial')
        self.assertEqual(result, True)
        self.assertEqual(state, 'initial')

    async def test_task_with_state_that_throws_an_exception_includes_state_in_exception(self):
        context = MagicMock()
        serialiser = DefaultSerialiser()

        with self.assertRaises(ValueError):
            await self.runner.run_task(_task_with_state_that_fails, context=context, state='initial')
        
        state = serialiser.from_proto(context.storage.task_exception.state)
        self.assertEqual(state, 'initial')


    async def test_passing_state_through_task_that_throws_an_exception_includes_state_in_exception(self):
        context = MagicMock()
        serialiser = DefaultSerialiser()

        with self.assertRaises(ValueError):
            await self.runner.run_task(_task_without_state_that_fails, context=context, state='initial')
        
        state = serialiser.from_proto(context.storage.task_exception.state)
        self.assertEqual(state, 'initial')


    async def test_task_that_retries_includes_state_in_retry(self):
        task_uid = 'u123'
        messages = []
        context = MagicMock()
        context.send_message = lambda *args: messages.append(args[0:3])
        serialiser = DefaultSerialiser()

        await self.runner.run_task(_task_that_retries, task_uid=task_uid, context=context, state='initial')

        _, _, retry_request = [m for m in messages if  m[2].uid == task_uid][0]
        
        state = serialiser.from_proto(retry_request.state)
        self.assertEqual(state, 'initial')
