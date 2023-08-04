import unittest
from unittest.mock import MagicMock
from statefun_tasks import FlinkTasks
from tests.utils import TaskRunner


tasks = FlinkTasks()


@tasks.bind(with_context=True)
def _task_with_context(context):
    return id(context)


@tasks.bind(with_context=True)
def _task_with_context_and_args(context, a, b):
    return id(context), a, b


@tasks.bind(with_context=True, with_state=True)
def _task_with_context_and_state(context, state):
    return state, id(context), state


@tasks.bind(with_context=True, with_state=True)
def _task_with_context_args_and_state(context, state, a, b):
    return state, id(context), a, b, state


class WithContextTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.runner = TaskRunner(tasks)

    async def test_with_context(self):
        context = MagicMock()
        result, _ = await self.runner.run_task(_task_with_context, context=context)
        self.assertEqual(result, id(context))

    async def test_with_context_and_args(self):
        context = MagicMock()
        result, _ = await self.runner.run_task(_task_with_context_and_args, 'a', 'b', context=context)
        self.assertEqual(result, (id(context), 'a', 'b'))

    async def test_with_context_and_state(self):
        context = MagicMock()
        result, _ = await self.runner.run_task(_task_with_context_and_state, state='initial', context=context)
        self.assertEqual(result, (id(context), 'initial'))

    async def test_with_context_args_and_state(self):
        context = MagicMock()
        result, _ = await self.runner.run_task(_task_with_context_args_and_state, 'a', 'b', state='initial', context=context)
        self.assertEqual(result, (id(context), 'a', 'b', 'initial'))
