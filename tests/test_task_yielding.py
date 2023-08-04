import unittest
from unittest.mock import MagicMock
from statefun_tasks import FlinkTasks, YieldTaskInvocation
from tests.utils import TaskRunner


tasks = FlinkTasks()


@tasks.bind()
def _yield():
    raise YieldTaskInvocation()


class TaskYieldingTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.runner = TaskRunner(tasks)

    async def test_task_that_yields_does_not_emit_a_result(self):
        messages = []
        context = MagicMock()
        context.send_message = lambda *args: messages.append(args[0:3])

        await self.runner.run_task(_yield, context=context)
        self.assertFalse(any(messages))
