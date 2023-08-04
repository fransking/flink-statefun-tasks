import unittest
from tests.utils import TaskRunner
from statefun_tasks import FlinkTasks


tasks = FlinkTasks()


@tasks.bind()
async def return_async_generator():
    yield 1
    yield 2
    yield 3


@tasks.bind()
def return_generator():
    yield 4
    yield 5
    yield 6


class GeneratorTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.runner = TaskRunner(tasks)

    async def test_async_generator(self):
        result, _ = await self.runner.run_task(return_async_generator)
        self.assertEqual(result, [1, 2, 3])  

    async def test_generator(self):
        result, _ = await self.runner.run_task(return_generator)
        self.assertEqual(result, [4, 5, 6])  
