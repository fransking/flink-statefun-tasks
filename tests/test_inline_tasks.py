import unittest
from datetime import timedelta

from statefun_tasks.extensions.inline_tasks import enable_inline_tasks, inline_task
from statefun_tasks import FlinkTasks, RetryPolicy, DefaultSerialiser, Pipeline
from tests.utils import TaskRunner


tasks = FlinkTasks()
enable_inline_tasks(tasks)


@inline_task(retry_policy=RetryPolicy(retry_for=[Exception], max_retries=4, delay=timedelta(seconds=1)))
def say_hello(name):
    return f'Hello {name}'


@tasks.bind(retry_policy=RetryPolicy(retry_for=[ValueError], max_retries=4, delay=timedelta(seconds=1)))
def say_goodbye(greeting):
    return f'{greeting} and goodbye'


@inline_task()
def wf(name):
    return say_hello.send(name).continue_with(say_goodbye)


class InlineTasksTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.runner = TaskRunner(tasks)

    async def test_inline_task_serialisation(self):
        pipeline = say_hello.send('Jane').continue_with(say_goodbye)
        proto = str(pipeline.to_proto())
        
        self.assertIn('retry_policy', proto)
        self.assertIn('ValueError', proto)


    async def test_inline_task_execution(self):
        request = say_hello.send("Jane").to_proto().entries[0].task_entry.request
        args, kwargs = DefaultSerialiser().deserialise_args_and_kwargs(request)
        result, _ = await self.runner.run_task("__builtins.run_code", *args, **kwargs)

        
        self.assertEqual(result, 'Hello Jane')

    async def test_inline_task_execution_creating_a_pipeline(self):
        request = wf.send("Jane").to_proto().entries[0].task_entry.request
        args, kwargs = DefaultSerialiser().deserialise_args_and_kwargs(request)
        result, _ = await self.runner.run_task("__builtins.run_code", *args, **kwargs)
        
        self.assertIsInstance(result, Pipeline)
        self.assertEqual(len(result.entries), 2)
        self.assertEqual(result.entries[0].task_entry.task_type, "__builtins.run_code")
        self.assertEqual(result.entries[1].task_entry.task_type, "tests.test_inline_tasks.say_goodbye")
