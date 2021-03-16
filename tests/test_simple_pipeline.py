import asyncio
import unittest

from tests.utils import TestHarness, tasks, other_tasks_instance, TaskErrorException


@tasks.bind()
def hello_workflow(first_name, last_name):
    return tasks.send(_say_hello, first_name, last_name)


@tasks.bind()
def hello_and_goodbye_workflow(first_name, last_name):
    return tasks.send(_say_hello, first_name, last_name).continue_with(_say_goodbye, goodbye_message="see you later!")


@tasks.bind()
def _say_hello(first_name, last_name):
    return f'Hello {first_name} {last_name}'


@tasks.bind()
async def _say_goodbye(greeting, goodbye_message):
    await asyncio.sleep(0)
    return f'{greeting}.  So now I will say {goodbye_message}'


class SimplePipelineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.test_harness = TestHarness()

    def test_pipeline(self):
        pipeline = tasks.send(hello_workflow, 'Jane', 'Doe')
        result = self.test_harness.run_pipeline(pipeline)
        self.assertEqual(result, 'Hello Jane Doe')

    def test_pipeline_using_kwargs(self):
        pipeline = tasks.send(hello_workflow, first_name='Jane', last_name='Doe')
        result = self.test_harness.run_pipeline(pipeline)
        self.assertEqual(result, 'Hello Jane Doe')

    def test_pipeline_with_continuation(self):
        pipeline = tasks.send(hello_and_goodbye_workflow, 'Jane', last_name='Doe')
        result = self.test_harness.run_pipeline(pipeline)
        self.assertEqual(result, 'Hello Jane Doe.  So now I will say see you later!')

    def test_unregistered_function(self):
        @other_tasks_instance.bind()
        def new_function():
            return 'Hello'

        pipeline = tasks.send(new_function, 'Jane', 'Doe')
        try:
            self.test_harness.run_pipeline(pipeline)
        except TaskErrorException as e:
            self.assertEqual(str(e.task_error.message), f'{__name__}.new_function is not a registered FlinkTask')
        else:
            self.fail('Expected a TaskErrorException')


if __name__ == '__main__':
    unittest.main()
