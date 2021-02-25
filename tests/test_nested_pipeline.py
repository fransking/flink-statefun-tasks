import asyncio
import unittest

from tests.utils import TestHarness, tasks


@tasks.bind()
def hello_workflow(first_name, last_name):
    return tasks.send(hello_and_goodbye_workflow, first_name, last_name)


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


class NestedPipelineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.test_harness = TestHarness()

    def test_nested_pipeline(self):
        pipeline = tasks.send(hello_workflow, 'Jane', 'Doe')
        result = self.test_harness.run_pipeline(pipeline)
        self.assertEqual(result, 'Hello Jane Doe.  So now I will say see you later!')

if __name__ == '__main__':
    unittest.main()
