import unittest

from tests.utils import TestHarness, tasks


@tasks.bind()
def passthrough_workflow_specifying_kwarg():
    return _say_hello_and_return_last_name.send('Jane', 'Doe') \
        .continue_with(_truncate, max_length=10) \
        .continue_with(_combine_greeting_and_last_name)


@tasks.bind()
def _say_hello_and_return_last_name(first_name, last_name):
    return f'Hello {first_name} {last_name}', last_name


@tasks.bind()
def _truncate(greeting, max_length):
    return greeting if len(greeting) <= max_length else (greeting[:max_length] + '...')


@tasks.bind()
def _combine_greeting_and_last_name(truncated_greeting, original_length):
    return f'{truncated_greeting} (last name {original_length})'


class PassthroughArgsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.test_harness = TestHarness()

    def test_passing_kwarg_through_task(self):
        pipeline = tasks.send(passthrough_workflow_specifying_kwarg)
        result = self.test_harness.run_pipeline(pipeline)
        self.assertEqual(result, 'Hello Jane... (last name Doe)')
