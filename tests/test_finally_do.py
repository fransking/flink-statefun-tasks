import asyncio
import unittest
from tests.utils import TestHarness, tasks, TaskErrorException


finally_flag = False
finally_args = []


@tasks.bind()
def workflow(first_name, last_name):
    return tasks.send(_say_hello, first_name, last_name) \
        .continue_with(_say_goodbye, goodbye_message="see you later!") \
        .finally_do(_cleanup)


@tasks.bind()
def workflow_with_cleanup_kwargs(first_name, last_name):
    return tasks.send(_say_hello, first_name, last_name) \
        .continue_with(_say_goodbye, goodbye_message="see you later!") \
        .finally_do(_cleanup_with_args, arg1='arg1', arg2='arg2')


@tasks.bind()
def workflow_with_error_and_cleanup(first_name, last_name):
    return tasks.send(_say_hello, first_name, last_name) \
        .continue_with(_throw_error) \
        .finally_do(_cleanup)


@tasks.bind()
def workflow_with_cleanup_in_middle(first_name, last_name):
    return tasks.send(_say_hello, first_name, last_name) \
        .finally_do(_cleanup) \
        .continue_with(_throw_error)


@tasks.bind()
def _say_hello(first_name, last_name):
    return f'Hello {first_name} {last_name}'


@tasks.bind()
async def _say_goodbye(greeting, goodbye_message):
    await asyncio.sleep(0)
    return f'{greeting}.  So now I will say {goodbye_message}'


@tasks.bind()
def _throw_error(*args):
    raise Exception('I am supposed to fail')


@tasks.bind()
def _cleanup(*args):
    global finally_flag
    finally_flag = True


@tasks.bind()
def _cleanup_with_args(arg1, arg2, *args):
    global finally_args
    finally_args = [arg1, arg2]


class FinallyDoTests(unittest.TestCase):
    def setUp(self) -> None:
        self.test_harness = TestHarness()

    def test_pipeline_with_finally(self):
        global finally_flag
        finally_flag = False
        pipeline = tasks.send(workflow, 'Jane', last_name='Doe')

        result = self.test_harness.run_pipeline(pipeline)

        self.assertEqual(result, 'Hello Jane Doe.  So now I will say see you later!')
        self.assertEqual(finally_flag, True)

    def test_pipeline_with_finally_kwargs(self):
        global finally_args
        finally_args = None
        pipeline = tasks.send(workflow_with_cleanup_kwargs, 'Jane', last_name='Doe')

        self.test_harness.run_pipeline(pipeline)

        self.assertEqual(finally_args, ['arg1', 'arg2'])

    def test_pipeline_with_finally_and_error(self):
        global finally_flag
        finally_flag = False
        pipeline = tasks.send(workflow_with_error_and_cleanup, 'Jane', last_name='Doe')

        try:
            self.test_harness.run_pipeline(pipeline)
        except TaskErrorException as e:
            self.assertIn('I am supposed to fail', e.task_error.message)
            self.assertEqual(finally_flag, True)
        else:
            self.fail('Expected an exception')

    def test_pipeline_with_cleanup_in_middle(self):
        pipeline = tasks.send(workflow_with_cleanup_in_middle, 'Jane', last_name='Doe')

        try:
            self.test_harness.run_pipeline(pipeline)
        except TaskErrorException as e:
            self.assertEqual(e.task_error.message, 'Invalid pipeline: "finally_do" must be called at the end of a pipeline')
        else:
            self.fail('Expected an exception')

if __name__ == '__main__':
    unittest.main()
