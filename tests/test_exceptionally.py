import unittest
from tests.utils import TestHarness, tasks, TaskErrorException


@tasks.bind()
def _say_hello(first_name, last_name):
    return f'Hello {first_name} {last_name}'


@tasks.bind()
def _throw_error(*args):
    raise Exception('I am supposed to fail')


@tasks.bind()
def _throw_another_error(*args):
    raise Exception('I am supposed to fail again')


@tasks.bind()
def _return_error(ex):
    return f'Exception: {ex.exception_message}'


@tasks.bind()
def _cleanup(*args):
    global finally_flag
    finally_flag = True


@tasks.bind()
def workflow_that_does_not_error(first_name, last_name):
    return tasks.send(_say_hello, first_name, last_name) \
        .exceptionally(_return_error) \
        .finally_do(_cleanup)


@tasks.bind()
def workflow_that_errors(first_name, last_name):
    return tasks.send(_say_hello, first_name, last_name) \
        .continue_with(_throw_error) \
        .exceptionally(_return_error) \
        .finally_do(_cleanup)

@tasks.bind()
def workflow_that_errors_in_the_exceptionally_task(first_name, last_name):
    return tasks.send(_say_hello, first_name, last_name) \
        .continue_with(_throw_error) \
        .exceptionally(_throw_another_error) \
        .finally_do(_cleanup)

@tasks.bind()
def workflow_with_exceptionally_in_middle(first_name, last_name):
    return tasks.send(_say_hello, first_name, last_name) \
        .exceptionally(_return_error) \
        .continue_with(_throw_error) \
        .finally_do(_cleanup)


@tasks.bind()
def workflow_with_exceptionally_in_middle_with_no_finally(first_name, last_name):
    return tasks.send(_say_hello, first_name, last_name) \
        .exceptionally(_return_error) \
        .continue_with(_throw_error)         


class ExceptionallyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.test_harness = TestHarness()


    def test_pipeline_that_does_not_error_but_has_an_exceptionally(self):
        pipeline = tasks.send(workflow_that_does_not_error, 'Jane', last_name='Doe')

        result = self.test_harness.run_pipeline(pipeline)
        self.assertEqual(result, 'Hello Jane Doe')

    def test_pipeline_that_errors_and_continues_into_an_exceptionally(self):
        pipeline = tasks.send(workflow_that_errors, 'Jane', last_name='Doe')

        result = self.test_harness.run_pipeline(pipeline)
        self.assertEqual(result, 'Exception: I am supposed to fail')

    def test_pipeline_that_errors_and_continues_into_an_exceptionally_that_also_errors(self):
        pipeline = tasks.send(workflow_that_errors_in_the_exceptionally_task, 'Jane', last_name='Doe')

        try:
            self.test_harness.run_pipeline(pipeline)
        except TaskErrorException as e:
            self.assertEqual(e.task_error.message, 'I am supposed to fail again')
        else:
            self.fail('Expected an exception')

    def test_pipeline_with_exceptionally_in_middle(self):
        pipeline = tasks.send(workflow_with_exceptionally_in_middle, 'Jane', last_name='Doe')

        try:
            self.test_harness.run_pipeline(pipeline)
        except TaskErrorException as e:
            self.assertEqual(e.task_error.message, 'Invalid pipeline: "exceptionally" must be called at the end of the pipeline before finally_do')
        else:
            self.fail('Expected an exception')

    def test_pipeline_with_exceptionally_after_finally(self):
        pipeline = tasks.send(workflow_with_exceptionally_in_middle_with_no_finally, 'Jane', last_name='Doe')

        try:
            self.test_harness.run_pipeline(pipeline)
        except TaskErrorException as e:
            self.assertEqual(e.task_error.message, 'Invalid pipeline: "exceptionally" must be called at the end of the pipeline')
        else:
            self.fail('Expected an exception')

if __name__ == '__main__':
    unittest.main()
