import unittest
from tests.utils import FlinkTestHarness, tasks, TaskErrorException

from statefun_tasks import in_parallel


@tasks.bind()
def _cleanup(*args):
    global finally_flag
    finally_flag = True


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
def _return_value(val):
    return f'Value: {val}'


@tasks.bind()
def _return_error(ex):
    return f'Exception: {ex.exception_message}'


@tasks.bind()
def _mark_completed(*args):
    global _completed
    _completed = True
    return _completed


@tasks.bind()
def _return_completed():
    global _completed
    return _completed


@tasks.bind()
def workflow_that_does_not_error(first_name, last_name):
    return tasks.send(_say_hello, first_name, last_name) \
        .exceptionally(_return_error)


@tasks.bind()
def workflow_that_does_not_error_with_a_finally(first_name, last_name):
    return tasks.send(_say_hello, first_name, last_name) \
        .exceptionally(_return_error) \
        .finally_do(_cleanup)


@tasks.bind()
def workflow_that_errors(first_name, last_name):
    return tasks.send(_say_hello, first_name, last_name) \
        .continue_with(_throw_error) \
        .exceptionally(_return_error)


@tasks.bind()
def workflow_that_errors_with_a_finally(first_name, last_name):
    return tasks.send(_say_hello, first_name, last_name) \
        .continue_with(_throw_error) \
        .exceptionally(_return_error) \
        .finally_do(_cleanup)


@tasks.bind()
def workflow_that_errors_in_the_exceptionally_task(first_name, last_name):
    return tasks.send(_say_hello, first_name, last_name) \
        .continue_with(_throw_error) \
        .exceptionally(_throw_another_error)


@tasks.bind()
def workflow_that_errors_in_the_middle_with_multiple_exceptionally_tasks(first_name, last_name):
    return tasks.send(_say_hello, first_name, last_name) \
        .continue_with(_throw_error) \
        .exceptionally(_return_error) \
        .continue_with(_throw_another_error) \
        .exceptionally(_return_error)
        
        
@tasks.bind()
def workflow_with_exceptionally_in_middle(first_name, last_name):
    return tasks.send(_say_hello, first_name, last_name) \
        .exceptionally(_return_error) \
        .continue_with(_throw_error)


@tasks.bind()
def workflow_with_exceptionally_in_middle_with_no_finally(first_name, last_name):
    return tasks.send(_say_hello, first_name, last_name) \
        .exceptionally(_return_error) \
        .continue_with(_throw_error)         


@tasks.bind()
def parallel_workflow_that_errors(first_name, last_name):
    return in_parallel([_throw_error.send(), _say_hello.send(first_name, last_name), _throw_another_error.send()]) \
        .exceptionally(_return_error)


@tasks.bind()
def parallel_workflow_that_returns_errors(first_name, last_name):
    return in_parallel([_throw_error.send(), _say_hello.send(first_name, last_name), _throw_another_error.send()], return_exceptions=True) \
        .exceptionally(_return_error)


@tasks.bind()
def empty_parallel_workflow_with_exceptionally_that_throws_error(first_name, last_name):
    return _say_hello.send(first_name, last_name) \
        .continue_with(_throw_error) \
        .continue_with(in_parallel([])) \
        .exceptionally(_return_error)


@tasks.bind()
def task_that_fails_followed_by_in_a_parallel_followed_by_exceptionally(first_name, last_name):
    grouping = [
        _say_hello.send(first_name, last_name),
        _say_hello.send(first_name, last_name)
    ]

    grouping2 = [
        _say_hello.send(first_name, last_name),
        _say_hello.send(first_name, last_name)
    ]

    grouping3 = [
        _say_hello.send(first_name, last_name),
        _say_hello.send(first_name, last_name)
    ]

    grouping4 = in_parallel(grouping3)

    return _throw_error.send() \
        .continue_with(in_parallel(grouping)) \
        .continue_with(_say_hello, first_name, last_name) \
        .continue_with(in_parallel([grouping4])) \
        .continue_with(_say_hello, first_name, last_name) \
        .exceptionally(_return_error)


@tasks.bind()
def parallel_workflow_with_exceptionally_inside_that_does_not_error(first_name, last_name, max_parallelism=None):
    return in_parallel([
        _say_hello.send(first_name, last_name).continue_with(_return_value).exceptionally(_return_error),
        _say_hello.send(first_name, last_name).continue_with(_return_value)
        ], max_parallelism=max_parallelism) \
        .continue_with(_return_value)

@tasks.bind()
def parallel_workflow_with_exceptionally_inside_that_does_error(first_name, last_name, max_parallelism=None):
    return in_parallel([
        _say_hello.send(first_name, last_name).continue_with(_throw_error).exceptionally(_return_error).exceptionally(_throw_error).exceptionally(_return_error),
        _say_hello.send(first_name, last_name).continue_with(_return_value)
        ], max_parallelism=max_parallelism) \
        .continue_with(_return_value)

@tasks.bind()
def parallel_workflow_with_exceptionally_inside_that_does_error_and_then_errors_in_exceptionally_and_then_recovers(first_name, last_name, max_parallelism=None):
    return in_parallel([
        _say_hello.send(first_name, last_name).continue_with(_throw_error).exceptionally(_throw_another_error).exceptionally(_return_error),
        _say_hello.send(first_name, last_name).continue_with(_return_value)
        ], max_parallelism=max_parallelism) \
        .continue_with(_return_value)

@tasks.bind()
def parallel_workflow_with_max_parallelsim_that_throws_error(first_name, last_name):
    return in_parallel([
        _say_hello.send(first_name, last_name).continue_with(_throw_error).exceptionally(_throw_another_error).exceptionally(_mark_completed),
        _return_completed.send()
        ], max_parallelism=1) \
        .continue_with(_return_value)

class ExceptionallyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.test_harness = FlinkTestHarness()


    def test_pipeline_that_does_not_error_but_has_an_exceptionally(self):
        pipeline = tasks.send(workflow_that_does_not_error, 'Jane', last_name='Doe')

        result = self.test_harness.run_pipeline(pipeline)
        self.assertEqual(result, 'Hello Jane Doe')

    def test_pipeline_that_does_not_error_but_has_an_exceptionally_and_a_finally(self):
        pipeline = tasks.send(workflow_that_does_not_error_with_a_finally, 'Jane', last_name='Doe')

        result = self.test_harness.run_pipeline(pipeline)
        self.assertEqual(result, 'Hello Jane Doe')

    def test_pipeline_that_errors_and_continues_into_an_exceptionally(self):
        pipeline = tasks.send(workflow_that_errors, 'Jane', last_name='Doe')

        result = self.test_harness.run_pipeline(pipeline)
        self.assertEqual(result, 'Exception: I am supposed to fail')

    def test_pipeline_that_errors_and_continues_into_an_exceptionally_and_then_finally(self):
        pipeline = tasks.send(workflow_that_errors_with_a_finally, 'Jane', last_name='Doe')

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

    def test_pipeline_that_errors_in_the_middle_with_multiple_exceptionally_tasks(self):
        pipeline = tasks.send(workflow_that_errors_in_the_middle_with_multiple_exceptionally_tasks, 'Jane', last_name='Doe')

        result = self.test_harness.run_pipeline(pipeline)
        self.assertEqual(result, 'Exception: I am supposed to fail again')

    def test_a_parallel_pipeline_that_errors_and_continues_into_an_exceptionally(self):
        pipeline = tasks.send(parallel_workflow_that_errors, 'Jane', last_name='Doe')

        result = self.test_harness.run_pipeline(pipeline)
        self.assertTrue('I am supposed to fail' in result)
        self.assertTrue('I am supposed to fail again' in result)

    def test_a_parallel_pipeline_that_returns_errors_and_continues_into_an_exceptionally(self):
        pipeline = tasks.send(parallel_workflow_that_returns_errors, 'Jane', last_name='Doe')

        result = self.test_harness.run_pipeline(pipeline)
        self.assertTrue('I am supposed to fail' in result[0].exception_message)
        self.assertEqual(result[1], 'Hello Jane Doe')
        self.assertTrue('I am supposed to fail again' in result[2].exception_message)

    def test_an_empty_parallel_pipeline_with_exceptionally(self):
        pipeline = tasks.send(empty_parallel_workflow_with_exceptionally_that_throws_error, 'Jane', last_name='Doe')

        result = self.test_harness.run_pipeline(pipeline)
        self.assertEqual(result, 'Exception: I am supposed to fail')

    def test_a_task_that_fails_followed_by_in_a_parallel_followed_by_exceptionally(self):
        pipeline = tasks.send(task_that_fails_followed_by_in_a_parallel_followed_by_exceptionally, 'Jane', last_name='Doe')

        result = self.test_harness.run_pipeline(pipeline)
        self.assertEqual(result, 'Exception: I am supposed to fail')

    def test_a_parallel_workflow_with_exceptionally_inside_that_does_not_error(self):
        pipeline = tasks.send(parallel_workflow_with_exceptionally_inside_that_does_not_error, 'Jane', last_name='Doe')

        result = self.test_harness.run_pipeline(pipeline)
        self.assertEqual(result, "Value: ['Value: Hello Jane Doe', 'Value: Hello Jane Doe']")

    def test_a_parallel_workflow_with_exceptionally_inside_that_does_error(self):
        pipeline = tasks.send(parallel_workflow_with_exceptionally_inside_that_does_error, 'Jane', last_name='Doe')

        result = self.test_harness.run_pipeline(pipeline)
        self.assertEqual(result, "Value: ['Exception: I am supposed to fail', 'Value: Hello Jane Doe']")

    def test_a_parallel_workflow_with_exceptionally_inside_that_does_not_error_and_has_max_parallelism(self):
        pipeline = tasks.send(parallel_workflow_with_exceptionally_inside_that_does_not_error, 'Jane', last_name='Doe', max_parallelism=1)

        result = self.test_harness.run_pipeline(pipeline)
        self.assertEqual(result, "Value: ['Value: Hello Jane Doe', 'Value: Hello Jane Doe']")

    def test_a_parallel_workflow_with_exceptionally_inside_that_does_error_and_has_max_parallelism(self):
        pipeline = tasks.send(parallel_workflow_with_exceptionally_inside_that_does_error, 'Jane', last_name='Doe', max_parallelism=1)

        result = self.test_harness.run_pipeline(pipeline)
        self.assertEqual(result, "Value: ['Exception: I am supposed to fail', 'Value: Hello Jane Doe']")

    def test_a_parallel_workflow_with_exceptionally_inside_that_does_error_and_then_errors_in_exceptionally_and_then_recovers(self):
        pipeline = tasks.send(parallel_workflow_with_exceptionally_inside_that_does_error_and_then_errors_in_exceptionally_and_then_recovers, 'Jane', last_name='Doe')

        result = self.test_harness.run_pipeline(pipeline)
        self.assertEqual(result, "Value: ['Exception: I am supposed to fail again', 'Value: Hello Jane Doe']")

    def test_a_parallel_workflow_with_max_parallelsim_that_throws_error_does_not_trigger_deferred_tasks_until_it_is_complete(self):
        pipeline = tasks.send(parallel_workflow_with_max_parallelsim_that_throws_error, 'Jane', last_name='Doe')

        result = self.test_harness.run_pipeline(pipeline)
        self.assertEqual(result, "Value: [True, True]")



if __name__ == '__main__':
    unittest.main()
