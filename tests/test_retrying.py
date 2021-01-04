import unittest

from statefun_tasks import RetryPolicy
from tests.utils import TestHarness, tasks, TaskErrorException


_fail_run_count = 0


@tasks.bind()
def fail_workflow(fail_times):
    return tasks.send(_fail, fail_times)


@tasks.bind(retry_policy=RetryPolicy(retry_for=[Exception], max_retries=3))
def _fail(count):
    global _fail_run_count
    if _fail_run_count < count:
        _fail_run_count += 1
        raise ValueError(f'Failed after {_fail_run_count} iterations')
    return f'Succeeded after {_fail_run_count} failures'


class RetryTests(unittest.TestCase):
    def setUp(self) -> None:
        global _fail_run_count
        _fail_run_count = 0
        self.test_harness = TestHarness()

    def test_failing_3_times_with_3_retries(self):
        pipeline = tasks.send(fail_workflow, 3)
        result = self.test_harness.run_pipeline(pipeline)
        self.assertEqual(result, 'Succeeded after 3 failures')

    def test_failing_4_times_with_3_retries(self):
        pipeline = tasks.send(fail_workflow, 4)
        try:
            self.test_harness.run_pipeline(pipeline)
        except TaskErrorException as e:
            self.assertEqual(e.task_error.message, 'Failed after 4 iterations')
        else:
            self.fail('Expected an exception')

if __name__ == '__main__':
    unittest.main()
