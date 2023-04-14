from datetime import timedelta
import unittest

from statefun_tasks import RetryPolicy
from tests.utils import FlinkTestHarness, tasks, TaskErrorException



@tasks.bind()
def fail_workflow(fail_times):
    return tasks.send(_fail, fail_times)


@tasks.bind(with_context=True, retry_policy=RetryPolicy(retry_for=[Exception], max_retries=3))
def _fail(context, count):
    fail_count = context.get_state(0)
    
    if fail_count < count:
        fail_count += 1
        context.set_state(fail_count)
        raise ValueError(f'Failed after {fail_count} iterations')
        
    return f'Succeeded after {fail_count} failures'


class RetryTests(unittest.TestCase):
    def setUp(self) -> None:
        global _fail_run_count
        _fail_run_count = 0
        self.test_harness = FlinkTestHarness()

    def test_failing_task_1_times_with_3_retries(self):
        pipeline = tasks.send(_fail, 1)
        result = self.test_harness.run_pipeline(pipeline)
        self.assertEqual(result, 'Succeeded after 1 failures')

    def test_failing_task_3_times_with_3_retries(self):
        pipeline = tasks.send(_fail, 3)
        result = self.test_harness.run_pipeline(pipeline)
        self.assertEqual(result, 'Succeeded after 3 failures')

    def test_failing_task_4_times_with_3_retries(self):
        pipeline = tasks.send(_fail, 4)
        try:
            self.test_harness.run_pipeline(pipeline)
        except TaskErrorException as e:
            self.assertEqual(e.task_error.message, 'Failed after 4 iterations')
        else:
            self.fail('Expected an exception')

    def test_failing_pipeline_1_time_with_3_retries(self):
        pipeline = tasks.send(fail_workflow, 1)
        result = self.test_harness.run_pipeline(pipeline)
        self.assertEqual(result, 'Succeeded after 1 failures')

    def test_failing_pipeline_3_times_with_3_retries(self):
        pipeline = tasks.send(fail_workflow, 3)
        result = self.test_harness.run_pipeline(pipeline)
        self.assertEqual(result, 'Succeeded after 3 failures')

    def test_failing_pipeline_4_times_with_3_retries(self):
        pipeline = tasks.send(fail_workflow, 4)
        try:
            self.test_harness.run_pipeline(pipeline)
        except TaskErrorException as e:
            self.assertEqual(e.task_error.message, 'Failed after 4 iterations')
        else:
            self.fail('Expected an exception')

    def test_failing_pipeline_with_retry_policy_override(self):
        pipeline = tasks.send(_fail, 4).set(retry_policy=RetryPolicy(retry_for=[Exception], max_retries=4, delay=timedelta(seconds=1)))
        result = self.test_harness.run_pipeline(pipeline)
        self.assertEqual(result, 'Succeeded after 4 failures')


if __name__ == '__main__':
    unittest.main()
