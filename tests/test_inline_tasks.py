import unittest
from datetime import timedelta

from statefun_tasks.extensions.inline_tasks import enable_inline_tasks, inline_task
from statefun_tasks import RetryPolicy
from tests.utils import TestHarness, tasks


class InlineTasksTests(unittest.TestCase):
    def setUp(self) -> None:
        self.test_harness = TestHarness()
        enable_inline_tasks(tasks)

    def test_inline_task(self):
        
        @inline_task(retry_policy=RetryPolicy(retry_for=[Exception], max_retries=4, delay=timedelta(seconds=1)))
        def say_hello(name):
            return f'Hello {name}'

        @inline_task(retry_policy=RetryPolicy(retry_for=[ValueError], max_retries=4, delay=timedelta(seconds=1)))
        def say_goodbye(greeting):
            return f'{greeting} and goodbye'

        @inline_task()
        def wf(name):
            return say_hello.send(name).continue_with(say_goodbye)

        pipeline = say_hello.send('Jane').continue_with(say_goodbye)
        proto = str(pipeline.to_proto())
        
        self.assertIn('retry_policy', proto)
        self.assertIn('ValueError', proto)
        
        result = self.test_harness.run_pipeline(pipeline)
        self.assertEqual(result, 'Hello Jane and goodbye')

        pipeline = wf.send('Joe')
        result = self.test_harness.run_pipeline(pipeline)
        self.assertEqual(result, 'Hello Joe and goodbye')

if __name__ == '__main__':
    unittest.main()
