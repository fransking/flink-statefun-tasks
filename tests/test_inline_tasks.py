import unittest

from statefun_tasks.extensions.inline_tasks import enable_inline_tasks, inline_task
from tests.utils import TestHarness, tasks


class InlineTasksTests(unittest.TestCase):
    def setUp(self) -> None:
        self.test_harness = TestHarness()
        enable_inline_tasks(tasks)

    def test_inline_task(self):
        
        @inline_task()
        def say_hello(name):
            return f'Hello {name}'

        @inline_task()
        def say_goodbye(greeting):
            return f'{greeting} and goodbye'

        pipeline = say_hello.send('Jane').continue_with(say_goodbye)
        result = self.test_harness.run_pipeline(pipeline)
        self.assertEqual(result, 'Hello Jane and goodbye')

if __name__ == '__main__':
    unittest.main()
