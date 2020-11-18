import unittest

from statefun_tasks import in_parallel
from tests.utils import TestHarness, tasks, TaskErrorException


join_results_called = False


@tasks.bind()
def _say_hello(first_name, last_name):
    return f'Hello {first_name} {last_name}'


@tasks.bind()
def _say_goodbye(greeting, goodbye_message):
    return f'{greeting}. So now I will say {goodbye_message}'


@tasks.bind()
def _fail():
    raise Exception('I am supposed to fail')


@tasks.bind()
def _join_results(results):
    global join_results_called
    join_results_called = True
    return '; '.join(results)


class ParallelWorkflowTests(unittest.TestCase):
    def setUp(self) -> None:
        self.test_harness = TestHarness()

    def test_parallel_workflow(self):
        pipeline = in_parallel([
            _say_hello.send("John", "Smith"),
            _say_hello.send("Jane", "Doe").continue_with(_say_goodbye, goodbye_message="see you later!"),
        ]).continue_with(_join_results)
        result = self.test_harness.run_pipeline(pipeline)

        self.assertEqual(result, 'Hello John Smith; Hello Jane Doe. So now I will say see you later!')

    def test_parallel_workflow_with_error(self):
        global join_results_called
        join_results_called = False
        
        pipeline = in_parallel([
            _say_hello.send("John", "Smith"),
            _fail.send(),
        ]).continue_with(_join_results)

        self.assertRaises(TaskErrorException, self.test_harness.run_pipeline, pipeline)

        self.assertEqual(join_results_called, False)

if __name__ == '__main__':
    unittest.main()
