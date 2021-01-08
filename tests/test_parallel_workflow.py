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


@tasks.bind(with_state=True)
def _say_hello_with_state(initial_state, first_name, last_name):
    state = len(first_name) + len(last_name)
    return state, f'Hello {first_name} {last_name}'


@tasks.bind()
def _say_goodbye_with_state(greeting, goodbye_message):
    return f'{greeting}. So now I will say {goodbye_message}'


@tasks.bind(with_state=True)
def _join_results_with_state(states, results):
    state = sum(states)/len(states)
    return state, '; '.join(results) + f' Average name length is {state}'


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

    def test_parallel_workflow_with_state(self):
        pipeline = in_parallel([
            _say_hello_with_state.send("John", "Smith"),
            _say_hello_with_state.send("Jane", "Doe").continue_with(_say_goodbye, goodbye_message="see you later!"),
        ]).continue_with(_join_results_with_state)
        result = self.test_harness.run_pipeline(pipeline)

        self.assertEqual(result, 'Hello John Smith; Hello Jane Doe. So now I will say see you later! Average name length is 8.0')


    def test_nested_parallel_workflow(self):
        pipeline = in_parallel([
            in_parallel([
            _say_hello.send("John", "Smith"),
            _say_hello.send("Jane", "Doe").continue_with(_say_goodbye, goodbye_message="see you later!")
            ])
        ])

        result = self.test_harness.run_pipeline(pipeline)

        self.assertEqual(result, [['Hello John Smith', 'Hello Jane Doe. So now I will say see you later!']])


    def test_continuation_into_nested_parallel_workflow(self):
        pipeline = _say_hello.send("John", "Smith").continue_with(in_parallel([in_parallel([
            _say_goodbye.send(goodbye_message="see you later!"),
            _say_goodbye.send(goodbye_message="see you later!")
        ])]))

        result = self.test_harness.run_pipeline(pipeline)

        self.assertEqual(result, [['Hello John Smith. So now I will say see you later!', 'Hello John Smith. So now I will say see you later!']])


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
