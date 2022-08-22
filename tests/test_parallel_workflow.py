from typing import OrderedDict
import unittest

from statefun_tasks import in_parallel
from tests.utils import TestHarness, tasks, TaskErrorException

join_results_called = False
join_results2_called = False
join_results3_called = False
say_goodbye_called = False


@tasks.bind()
def _say_hello(first_name, last_name):
    return f'Hello {first_name} {last_name}'

@tasks.bind()
def _say_hello_with_title(first_name, last_name, title="Mr"):
    return f'Hello {title} {first_name} {last_name}'


@tasks.bind()
def _say_goodbye(greeting, goodbye_message):
    global say_goodbye_called
    say_goodbye_called = True
    return f'{greeting}. So now I will say {goodbye_message}'


@tasks.bind()
def _fail(*args):
    raise Exception('I am supposed to fail')


@tasks.bind()
def _join_results(results):
    global join_results_called
    join_results_called = True
    return '; '.join(results)


@tasks.bind()
def _join_results2(results):
    global join_results2_called
    join_results2_called = True
    return '; '.join(results)


@tasks.bind()
def _join_results3(results):
    global join_results3_called
    join_results3_called = True
    return '; '.join(results)


@tasks.bind()
def _print_results(results):
    return str(results)


@tasks.bind()
def _print_results2(results, abc):
    return str(results) + str(abc)
    

@tasks.bind(with_state=True)
def _say_hello_with_state(initial_state, first_name, last_name):
    state = len(first_name) + len(last_name)
    return state, f'Hello {first_name} {last_name}'


@tasks.bind(with_state=True)
def _say_hello_with_initial_state(_, first_name, last_name, initial_state):
    return initial_state, f'Hello {first_name} {last_name}'


@tasks.bind(with_state=True)
def _say_hello_from_state(initial_state):
    return initial_state, f'Hello {initial_state}'


@tasks.bind()
def _say_goodbye_with_state(greeting, goodbye_message):
    return f'{greeting}. So now I will say {goodbye_message}'


@tasks.bind(with_state=True)
def _join_results_with_state(state, results):
    return state, '; '.join(results) + f' {state}'


@tasks.bind(with_state=True)
def _join_results_with_dictionary_state(state, results):
    keys = sorted(state.keys())

    ordered = OrderedDict()
    for key in keys:
        ordered[key] = state[key]

    return state, '; '.join(results) + f' {ordered}'



@tasks.bind()
def _generate_name():
    return 'John', 'Smith'


@tasks.bind()
def _add_greeting(first_name, last_name, greeting):
    return f'{greeting} {first_name} {last_name}'


@tasks.bind()
def _return_empty_parallel():
    return in_parallel([])



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

        self.assertEqual(result, 'Hello John Smith; Hello Jane Doe. So now I will say see you later! 9')

    def test_parallel_workflow_with_aggregated_dictionary_state(self):
        pipeline = in_parallel([
            _say_hello_with_initial_state.send("Jane", "Doe", {'A': 2 , 'B': 2}).continue_with(_say_goodbye, goodbye_message="see you later!"),
            _say_hello_with_initial_state.send("John", "Smith", {'A': 1}),
        ]).continue_with(_join_results_with_dictionary_state)
        result = self.test_harness.run_pipeline(pipeline)

        self.assertEqual(result, "Hello Jane Doe. So now I will say see you later!; Hello John Smith OrderedDict([('A', 2), ('B', 2)])")

    def test_nested_parallel_workflow(self):
        pipeline = in_parallel([
            in_parallel([
                in_parallel([
                _say_hello.send("John", "Smith"),
                _say_hello.send("Jane", "Doe").continue_with(_say_goodbye, goodbye_message="see you later!")
                ]).continue_with(_join_results)
            ])
        ])

        result = self.test_harness.run_pipeline(pipeline)

        self.assertEqual(result, [['Hello John Smith; Hello Jane Doe. So now I will say see you later!']])

    def test_nested_parallel_workflow_continuations(self):

        pipeline = in_parallel([
            in_parallel([
                in_parallel([
                _say_hello.send("John", "Smith"),
                _say_hello.send("Jane", "Doe").continue_with(_say_goodbye, goodbye_message="see you later!")
                ]).continue_with(_join_results)
            ]).continue_with(_join_results2)
        ]).continue_with(_join_results3)


        result = self.test_harness.run_pipeline(pipeline)

        self.assertEqual(result, 'Hello John Smith; Hello Jane Doe. So now I will say see you later!')
        self.assertEqual(join_results_called, True)
        self.assertEqual(join_results2_called, True)
        self.assertEqual(join_results3_called, True)

    def test_continuation_into_parallel_workflow(self):
        pipeline = _say_hello.send("John", "Smith").continue_with(in_parallel([
            _say_goodbye.send(goodbye_message="see you later!"),
            _say_goodbye.send(goodbye_message="see you later!")
        ]))

        result = self.test_harness.run_pipeline(pipeline)

        self.assertEqual(result, ['Hello John Smith. So now I will say see you later!', 'Hello John Smith. So now I will say see you later!'])

    def test_continuation_into_parallel_workflow_with_contination(self):
        pipeline = _say_hello.send("John", "Smith").continue_with(in_parallel([
            _say_goodbye.send(goodbye_message="see you later!"),
            _say_goodbye.send(goodbye_message="see you later!")
        ]).continue_with(_join_results))

        result = self.test_harness.run_pipeline(pipeline)

        self.assertEqual(result, 'Hello John Smith. So now I will say see you later!; Hello John Smith. So now I will say see you later!')

    def test_continuation_into_parallel_workflow_with_two_continations(self):
        pipeline = _say_hello.send("John", "Smith").continue_with(in_parallel([
            _say_goodbye.send(goodbye_message="see you later!"),
            _say_goodbye.send(goodbye_message="see you later!"),
        ])).continue_with(_join_results).continue_with(_print_results)

        result = self.test_harness.run_pipeline(pipeline)

        self.assertEqual(result, 'Hello John Smith. So now I will say see you later!; Hello John Smith. So now I will say see you later!')

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
            _say_goodbye.send("John", "Bye")
        ]).continue_with(_join_results)

        self.assertRaises(TaskErrorException, self.test_harness.run_pipeline, pipeline)

        self.assertEqual(join_results_called, False)
        self.assertEqual(say_goodbye_called, True)

    def test_parallel_workflow_with_error_and_continuations(self):
        global join_results_called
        join_results_called = False

        pipeline = in_parallel([
            _fail.send().continue_with(in_parallel([_say_hello.send("John", "Smith").continue_with(_join_results)])),  # this chain will fail at first step
            _say_goodbye.send("John", "Bye") # this chain will proceed
        ])

        # overall we will get an exception

        self.assertRaises(TaskErrorException, self.test_harness.run_pipeline, pipeline)

        self.assertEqual(join_results_called, False)
        self.assertEqual(say_goodbye_called, True)

    def test_empty_parallel_workflow(self):
        pipeline = in_parallel([])

        result = self.test_harness.run_pipeline(pipeline)
        self.assertEqual(result, [])


    def test_empty_parallel_child_pipeline(self):
        pipeline = _return_empty_parallel.send().continue_with(_print_results2, abc=12)

        result = self.test_harness.run_pipeline(pipeline)
        self.assertEqual(result, '[]12')

    def test_empty_parallel_workflow_with_a_continuation(self):
        pipeline = in_parallel([]).continue_with(_print_results2, abc=12)

        result = self.test_harness.run_pipeline(pipeline)
        self.assertEqual(result, '[]12')

    def test_parallel_workflow_with_last_task_in_group_being_a_group(self):
        pipeline = in_parallel([
            _say_hello.send("John", "Smith"),
            _say_hello.send("Jane", "Doe").continue_with(_say_goodbye, goodbye_message="see you later!"),
            in_parallel([
                _say_hello.send("Bob", "Smith"),
                _say_hello.send("Tom", "Smith"),
                ]),
        ]).continue_with(_print_results)

        result = self.test_harness.run_pipeline(pipeline)

        self.assertEqual(result, "['Hello John Smith', 'Hello Jane Doe. So now I will say see you later!', ['Hello Bob Smith', 'Hello Tom Smith']]")

    def test_parallel_workflow_with_max_parallelism(self):
        pipeline = in_parallel([
            _say_hello.send("Jane", "Doe"),
            in_parallel([
                _say_hello.send("Bob", "Smith").continue_with(_say_goodbye, goodbye_message="see you later!"),
                _say_hello.send("Tom", "Smith"),
                ], max_parallelism=1),
        ]).continue_with(_print_results)

        result = self.test_harness.run_pipeline(pipeline)
        self.assertEqual(result, "['Hello Jane Doe', ['Hello Bob Smith. So now I will say see you later!', 'Hello Tom Smith']]")

    def test_parallel_workflow_composition(self):
        p1 = in_parallel([_say_hello.send("A", "B"), _say_hello.send("C", "D")])
        p2 = in_parallel([_say_hello.send("E", "F"), _say_hello.send("G", "H")])
        p3 = in_parallel([_say_hello.send("I", "J")])

        pipeline = in_parallel([p1, p2, p3])

        result = self.test_harness.run_pipeline(pipeline)
        self.assertEqual(result, [['Hello A B', 'Hello C D'], ['Hello E F', 'Hello G H'], ['Hello I J']])

    def test_parallel_workflow_with_max_parallelism_as_continuation(self):
        pipeline = _say_hello.send("Jane", "Doe").continue_with(in_parallel([
            _print_results.send(),
            _print_results.send()
        ], max_parallelism=1))

        result = self.test_harness.run_pipeline(pipeline)
        self.assertEqual(result, ['Hello Jane Doe', 'Hello Jane Doe'])

    def test_empty_parallel_pipeline_continuation(self):
        global join_results_called
        join_results_called = False

        pipeline = _print_results.send("12").continue_with(in_parallel([])).continue_with(in_parallel([])).continue_with(_join_results)
        result = self.test_harness.run_pipeline(pipeline)
        self.assertTrue(join_results_called)
        self.assertEqual(result, None)

    def test_empty_parallel_pipeline(self):
        global join_results_called
        join_results_called = False

        pipeline = in_parallel([in_parallel([])]).continue_with(in_parallel([])).continue_with(_join_results)
        result = self.test_harness.run_pipeline(pipeline)
        self.assertTrue(join_results_called)
        self.assertEqual(result, None)

    def test_passing_arg_into_parallel_pipeline(self):
        pipeline = _print_results.send('John').continue_with(
            in_parallel([_say_hello.send(last_name='Smith'), _say_hello.send(last_name='Doe')]))
        result = self.test_harness.run_pipeline(pipeline)
        self.assertEqual(result, ['Hello John Smith', 'Hello John Doe'])

    def test_passing_multiple_args_into_parallel_pipeline(self):
        pipeline = _generate_name.send().continue_with(
            in_parallel([_add_greeting.send('Hello'), _add_greeting.send('Goodbye')]))
        result = self.test_harness.run_pipeline(pipeline)
        self.assertEqual(result, ['Hello John Smith', 'Goodbye John Smith'])

    def test_parallel_pipeline_wtih_stages(self):
        entries = [
            _add_greeting.send('John','Smith', 'Hello'), 
            _add_greeting.send('John','Smith', 'Goodbye'),
            _add_greeting.send('John','Smith', 'Hello to you'), 
            _add_greeting.send('John','Smith', 'Goodbye to you'),
             _add_greeting.send('Jane','Doe', 'Hello')
        ]

        # this will spint the 4 tasks into 3 stages (2 + 2 + 1 tasks in each) followed by a flatten
        pipeline = in_parallel(entries, num_stages=3, max_parallelism=2)
        result = self.test_harness.run_pipeline(pipeline)
        self.assertEqual(result, ['Hello John Smith', 'Goodbye John Smith', 'Hello to you John Smith', 'Goodbye to you John Smith', 'Hello Jane Doe'])

    def test_passing_arg_into_parallel_pipeline_in_stages(self):
        pipeline = _print_results.send('John').continue_with(
            in_parallel([_say_hello.send(last_name='Smith'), _say_hello.send(last_name='Doe'), _say_hello.send(last_name='Adams')], num_stages=2))

        result = self.test_harness.run_pipeline(pipeline)
        self.assertEqual(result, ['Hello John Smith', 'Hello John Doe', 'Hello John Adams'])

    def test_passing_initial_args_into_parallel_pipeline(self):
        pipeline = in_parallel([_say_hello.send(last_name='Smith'), _say_hello.send(last_name='Doe')]).with_initial(args='John')
        result = self.test_harness.run_pipeline(pipeline)
        self.assertEqual(result, ['Hello John Smith', 'Hello John Doe'])
        
    def test_passing_initial_args_into_parallel_pipeline_when_tasks_are_deferred_due_to_max_parallelism(self):
        pipeline = in_parallel([_say_hello.send(last_name='Smith'), _say_hello.send(last_name='Doe')], max_parallelism=1).with_initial(args='John')
        result = self.test_harness.run_pipeline(pipeline)
        self.assertEqual(result, ['Hello John Smith', 'Hello John Doe'])

    def test_passing_initial_args_into_parallel_pipeline_when_task_has_default_parameters(self):
        pipeline = in_parallel([_say_hello_with_title.send(last_name="Smith")]).with_initial(args='John')
        result = self.test_harness.run_pipeline(pipeline)
        self.assertEqual(result, ['Hello Mr John Smith'])

    def test_passing_initial_args_into_parallel_pipeline_when_task_overriding_default_parameters(self):
        pipeline = in_parallel([_say_hello_with_title.send(last_name="Smith", title="Sir")]).with_initial(args='John')
        result = self.test_harness.run_pipeline(pipeline)
        self.assertEqual(result, ['Hello Sir John Smith'])

    def test_passing_initial_args_into_parallel_pipeline_when_setting_initial_kwargs(self):
        pipeline = in_parallel([_say_hello_with_title.send(last_name="Smith")]).with_initial(args=('John',), kwargs={'title': 'Sir'})
        result = self.test_harness.run_pipeline(pipeline)
        self.assertEqual(result, ['Hello Sir John Smith'])

    def test_passing_initial_args_into_parallel_pipeline_when_setting_initial_kwargs_in_addition_to_task_kwargs(self):
        pipeline = in_parallel([_say_hello_with_title.send(last_name="Smith"), _say_hello_with_title.send(last_name="Adams", title='Mr')], max_parallelism=1, num_stages=2).with_initial(args=('John',), kwargs={'title': 'Sir'})
        result = self.test_harness.run_pipeline(pipeline)
        self.assertEqual(result, ['Hello Sir John Smith', 'Hello Mr John Adams'])

    def test_passing_initial_args_into_parallel_pipeline_when_setting_only_initial_kwargs_in_addition_to_task_kwargs(self):
        pipeline = in_parallel([_say_hello_with_title.send(last_name="Smith"), _say_hello_with_title.send(last_name="Adams", title='Mr')], max_parallelism=1, num_stages=2).with_initial(kwargs={'title': 'Sir', 'first_name': 'John'})
        result = self.test_harness.run_pipeline(pipeline)
        self.assertEqual(result, ['Hello Sir John Smith', 'Hello Mr John Adams'])

    def test_passing_initial_state_into_parallel_pipeline(self):
        pipeline = in_parallel([_say_hello_from_state.send(), _say_hello_from_state.send()], max_parallelism=1, num_stages=2).with_initial(state='John Smith')
        result = self.test_harness.run_pipeline(pipeline)
        self.assertEqual(result, ['Hello John Smith', 'Hello John Smith'])


if __name__ == '__main__':
    unittest.main()
