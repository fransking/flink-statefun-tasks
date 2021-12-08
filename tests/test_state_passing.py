from statefun_tasks import in_parallel
import unittest

from tests.utils import TestHarness, tasks

_final_state = None

@tasks.bind()
def state_passing_workflow():
    return _create_workflow_state.send(initial_state=123) \
        .continue_with(_add_two_numbers, 3, 5) \
        .continue_with(_update_workflow_state) \
        .continue_with(_noop) \
        .finally_do(_destroy_workflow_state)


@tasks.bind()
def state_passing_workflow_with_exception():
    return _create_workflow_state.send(initial_state=456) \
        .continue_with(_add_two_numbers, 3, 5) \
        .continue_with(_update_workflow_state) \
        .continue_with(_raise_exception) \
        .finally_do(_destroy_workflow_state)


@tasks.bind()
def state_passing_workflow_with_exception_in_parallel():
    return _create_workflow_state.send(initial_state=456) \
        .continue_with(_add_two_numbers, 3, 5) \
        .continue_with(_update_workflow_state) \
        .continue_with(in_parallel([_raise_exception.send()])) \
        .finally_do(_destroy_workflow_state)


@tasks.bind(with_state=True)
def _create_workflow_state(state, initial_state):
    return initial_state


@tasks.bind()
def _add_two_numbers(num1, num2):
    return num1 + num2


@tasks.bind(with_state=True)
def _update_workflow_state(state, *args):
    state = state * 10
    return (state, *args)


@tasks.bind()
def _noop(*args):
    return args


@tasks.bind()
def _raise_exception(*args):
    raise ValueError()


@tasks.bind(with_state=True)
def _destroy_workflow_state(state):
    global _final_state
    _final_state = state


@tasks.bind(with_state=True)
def a_pipeline_calling_pipeline_stages(initial_state):
    initial_state= {'Some': 'State'}
    return initial_state, set_pipeline_state.send().continue_with(a_pipeline_stage).continue_with(return_state)

@tasks.bind(with_state=True)
def set_pipeline_state(initial_state):
    initial_state= {'Some': 'State'}
    return initial_state, True

@tasks.bind(with_state=True)
def a_pipeline_stage(state, *args):
    state['New'] = 'State'
    return state, a_task_that_sets_state.send()

@tasks.bind(with_state=True)
def return_state(state, *args):
    return state, state

@tasks.bind(with_state=True)
def a_parallel_pipeline(state):
    return state, in_parallel([a_task_that_sets_state.send() for _ in range(0, 2)]).continue_with(an_aggregation_task)

@tasks.bind(with_state=True)
def a_task_that_sets_state(_):
    return {'Some': 'State'}, True

@tasks.bind(with_state=True)
def an_aggregation_task(state, results):
    return state, state


class StatePassingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.test_harness = TestHarness()

    def test_passing_state_through_task(self):
        pipeline = tasks.send(state_passing_workflow)
        result = self.test_harness.run_pipeline(pipeline)
        self.assertEqual(result, 8)
        self.assertEqual(_final_state, 1230)

        
    def test_passing_state_through_exception_throwing_task(self):
        pipeline = tasks.send(state_passing_workflow_with_exception)

        with self.assertRaises(Exception):
            self.test_harness.run_pipeline(pipeline)

        self.assertEqual(_final_state, 4560)

    def test_passing_state_through_pipeline_stages(self):
        pipeline = tasks.send(a_pipeline_calling_pipeline_stages)
        result = self.test_harness.run_pipeline(pipeline)
        self.assertEqual({'New': 'State', 'Some': 'State'}, result)

    def test_passing_state_through_tasks_in_parallel(self):
        pipeline = tasks.send(a_parallel_pipeline)
        result = self.test_harness.run_pipeline(pipeline)
        self.assertEqual({'Some': 'State'}, result)

    def test_passing_state_through_tasks_in_parallel_that_throws_exception(self):
        pipeline = tasks.send(state_passing_workflow_with_exception_in_parallel)
        with self.assertRaises(Exception):
            self.test_harness.run_pipeline(pipeline)

        self.assertEqual(_final_state, 4560)


if __name__ == '__main__':
    unittest.main()
