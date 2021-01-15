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

if __name__ == '__main__':
    unittest.main()
