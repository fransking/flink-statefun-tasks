import unittest

from tests.utils import TestHarness, tasks



@tasks.bind(with_context=True)
def _with_context_and_arg_task(context, arg):
    return str(type(context)), arg, context.get_pipeline_id()


@tasks.bind(with_context=True, with_state=True)
def _with_context_and_arg_and_state_task(context, state, arg):
    return None, str(type(context)), arg, context.get_pipeline_id()


class WithContextTests(unittest.TestCase):
    def setUp(self) -> None:
        self.test_harness = TestHarness()

    def test_with_context_and_arg(self):
        pipeline = tasks.send(_with_context_and_arg_task, 8)
        result = self.test_harness.run_pipeline(pipeline)
        self.assertEqual(result, ("<class 'statefun_tasks.context.TaskContext'>", 8, pipeline.id))

    def test_with_context_and_arg_and_state(self):
        pipeline = tasks.send(_with_context_and_arg_and_state_task, 8)
        result = self.test_harness.run_pipeline(pipeline)
        self.assertEqual(result, ("<class 'statefun_tasks.context.TaskContext'>", 8, pipeline.id))

        
if __name__ == '__main__':
    unittest.main()
