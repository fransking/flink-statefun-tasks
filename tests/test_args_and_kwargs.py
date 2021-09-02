import unittest

from tests.utils import TestHarness, tasks


@tasks.bind()
def hello_workflow(first_name='Jane', last_name='Doe'):
    return f'Hello {first_name} {last_name}'


@tasks.bind()
def pass_into_multi_arg_workflow(initial_return, *args, **kwargs):
    return tasks.send(pass_through, *initial_return) \
        .continue_with(multi_arg_workflow, *args, **kwargs)


@tasks.bind()
def pass_through(*args):
    return args


@tasks.bind()
def multi_arg_workflow(a, b, c, d):
    return ','.join([a, b, c, d])


class ArgsAndKwargsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.test_harness = TestHarness()

    def test_passing_all_args(self):
        pipeline = tasks.send(multi_arg_workflow, 'a', 'b', 'c', 'd')
        result = self.test_harness.run_pipeline(pipeline)
        self.assertEqual(result, 'a,b,c,d')

    def test_passing_all_args_as_kwargs(self):
        pipeline = tasks.send(multi_arg_workflow, a='a', b='b', c='c', d='d')
        result = self.test_harness.run_pipeline(pipeline)
        self.assertEqual(result, 'a,b,c,d')

    def test_passing_mix_of_args_and_kwargs(self):
        pipeline = tasks.send(multi_arg_workflow, 'a', 'b', c='c', d='d')
        result = self.test_harness.run_pipeline(pipeline)
        self.assertEqual(result, 'a,b,c,d')

    def test_specifying_default_args_as_kwargs(self):
        pipeline = tasks.send(hello_workflow, first_name='John', last_name='Smith')
        result = self.test_harness.run_pipeline(pipeline)
        self.assertEqual(result, 'Hello John Smith')

    def test_specifying_default_args_as_args(self):
        pipeline = tasks.send(hello_workflow, 'John', 'Smith')
        result = self.test_harness.run_pipeline(pipeline)
        self.assertEqual(result, 'Hello John Smith')

    def test_specifying_single_default_arg(self):
        pipeline = tasks.send(hello_workflow, last_name='Smith')
        result = self.test_harness.run_pipeline(pipeline)
        self.assertEqual(result, 'Hello Jane Smith')

    def test_multi_part_workflow_with_all_args_from_first_task_return(self):
        pipeline = tasks.send(pass_into_multi_arg_workflow, ('a', 'b', 'c', 'd'))
        result = self.test_harness.run_pipeline(pipeline)
        self.assertEqual(result, 'a,b,c,d')

    def test_multi_part_workflow_with_middle_args_from_first_task_return(self):
        pipeline = tasks.send(pass_into_multi_arg_workflow, ('b', 'c'), a='a', d='d')
        result = self.test_harness.run_pipeline(pipeline)
        self.assertEqual(result, 'a,b,c,d')

    def test_multi_part_workflow_with_additional_args_for_second_task_passed_as_args(self):
        pipeline = tasks.send(pass_into_multi_arg_workflow, ('a', 'b'), 'c', 'd')
        result = self.test_harness.run_pipeline(pipeline)
        self.assertEqual(result, 'a,b,c,d')


if __name__ == '__main__':
    unittest.main()
