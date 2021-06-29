import unittest

from tests.utils import TestHarness, tasks


_root_pipeline_id = None

@tasks.bind(with_context=True)
def hello_workflow(context, first_name, last_name):
    global _root_pipeline_id

    if not (context.get_root_pipeline_id() == context.get_pipeline_id()):
        raise ValueError('Root ID Mismatch')

    _root_pipeline_id = context.get_root_pipeline_id()

    return tasks.send(hello_and_goodbye_workflow, first_name, last_name)


@tasks.bind(with_context=True)
def hello_and_goodbye_workflow(context, first_name, last_name):

    if not (context.get_root_pipeline_id() == _root_pipeline_id):
        raise ValueError('Root ID Mismatch')

    return tasks.send(_say_hello, first_name, last_name).continue_with(_say_goodbye, goodbye_message="see you later!")


@tasks.bind(with_context=True)
def _say_hello(context, first_name, last_name):

    if not (context.get_root_pipeline_id() == _root_pipeline_id):
        raise ValueError('Root ID Mismatch')

    return f'Hello {first_name} {last_name}'


@tasks.bind(with_context=True)
def _say_goodbye(context, greeting, goodbye_message):

    if not (context.get_root_pipeline_id() == _root_pipeline_id):
        raise ValueError('Root ID Mismatch')

    return f'{greeting}.  So now I will say {goodbye_message}'


@tasks.bind(is_fruitful=False)
def _non_fruitful_workflow():
    return _task.send()

_return = None

@tasks.bind()
def _task():
    global _return
    _return = 5
    return 'ignore this'

@tasks.bind()
def _task2():
    return _return

class NestedPipelineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.test_harness = TestHarness()

    def test_nested_pipeline(self):
        pipeline = tasks.send(hello_workflow, 'Jane', 'Doe')
        result = self.test_harness.run_pipeline(pipeline)
        self.assertEqual(result, 'Hello Jane Doe.  So now I will say see you later!')

    def test_nested_non_fruitful_pipeline(self):
        pipeline = _non_fruitful_workflow.send().continue_with(_task2)
        result = self.test_harness.run_pipeline(pipeline)
        self.assertEqual(result, 5)

if __name__ == '__main__':
    unittest.main()
