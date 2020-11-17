import unittest

from tests.utils import TestHarness, tasks, TaskErrorException


class MyClass:
    def __init__(self, val):
        self._my_field = val

    @property
    def my_field(self):
        return self._my_field


@tasks.bind(content_type='application/python-pickle')
def _return_my_class_instance(inner_val):
    return MyClass(inner_val)


@tasks.bind(content_type='application/json')
def _return_my_class_instance_as_json(inner_val):
    return MyClass(inner_val)


class ResultSerialisationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.test_harness = TestHarness()

    def test_returning_non_json_serialisable_type(self):
        pipeline = tasks.send(_return_my_class_instance, 'my_val')
        result = self.test_harness.run_pipeline(pipeline)
        self.assertIsInstance(result, MyClass)
        self.assertEqual(result.my_field, 'my_val')

    def test_returning_non_json_serialisable_type_with_content_type_application_json(self):
        pipeline = tasks.send(_return_my_class_instance_as_json, 'my_val')
        try:
            self.test_harness.run_pipeline(pipeline)
        except TaskErrorException as e:
            self.assertIn('Object of type MyClass is not JSON serializable', str(e))
        else:
            self.fail('Expected an exception')
