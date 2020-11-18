import unittest

from tests.utils import TestHarness, tasks, TaskErrorException


class MyClass:
    def __init__(self, val):
        self._my_field = val

    @property
    def my_field(self):
        return self._my_field


@tasks.bind(content_type='application/python-pickle')
def return_my_class_instance(inner_val):
    return MyClass(inner_val)


@tasks.bind(content_type='application/json')
def return_my_class_instance_as_json(inner_val):
    return MyClass(inner_val)


@tasks.bind()
def return_my_class_instance_pipeline(inner_val, content_type):
    return tasks.send(return_my_class_instance, inner_val).set(content_type=content_type)


class ResultSerialisationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.test_harness = TestHarness()

    def test_returning_non_json_serialisable_type(self):
        pipeline = tasks.send(return_my_class_instance, 'my_val')
        result = self.test_harness.run_pipeline(pipeline)
        self.assertIsInstance(result, MyClass)
        self.assertEqual(result.my_field, 'my_val')

    def test_returning_non_json_serialisable_type_with_content_type_application_json(self):
        pipeline = tasks.send(return_my_class_instance_as_json, 'my_val')
        try:
            self.test_harness.run_pipeline(pipeline)
        except TaskErrorException as e:
            self.assertIn('not JSON serializable', str(e))
        else:
            self.fail('Expected an exception')

    def test_sending_data_as_application_json_when_json_serialisable(self):
        pipeline = tasks.send(return_my_class_instance_pipeline, {'a': 'b'}, 'application/json')
        result = self.test_harness.run_pipeline(pipeline)
        self.assertEqual(result.my_field, {'a': 'b'})

    def test_sending_data_as_application_json_when_not_json_serialisable(self):
        pipeline = tasks.send(return_my_class_instance_pipeline, MyClass('a'), 'application/json')
        try:
            self.test_harness.run_pipeline(pipeline)
        except Exception as e:
            self.assertIn('not JSON serializable', str(e))
        else:
            self.fail('Expected an exception')

    def test_sending_data_as_python_pickle_when_not_json_serialisable(self):
        pipeline = tasks.send(return_my_class_instance_pipeline, MyClass('inner_inner_val'), 'application/python-pickle').set(content_type='application/python-pickle')
        result = self.test_harness.run_pipeline(pipeline)
        self.assertEqual(result.my_field.my_field, 'inner_inner_val')

if __name__ == '__main__':
    unittest.main()
