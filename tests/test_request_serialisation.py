import unittest

from tests.utils import TestHarness, tasks, TaskErrorException


class MyClass:
    def __init__(self, val):
        self._my_field = val

    @property
    def my_field(self):
        return self._my_field


@tasks.bind()
def return_my_class_my_field(my_class):
    return my_class.my_field


class RequestSerialisationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.test_harness = TestHarness()

    def test_sending_request_data_as_application_json_when_not_json_serialisable(self):
        pipeline = tasks.send(return_my_class_my_field, MyClass('a')).set(content_type='application/json')
        try:
            self.test_harness.run_pipeline(pipeline)
        except Exception as e:
            self.assertIn('not JSON serializable', str(e))
        else:
            self.fail('Expected an exception')

    def test_sending_request_data_as_picke_when_not_json_serialisable(self):
        pipeline = tasks.send(return_my_class_my_field, MyClass('a')).set(content_type='application/python-pickle')
        result = self.test_harness.run_pipeline(pipeline)
        self.assertEqual(result, 'a')


if __name__ == '__main__':
    unittest.main()
