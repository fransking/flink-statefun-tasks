import unittest

from statefun_tasks.messages_pb2 import Address
from tests.utils import TestHarness, tasks, TaskErrorException


class MyClass:
    def __init__(self):
        pass

@tasks.bind()
def return_my_class_my_field(my_class):
    pass


@tasks.bind()
def receive_protobuf(address: Address):
    return str(type(address))


class RequestSerialisationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.test_harness = TestHarness()

    def test_sending_request_data_that_is_not_protobuf_serialisable(self):
        pipeline = tasks.send(return_my_class_my_field, MyClass())
        try:
            self.test_harness.run_pipeline(pipeline)
        except Exception as e:
            self.assertIn('Cannot wrap non-scalar', str(e))
        else:
            self.fail('Expected an exception')


    def test_sending_annotated_protobuf_type(self):
        pipeline = tasks.send(receive_protobuf, Address(namespace='namespace'))
        result = self.test_harness.run_pipeline(pipeline)
        self.assertEqual("<class 'messages_pb2.Address'>", result)

if __name__ == '__main__':
    unittest.main()
