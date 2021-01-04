import unittest

from google.protobuf.any_pb2 import Any
from tests.test_messages_pb2 import TestProto, TestResultProto, UnknownProto
from tests.utils import TestHarness, tasks, TaskErrorException


class MyClass:
    def __init__(self):
        pass

@tasks.bind()
def return_my_class_my_field(my_class):
    pass


@tasks.bind()
def receive_and_reply_protobuf_fully_annotated(test_proto: TestProto) -> TestResultProto:
    return TestResultProto(value_str=str(type(test_proto)))


@tasks.bind()
def receive_and_reply_protobuf_not_annotated(test_proto):
    return UnknownProto(value_str=str(type(test_proto)))


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


    def test_sending_protobuf_to_function_with_annotations_uses_exact_protos(self):
        pipeline = tasks.send(receive_and_reply_protobuf_fully_annotated, TestProto())
        result = self.test_harness.run_pipeline(pipeline)
        self.assertIsInstance(result, TestResultProto)
        self.assertEqual("<class 'test_messages_pb2.TestProto'>", result.value_str)

    def test_sending_protobuf_to_function_without_annotations_packs_as_any(self):
        pipeline = tasks.send(receive_and_reply_protobuf_not_annotated, UnknownProto())
        result = self.test_harness.run_pipeline(pipeline)
        self.assertIsInstance(result, Any)

        unknown = UnknownProto()
        result.Unpack(unknown)
        self.assertEqual("<class 'google.protobuf.any_pb2.Any'>", unknown.value_str)


if __name__ == '__main__':
    unittest.main()
