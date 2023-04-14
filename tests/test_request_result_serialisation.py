import unittest

from google.protobuf.any_pb2 import Any

from tests.test_messages_pb2 import Proto, ResultProto, UnknownProto
from tests.utils import FlinkTestHarness, tasks


class MyClass:
    def __init__(self):
        pass

@tasks.bind()
def return_my_class_my_field(my_class):
    pass


@tasks.bind()
def receive_and_reply_protobuf_fully_annotated(test_proto: Proto) -> ResultProto:
    return ResultProto(value_str=str(type(test_proto)))


@tasks.bind()
def receive_and_reply_protobuf_not_annotated(test_proto) -> Any:
    return UnknownProto(value_str=str(type(test_proto)))


@tasks.bind()
def protobuf_fully_annotated_continuation(test_proto: ResultProto) -> ResultProto:
    return ResultProto(value_str=str(type(test_proto)))


@tasks.bind()
def receive_and_reply_primitives(a, b, c, d):
    return [a, b, {'c': c}, (d,)]


class RequestResultSerialisationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.test_harness = FlinkTestHarness()

    def test_sending_request_data_that_is_not_protobuf_serialisable(self):
        pipeline = tasks.send(return_my_class_my_field, MyClass())
        try:
            self.test_harness.run_pipeline(pipeline)
        except Exception as e:
            self.assertIn('Cannot convert value', str(e))
        else:
            self.fail('Expected an exception')


    def test_sending_protobuf_to_function_with_annotations_uses_exact_protos(self):
        pipeline = tasks.send(receive_and_reply_protobuf_fully_annotated, Proto())
        result = self.test_harness.run_pipeline(pipeline)
        self.assertIsInstance(result, ResultProto)
        self.assertEqual("<class 'test_messages_pb2.Proto'>", result.value_str)

    def test_sending_protobuf_to_function_continations_with_annotations_uses_exact_protos(self):
        pipeline = tasks.send(receive_and_reply_protobuf_fully_annotated, Proto()).continue_with(protobuf_fully_annotated_continuation)
        result = self.test_harness.run_pipeline(pipeline)
        self.assertIsInstance(result, ResultProto)
        self.assertEqual("<class 'test_messages_pb2.ResultProto'>", result.value_str)

    def test_sending_protobuf_to_function_without_annotations_packs_as_any(self):
        pipeline = tasks.send(receive_and_reply_protobuf_not_annotated, UnknownProto())
        result = self.test_harness.run_pipeline(pipeline)
        self.assertIsInstance(result, Any)

        unknown = UnknownProto()
        result.Unpack(unknown)
        self.assertEqual("<class 'google.protobuf.any_pb2.Any'>", unknown.value_str)

    def test_sending_primitives_to_function(self):
        pipeline = tasks.send(receive_and_reply_primitives, 1, 2.0, [3], 'four')
        result = self.test_harness.run_pipeline(pipeline)
        self.assertIsInstance(result, list)

        a, b, c, d = result

        self.assertEqual(1, a)
        self.assertEqual(2.0, b)
        self.assertEqual({'c': [3]}, c)
        self.assertEqual(('four', ), d)

    def test_sending_embedded_protobuf_in_primitives_to_function(self):
        pipeline = tasks.send(receive_and_reply_primitives, 1, Proto(), [3], UnknownProto())
        result = self.test_harness.run_pipeline(pipeline)
        self.assertIsInstance(result, list)

        a, b, c, d = result

        self.assertEqual(1, a)
        self.assertIsInstance(b, Proto)
        self.assertEqual({'c': [3]}, c)

        p, = d
        self.assertIsInstance(p, Any)



if __name__ == '__main__':
    unittest.main()
