import unittest
from dataclasses import dataclass
from google.protobuf.message import Message
from google.protobuf.wrappers_pb2 import DoubleValue, StringValue
from statefun_tasks.core.statefun.request_reply_pb2 import Address
from statefun_tasks import TaskRequest
from statefun_tasks.protobuf import _convert_from_proto, _convert_to_proto, ScalarTypeProtobufConverter, \
    _generate_default_converters, ObjectProtobufConverter
from tests.test_messages_pb2 import MyType


class ProtobufTests(unittest.TestCase):
    def setUp(self) -> None:
        self.default_converters = _generate_default_converters()

    def test_copy_statefun_address_into_task_request(self):
        address = Address(namespace="tests", type="test", id="id")
        task_request = TaskRequest()
        task_request.reply_address.ParseFromString(address.SerializeToString())
        self.assertEqual(task_request.reply_address.namespace, address.namespace)
        self.assertEqual(task_request.reply_address.type, address.type)
        self.assertEqual(task_request.reply_address.id, address.id)

    def test_convert_dict_to_protofbuf(self):
        data = {
            'int': 123,
            'float': 1.23,
            'str': '123',
            'list': [1, 2, 3],
            'dict': {
                'a': 1,
                'b': 2
            },
            'dict_in_list': [1, {'a': 1}],
            'proto': Address(namespace="tests", type="test", id="id")
        }

        proto = _convert_to_proto(data, self.default_converters)
        reconsituted_data = _convert_from_proto(proto, [Address], self.default_converters)

        self.assertEqual(reconsituted_data['int'], 123)
        self.assertEqual(reconsituted_data['float'], 1.23)
        self.assertEqual(reconsituted_data['str'], '123')
        self.assertEqual(reconsituted_data['list'], [1, 2, 3])
        self.assertEqual(reconsituted_data['dict']['a'], 1)
        self.assertEqual(reconsituted_data['dict']['b'], 2)
        self.assertEqual(reconsituted_data['dict_in_list'][0], 1)
        self.assertEqual(reconsituted_data['dict_in_list'][1]['a'], 1)
        self.assertTrue(isinstance(reconsituted_data['proto'], Address))
        self.assertEqual(reconsituted_data['proto'].namespace, 'tests')
        self.assertEqual(reconsituted_data['proto'].type, 'test')
        self.assertEqual(reconsituted_data['proto'].id, 'id')

    def test_convert_list_to_protofbuf(self):
        data = [
            Address(namespace="tests", type="test", id="id"),
            1,
            '123'
        ]

        proto = _convert_to_proto(data, _generate_default_converters())
        reconsituted_data = _convert_from_proto(proto, [Address], self.default_converters)

        self.assertTrue(isinstance(reconsituted_data[0], Address))
        self.assertEqual(reconsituted_data[0].namespace, 'tests')
        self.assertEqual(reconsituted_data[0].type, 'test')
        self.assertEqual(reconsituted_data[0].id, 'id')
        self.assertEqual(reconsituted_data[1], 1)
        self.assertEqual(reconsituted_data[2], '123')


class ScalarTypeConverterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.float_converter = ScalarTypeProtobufConverter(float, DoubleValue)

    def test_can_convert_float_type_with_float_converter(self):
        can_convert = self.float_converter.can_convert_to_proto(1.0)
        self.assertTrue(can_convert)

    def test_can_convert_str_type_with_float_converter(self):
        can_convert = self.float_converter.can_convert_to_proto('string')
        self.assertFalse(can_convert)

    def test_convert_with_float_converter(self):
        proto_value = self.float_converter.convert_to_proto(1.23)
        self.assertIsInstance(proto_value, DoubleValue)
        self.assertEqual(proto_value.value, 1.23)

    def test_convert_back_with_float_converter(self):
        proto_value = DoubleValue()
        proto_value.value = 1.23
        python_value = self.float_converter.convert_from_proto(proto_value)
        self.assertEqual(python_value, 1.23)

    def test_float_converter_can_convert_proto_double_value(self):
        can_convert = self.float_converter.can_convert_from_proto(DoubleValue())
        self.assertTrue(can_convert)

    def test_float_converter_can_convert_proto_string_value(self):
        can_convert = self.float_converter.can_convert_from_proto(StringValue())
        self.assertFalse(can_convert)


class CustomProtobufConverterTests(unittest.TestCase):
    @dataclass
    class MyType:
        string_field: str

    class MyConverter(ObjectProtobufConverter[MyType]):
        def can_convert_to_proto(self, obj: object) -> bool:
            return type(obj) == CustomProtobufConverterTests.MyType

        def can_convert_from_proto(self, message: Message) -> bool:
            return type(message) == MyType

        def convert_to_proto(self, obj: 'CustomProtobufConverterTests.MyType') -> MyType:
            message = MyType()
            message.string_field = obj.string_field
            return message

        def convert_from_proto(self, message: MyType) -> object:
            val = CustomProtobufConverterTests.MyType(message.string_field)
            return val

    def test_converting_without_suitable_converter(self):
        obj = self.MyType('my_val')
        try:
            _convert_to_proto(obj, [])
        except ValueError as e:
            self.assertIn('Cannot convert value of type', str(e))
        else:
            self.fail('Expected an exception')

    def test_to_and_from_protobuf(self):
        obj = self.MyType('my_val')
        converters = [self.MyConverter()]
        proto_message = _convert_to_proto(obj, converters)
        python_val = _convert_from_proto(proto_message, [], converters)
        self.assertIsInstance(proto_message, MyType)
        self.assertIsInstance(python_val, self.MyType)
        self.assertEqual(python_val.string_field, 'my_val')
