import unittest
from dataclasses import dataclass
from google.protobuf.message import Message
from google.protobuf.any_pb2 import Any
from google.protobuf.wrappers_pb2 import DoubleValue, StringValue
from statefun_tasks.core.statefun.request_reply_pb2 import Address
from statefun_tasks import TaskRequest
from statefun_tasks.messages_pb2 import MapOfStringToValue, ArrayOfValue, TupleOfValue, Value, NoneValue
from statefun_tasks.protobuf import convert_from_proto, convert_to_proto, pack_value, ScalarTypeProtobufConverter, \
    DEFAULT_CONVERTERS, ObjectProtobufConverter
from tests.test_messages_pb2 import MyType


class ProtobufTests(unittest.TestCase):

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

        proto = convert_to_proto(data, DEFAULT_CONVERTERS)
        reconsituted_data = convert_from_proto(proto, [Address], DEFAULT_CONVERTERS)

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

        proto = convert_to_proto(data, DEFAULT_CONVERTERS)
        reconsituted_data = convert_from_proto(proto, [Address], DEFAULT_CONVERTERS)

        self.assertTrue(isinstance(reconsituted_data, list))
        self.assertTrue(isinstance(reconsituted_data[0], Address))
        self.assertEqual(reconsituted_data[0].namespace, 'tests')
        self.assertEqual(reconsituted_data[0].type, 'test')
        self.assertEqual(reconsituted_data[0].id, 'id')
        self.assertEqual(reconsituted_data[1], 1)
        self.assertEqual(reconsituted_data[2], '123')

    def test_convert_tuple_to_protofbuf(self):
        data = (
            Address(namespace="tests", type="test", id="id"),
            1,
            '123'
        )

        proto = convert_to_proto(data, DEFAULT_CONVERTERS)
        reconsituted_data = convert_from_proto(proto, [Address], DEFAULT_CONVERTERS)

        self.assertTrue(isinstance(reconsituted_data, tuple))
        self.assertTrue(isinstance(reconsituted_data[0], Address))
        self.assertEqual(reconsituted_data[0].namespace, 'tests')
        self.assertEqual(reconsituted_data[0].type, 'test')
        self.assertEqual(reconsituted_data[0].id, 'id')
        self.assertEqual(reconsituted_data[1], 1)
        self.assertEqual(reconsituted_data[2], '123')

class LegacyProtobufTests(unittest.TestCase):

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

        proto = convert_to_proto(data, DEFAULT_CONVERTERS, use_legacy_types=True)
        reconsituted_data = convert_from_proto(proto, [Address], DEFAULT_CONVERTERS)

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

        proto = convert_to_proto(data, DEFAULT_CONVERTERS, use_legacy_types=True)
        reconsituted_data = convert_from_proto(proto, [Address], DEFAULT_CONVERTERS)

        self.assertTrue(isinstance(reconsituted_data, list))
        self.assertTrue(isinstance(reconsituted_data[0], Address))
        self.assertEqual(reconsituted_data[0].namespace, 'tests')
        self.assertEqual(reconsituted_data[0].type, 'test')
        self.assertEqual(reconsituted_data[0].id, 'id')
        self.assertEqual(reconsituted_data[1], 1)
        self.assertEqual(reconsituted_data[2], '123')

    def test_convert_tuple_to_protofbuf(self):
        data = (
            Address(namespace="tests", type="test", id="id"),
            1,
            '123'
        )

        proto = convert_to_proto(data, DEFAULT_CONVERTERS, use_legacy_types=True)
        reconsituted_data = convert_from_proto(proto, [Address], DEFAULT_CONVERTERS)

        self.assertTrue(isinstance(reconsituted_data, tuple))
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
            convert_to_proto(obj, [])
        except ValueError as e:
            self.assertIn('Cannot convert value of type', str(e))
        else:
            self.fail('Expected an exception')

    def test_to_and_from_protobuf(self):
        obj = self.MyType('my_val')
        converters = [self.MyConverter()]
        proto_message = convert_to_proto(obj, converters)
        
        python_val = convert_from_proto(proto_message, [], converters)
        self.assertIsInstance(proto_message, MyType)
        self.assertIsInstance(python_val, self.MyType)
        self.assertEqual(python_val.string_field, 'my_val')


class LegacyCustomProtobufConverterTests(CustomProtobufConverterTests):

    def test_to_and_from_protobuf(self):
        obj = self.MyType('my_val')
        converters = [self.MyConverter()]
        proto_message = convert_to_proto(obj, converters, use_legacy_types=True)
        
        python_val = convert_from_proto(proto_message, [], converters)
        self.assertIsInstance(proto_message, MyType)
        self.assertIsInstance(python_val, self.MyType)
        self.assertEqual(python_val.string_field, 'my_val')


class PackValueTests(unittest.TestCase):

    def test_pack_value_returns_same_value_instance(self):
        v = Value()
        v.string_value = 'already packed'
        result = pack_value(v, DEFAULT_CONVERTERS)
        self.assertIs(result, v)

    def test_pack_value_none(self):
        result = pack_value(None, DEFAULT_CONVERTERS)
        self.assertIsInstance(result, Value)
        self.assertIsInstance(result.none_value, NoneValue)

    def test_pack_value_bool_true(self):
        result = pack_value(True, DEFAULT_CONVERTERS)
        self.assertIsInstance(result, Value)
        self.assertTrue(result.bool_value)

    def test_pack_value_bool_false(self):
        result = pack_value(False, DEFAULT_CONVERTERS)
        self.assertIsInstance(result, Value)
        self.assertFalse(result.bool_value)

    def test_pack_value_int(self):
        result = pack_value(42, DEFAULT_CONVERTERS)
        self.assertIsInstance(result, Value)
        self.assertEqual(result.int_value, 42)

    def test_pack_value_float(self):
        result = pack_value(3.14, DEFAULT_CONVERTERS)
        self.assertIsInstance(result, Value)
        self.assertAlmostEqual(result.double_value, 3.14)

    def test_pack_value_str(self):
        result = pack_value('hello', DEFAULT_CONVERTERS)
        self.assertIsInstance(result, Value)
        self.assertEqual(result.string_value, 'hello')

    def test_pack_value_bytes(self):
        result = pack_value(b'data', DEFAULT_CONVERTERS)
        self.assertIsInstance(result, Value)
        self.assertEqual(result.bytes_value, b'data')

    def test_pack_value_map_of_string_to_value(self):
        m = MapOfStringToValue()
        inner = Value()
        inner.string_value = 'v'
        m.items['k'].CopyFrom(inner)
        result = pack_value(m, DEFAULT_CONVERTERS)
        self.assertIsInstance(result, Value)
        self.assertEqual(result.map_value.items['k'].string_value, 'v')

    def test_pack_value_array_of_value(self):
        a = ArrayOfValue()
        inner = Value()
        inner.int_value = 7
        a.items.append(inner)
        result = pack_value(a, DEFAULT_CONVERTERS)
        self.assertIsInstance(result, Value)
        self.assertEqual(result.array_value.items[0].int_value, 7)

    def test_pack_value_tuple_of_value(self):
        t = TupleOfValue()
        inner = Value()
        inner.double_value = 1.5
        t.items.append(inner)
        result = pack_value(t, DEFAULT_CONVERTERS)
        self.assertIsInstance(result, Value)
        self.assertEqual(result.tuple_value.items[0].double_value, 1.5)

    def test_pack_value_any(self):
        address = Address(namespace='ns', type='t', id='1')
        any_proto = Any()
        any_proto.Pack(address)
        result = pack_value(any_proto, DEFAULT_CONVERTERS)
        self.assertIsInstance(result, Value)
        self.assertTrue(result.HasField('any_value'))

    def test_pack_value_custom_type_via_converter(self):
        address = Address(namespace='ns', type='t', id='1')
        result = pack_value(address, DEFAULT_CONVERTERS)
        self.assertIsInstance(result, Value)
        self.assertTrue(result.HasField('any_value'))
        unpacked = Address()
        result.any_value.Unpack(unpacked)
        self.assertEqual(unpacked.namespace, 'ns')
        self.assertEqual(unpacked.type, 't')
        self.assertEqual(unpacked.id, '1')

    def test_pack_value_bool_takes_precedence_over_int(self):
        # bool is a subclass of int; ensure bool is handled before int
        result = pack_value(True, DEFAULT_CONVERTERS)
        self.assertTrue(result.HasField('bool_value'))
        self.assertFalse(result.HasField('int_value'))
