import itertools
from abc import ABC, abstractmethod
from statefun_tasks.utils import is_tuple
from statefun_tasks.messages_pb2 import (MapOfStringToAny, ArrayOfAny, TupleOfAny, TaskEntry, GroupEntry, NoneValue,
                                         TaskRetryPolicy, TaskRequest, TaskResult, TaskException, TaskState, 
                                         Pipeline, PipelineEntry, Address,ArgsAndKwargs, TaskResultOrException,
                                         MapOfStringToValue, ArrayOfValue, TupleOfValue, Value, ValueArgsAndKwargs)
from google.protobuf.wrappers_pb2 import DoubleValue, Int64Value, BoolValue, StringValue, BytesValue
from google.protobuf.any_pb2 import Any
from google.protobuf.message import Message

from typing import Type, Union, TypeVar, Generic, Iterable


_FRAMEWORK_KNOWN_PROTO_TYPES = [
    # legacy wrappers
    DoubleValue,
    Int64Value,
    BoolValue,
    StringValue,
    BytesValue,

    # used to represent python None
    NoneValue,

    # flink task types
    MapOfStringToAny,
    TupleOfAny,
    ArrayOfAny,
    MapOfStringToValue,
    ArrayOfValue,
    TupleOfValue,
    Value,
    TaskEntry,
    GroupEntry,
    TaskRetryPolicy,
    TaskRequest,
    TaskResult,
    TaskException,
    TaskState,
    Pipeline,
    PipelineEntry,
    Address,
    ArgsAndKwargs,
    ValueArgsAndKwargs,
    TaskResultOrException
]

TProtoType = TypeVar('TProtoType', bound=Message)


class ObjectProtobufConverter(ABC, Generic[TProtoType]):
    @abstractmethod
    def can_convert_to_proto(self, obj: object) -> bool:
        pass

    @abstractmethod
    def can_convert_from_proto(self, message: Message) -> bool:
        pass

    @abstractmethod
    def convert_to_proto(self, obj: object) -> TProtoType:
        pass

    @abstractmethod
    def convert_from_proto(self, message: TProtoType) -> object:
        pass


TScalarProtoType = TypeVar('TScalarProtoType', bound=Message)


class ScalarTypeProtobufConverter(Generic[TScalarProtoType], ObjectProtobufConverter[TScalarProtoType]):
    def __init__(self, python_type, proto_type: TScalarProtoType):
        self._python_type = python_type
        self._proto_type = proto_type

    def can_convert_to_proto(self, obj: object) -> bool:
        return type(obj) == self._python_type

    def can_convert_from_proto(self, message: Message) -> bool:
        return type(message) == self._proto_type

    def convert_to_proto(self, obj: object) -> TScalarProtoType:
        wrapped_value = self._proto_type()
        wrapped_value.value = obj
        return wrapped_value

    def convert_from_proto(self, message: TScalarProtoType) -> object:
        return message.value


class NoneTypeProtobufConverter(ObjectProtobufConverter[NoneValue]):
    def can_convert_to_proto(self, obj: object) -> bool:
        return obj is None

    def can_convert_from_proto(self, message: Message) -> bool:
        return type(message) == NoneValue

    def convert_to_proto(self, obj: object) -> NoneValue:
        return NoneValue()

    def convert_from_proto(self, message: NoneValue) -> object:
        return None


DEFAULT_CONVERTERS = [
    ScalarTypeProtobufConverter(float, DoubleValue),
    ScalarTypeProtobufConverter(int, Int64Value),
    ScalarTypeProtobufConverter(bool, BoolValue),
    ScalarTypeProtobufConverter(str, StringValue),
    ScalarTypeProtobufConverter(bytes, BytesValue),
    NoneTypeProtobufConverter(),
]


def pack_any(value) -> Any:
    if isinstance(value, Any):
        return value

    proto = Any()
    proto.Pack(value)
    return proto


def unpack_any(value, known_proto_types):
    if isinstance(value, Any):
        for proto_type in itertools.chain(_FRAMEWORK_KNOWN_PROTO_TYPES, known_proto_types):
            if value.Is(proto_type.DESCRIPTOR):
                unwrapped = proto_type()
                value.Unpack(unwrapped)
                return unwrapped
        return value

    return value


def wrap_value(v: object, converters: Iterable[ObjectProtobufConverter], use_legacy_types: bool = False) -> Message:
    if isinstance(v, Message):
        # already protobuf so no need to attempt conversion
        return v

    compatible_converter = next((c for c in converters if c.can_convert_to_proto(v)), None)
    if compatible_converter is None:
        raise ValueError(
            f'Cannot convert value of type {type(v)} to protobuf. '
            'Try converting to protobuf first, or provide a compatible converter.')
    
    should_wrap = use_legacy_types or not isinstance(compatible_converter, (ScalarTypeProtobufConverter, NoneTypeProtobufConverter))

    return compatible_converter.convert_to_proto(v) if should_wrap else pack_value(v, converters)


def unwrap_value(v: Message, converters: Iterable[ObjectProtobufConverter]):
    compatible_converter = next((c for c in converters if c.can_convert_from_proto(v)), None)
    if compatible_converter is None:
        # task args can be protobuf messages, so not everything needs to be converted
        return v
    return compatible_converter.convert_from_proto(v)


def is_wrapped_known_proto_type(value, known_proto_types):
    return isinstance(value, Any) and any(
        (value.Is(proto_type.DESCRIPTOR) for proto_type in itertools.chain(_FRAMEWORK_KNOWN_PROTO_TYPES, known_proto_types)))

def can_pack_value_without_wrapping_first(value) -> bool:
    if isinstance(value, Value):
        return True
    
    if value is None:
        return True
    elif isinstance(value, (bool, int, float, str, bytes)):
        return True
    elif isinstance(value, (MapOfStringToValue, ArrayOfValue, TupleOfValue)):
        return True
    elif isinstance(value, Any):
        return True
    else:
        return False

def pack_value(value, converters: Iterable[ObjectProtobufConverter]) -> Value:
    if isinstance(value, Value):
        return value

    proto = Value()
    
    if value is None:
        proto.none_value.CopyFrom(NoneValue())
    elif isinstance(value, bool):
        proto.bool_value = value
    elif isinstance(value, int):
        proto.int_value = value
    elif isinstance(value, float):
        proto.double_value = value
    elif isinstance(value, str):
        proto.string_value = value
    elif isinstance(value, bytes):
        proto.bytes_value = value
    elif isinstance(value, MapOfStringToValue):
        proto.map_value.CopyFrom(value)
    elif isinstance(value, ArrayOfValue):
        proto.array_value.CopyFrom(value)
    elif isinstance(value, TupleOfValue):
        proto.tuple_value.CopyFrom(value)
    elif isinstance(value, Any):
        proto.any_value.CopyFrom(value)
    else:
        proto.any_value.CopyFrom(pack_any(wrap_value(value, converters)))

    return proto


def unpack_value(value: Value):
    if value.HasField('none_value'):
        return None
    elif value.HasField('bool_value'):
        return value.bool_value
    elif value.HasField('int_value'):
        return value.int_value
    elif value.HasField('double_value'):
        return value.double_value
    elif value.HasField('string_value'):
        return value.string_value
    elif value.HasField('bytes_value'):
        return value.bytes_value
    elif value.HasField('map_value'):
        return value.map_value 
    elif value.HasField('array_value'):
        return value.array_value
    elif value.HasField('tuple_value'):
        return value.tuple_value
    elif value.HasField('any_value'):
        return value.any_value
    else:
        raise ValueError(f'Unsupported Value type: {value}')


def convert_to_proto(
        data, 
        protobuf_converters: Iterable[ObjectProtobufConverter],
        use_legacy_types: bool = False
    ) -> Union[MapOfStringToAny, ArrayOfAny, TupleOfAny, Message, MapOfStringToValue, ArrayOfValue, TupleOfValue, Value]:

    if use_legacy_types:
        def convert(obj):
            if isinstance(obj, dict):
                proto = MapOfStringToAny()

                for k, v in obj.items():
                    v = pack_any(convert(v))
                    proto.items[k].CopyFrom(v)

                return proto

            elif is_tuple(obj):
                proto = TupleOfAny()

                for v in obj:
                    v = pack_any(convert(v))
                    proto.items.append(v)

                return proto
            elif isinstance(obj, list):
                proto = ArrayOfAny()

                for v in obj:
                    v = pack_any(convert(v))
                    proto.items.append(v)

                return proto
            else:
                return wrap_value(obj, protobuf_converters, use_legacy_types=True)
    else:        
        def convert(obj):
            if isinstance(obj, dict):
                proto = MapOfStringToValue()

                for k, v in obj.items():
                    proto.items[k].CopyFrom(pack_value(convert(v), protobuf_converters))

                return proto

            elif is_tuple(obj):
                proto = TupleOfValue()

                for v in obj:
                    proto.items.append(pack_value(convert(v), protobuf_converters))

                return proto
            elif isinstance(obj, list):
                proto = ArrayOfValue()

                for v in obj:
                    proto.items.append(pack_value(convert(v), protobuf_converters))

                return proto
            else:

                # todo this needs a more specialised way of wrapping / packing
                return wrap_value(obj, protobuf_converters)

    return convert(data)


def convert_from_proto(
        proto: Union[MapOfStringToAny, ArrayOfAny, TupleOfAny, Message, MapOfStringToValue, ArrayOfValue, TupleOfValue, Value], 
        known_proto_types : Iterable[Type[Message]],
        protobuf_converters: Iterable[ObjectProtobufConverter]
    ):
    known_proto_types = (known_proto_types or [])
    protobuf_converters = protobuf_converters or []

    def convert(obj):

        if isinstance(obj, MapOfStringToValue):
            return {k: convert(unpack_value(v)) for k, v in obj.items.items()}

        elif isinstance(obj, MapOfStringToAny):
            return {k: convert(unpack_any(v, known_proto_types)) for k, v in obj.items.items()}

        elif isinstance(obj, ArrayOfAny):
            return [convert(unpack_any(v, known_proto_types)) for v in obj.items]

        elif isinstance(obj, ArrayOfValue):
            return [convert(unpack_value(v)) for v in obj.items]

        elif isinstance(obj, TupleOfAny):
            return tuple(convert(unpack_any(v, known_proto_types)) for v in obj.items)

        elif isinstance(obj, TupleOfValue):
            return tuple(convert(unpack_value(v)) for v in obj.items)

        elif isinstance(obj, Value):
            return convert(unpack_value(obj))

        elif isinstance(obj, Any):
            if is_wrapped_known_proto_type(obj, known_proto_types):
                return convert(unpack_any(obj, known_proto_types))
            else:
                return obj  # leave it as an any and go no futher with it
        else:
            return unwrap_value(obj, protobuf_converters)

    return convert(proto)
