import itertools
from abc import ABC, abstractmethod
from statefun_tasks.utils import _is_tuple
from statefun_tasks.messages_pb2 import MapOfStringToAny, ArrayOfAny, TupleOfAny, TaskEntry, GroupEntry, NoneValue, \
    TaskRetryPolicy, TaskRequest, TaskResult, TaskException, TaskState, TaskResults, Pipeline, PipelineEntry, Address, \
    ArgsAndKwargs, PipelineState

from google.protobuf.wrappers_pb2 import DoubleValue, Int64Value, BoolValue, StringValue, BytesValue
from google.protobuf.any_pb2 import Any
from google.protobuf.message import Message

from typing import Union, TypeVar, Generic, Iterable, List

_FRAMEWORK_KNOWN_PROTO_TYPES = [
    # wrappers
    DoubleValue,
    Int64Value,
    BoolValue,
    StringValue,
    BytesValue,
    NoneValue,

    # flink task types
    MapOfStringToAny,
    TupleOfAny,
    ArrayOfAny,
    TaskEntry,
    GroupEntry,
    TaskRetryPolicy,
    TaskRequest,
    TaskResult,
    TaskException,
    TaskState,
    TaskResults,
    Pipeline,
    PipelineEntry,
    Address,
    ArgsAndKwargs,
    PipelineState
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

    def convert_to_proto(self, obj: object) -> TScalarProtoType:
        return NoneValue()

    def convert_from_proto(self, message: TScalarProtoType) -> object:
        return None


def _generate_default_converters() -> List[ScalarTypeProtobufConverter]:
    return [
        ScalarTypeProtobufConverter(float, DoubleValue),
        ScalarTypeProtobufConverter(int, Int64Value),
        ScalarTypeProtobufConverter(bool, BoolValue),
        ScalarTypeProtobufConverter(str, StringValue),
        ScalarTypeProtobufConverter(bytes, BytesValue),
        NoneTypeProtobufConverter()
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


def _wrap_value(v: object, converters: Iterable[ObjectProtobufConverter]) -> Message:
    if isinstance(v, Message):
        # already protobuf so no need to attempt conversion
        return v

    compatible_converter = next((c for c in converters if c.can_convert_to_proto(v)), None)
    if compatible_converter is None:
        raise ValueError(
            f'Cannot convert value of type {type(v)} to protobuf. '
            'Try converting to protobuf first, or provide a compatible converter.')
    return compatible_converter.convert_to_proto(v)


def _unwrap_value(v: Message, converters: Iterable[ObjectProtobufConverter]):
    compatible_converter = next((c for c in converters if c.can_convert_from_proto(v)), None)
    if compatible_converter is None:
        # task args can be protobuf messages, so not everything needs to be converted
        return v
    return compatible_converter.convert_from_proto(v)


def _parse_any_from_bytes(bytes) -> Any:
    proto = Any()
    proto.ParseFromString(bytes)
    return proto


def _is_wrapped_known_proto_type(value, known_proto_types):
    return isinstance(value, Any) and any(
        (value.Is(proto_type.DESCRIPTOR) for proto_type in itertools.chain(_FRAMEWORK_KNOWN_PROTO_TYPES, known_proto_types)))


def _convert_to_proto(data, protobuf_converters: Iterable[ObjectProtobufConverter]) \
        -> Union[MapOfStringToAny, ArrayOfAny, TupleOfAny, Message]:
    def convert(obj):
        if isinstance(obj, dict):
            proto = MapOfStringToAny()

            for k, v in obj.items():
                v = pack_any(convert(v))
                proto.items[k].CopyFrom(v)

            return proto

        elif _is_tuple(obj):
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
            return _wrap_value(obj, protobuf_converters)

    return convert(data)


def _convert_from_proto(proto: Union[MapOfStringToAny, ArrayOfAny, TupleOfAny, Message], known_proto_types,
                        protobuf_converters: Iterable[ObjectProtobufConverter]):
    known_proto_types = (known_proto_types or [])
    protobuf_converters = protobuf_converters or []

    def convert(obj):
        if isinstance(obj, MapOfStringToAny):
            return {k: convert(unpack_any(v, known_proto_types)) for k, v in obj.items.items()}

        elif isinstance(obj, ArrayOfAny):
            return [convert(unpack_any(v, known_proto_types)) for v in obj.items]

        elif isinstance(obj, TupleOfAny):
            return tuple(convert(unpack_any(v, known_proto_types)) for v in obj.items)

        elif isinstance(obj, Any):
            if _is_wrapped_known_proto_type(obj, known_proto_types):
                return convert(unpack_any(obj, known_proto_types))
            else:
                return obj  # leave it as an any and go no futher with it
        else:
            return _unwrap_value(obj, protobuf_converters)

    return convert(proto)
