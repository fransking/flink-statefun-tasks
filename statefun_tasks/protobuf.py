from statefun_tasks.utils import _is_tuple
from statefun_tasks.messages_pb2 import MapOfStringToAny, ArrayOfAny, TupleOfAny, TaskEntry, GroupEntry, NoneValue, \
    TaskRetryPolicy, TaskRequest, TaskResult, TaskException, TaskState, TaskResults, Pipeline, PipelineEntry, Address, \
    ArgsAndKwargs, PipelineState

from google.protobuf.wrappers_pb2 import DoubleValue, Int64Value, BoolValue, StringValue, BytesValue
from google.protobuf.any_pb2 import Any
from google.protobuf.message import Message

from typing import Union

_SCALAR_TYPE_MAP = {
    float: DoubleValue,
    int: Int64Value,
    bool: BoolValue,
    str: StringValue,
    bytes: BytesValue
}

_KNOWN_PROTO_TYPES = [
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


def pack_any(value) -> Any:
    if isinstance(value, Any):
        return value

    proto = Any()
    proto.Pack(value)
    return proto


def unpack_any(value, known_proto_types):
    known_proto_types = _KNOWN_PROTO_TYPES + list(known_proto_types)

    if isinstance(value, Any):
        for proto_type in known_proto_types:
            if value.Is(proto_type.DESCRIPTOR):
                unwrapped = proto_type()
                value.Unpack(unwrapped)
                return unwrapped
        return value

    return value

def _wrap_value(v):
    # if none return NoneValue wrapper
    if v is None:
        return NoneValue()

    python_type = type(v)
    # wrap scalars in protobuf wrappers
    if python_type in _SCALAR_TYPE_MAP:
        mapped = _SCALAR_TYPE_MAP[python_type]()
        mapped.value = v
    # leave other protobufs alone
    elif isinstance(v, Message):
        mapped = v
    else:
        raise ValueError(f'Cannot wrap non-scalar {type(v)} in a protobuf.  Try converting to protobuf first.')

    return mapped


def _unwrap_value(v):
    # if NoneValue wrapper return None
    if isinstance(v, NoneValue):
        return None

    proto_type = type(v)
    # unwrap scalars in protobuf wrappers
    if proto_type in _SCALAR_TYPE_MAP.values():
        return v.value
    return v


def _parse_any_from_bytes(bytes) -> Any:
    proto = Any()
    proto.ParseFromString(bytes)
    return proto


def _is_wrapped_known_proto_type(value, known_proto_types):
    return isinstance(value, Any) and any([value.Is(proto_type.DESCRIPTOR) for proto_type in known_proto_types])


def _convert_to_proto(data) -> Union[MapOfStringToAny, ArrayOfAny, TupleOfAny, Message]:
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
            return _wrap_value(obj)

    return convert(data)


def _convert_from_proto(proto: Union[MapOfStringToAny, ArrayOfAny, TupleOfAny, Message], known_proto_types=[]):
    all_known_proto_types = _KNOWN_PROTO_TYPES + list(known_proto_types)

    def convert(obj):
        if isinstance(obj, MapOfStringToAny):
            return {k: convert(unpack_any(v, all_known_proto_types)) for k, v in obj.items.items()}

        elif isinstance(obj, ArrayOfAny):
            return [convert(unpack_any(v, all_known_proto_types)) for v in obj.items]

        elif isinstance(obj, TupleOfAny):
            return tuple(convert(unpack_any(v, all_known_proto_types)) for v in obj.items)

        elif isinstance(obj, Any):
            if _is_wrapped_known_proto_type(obj, all_known_proto_types):
                return convert(unpack_any(obj, all_known_proto_types))
            else:
                return obj  # leave it as an any and go no futher with it
        else:
            return _unwrap_value(obj)

    return convert(proto)
