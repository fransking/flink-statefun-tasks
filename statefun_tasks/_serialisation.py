from .messages_pb2 import TaskRequest, TaskResult, TaskException, ArgsAndKwargs
from ._protobuf import _convert_from_proto, _convert_to_proto, _pack_any, _unpack_any, _parse_any_from_bytes
from ._utils import _is_tuple
from google.protobuf.any_pb2 import Any
from google.protobuf.message import Message


class DefaultSerialiser(object):
    def __init__(self, known_proto_types=[]):
        self._known_proto_types = set(known_proto_types)

    def register_proto_types(self, proto_types):
        self._known_proto_types.update(proto_types)

    def to_proto(self, obj):
        return _convert_to_proto(obj)

    def from_proto(self, proto):
        return _convert_from_proto(proto, self._known_proto_types)

    def serialise_request(self, task_request: TaskRequest, args, kwargs, state=None):

        # if kwargs are empty and this is a single protobuf arguments then 
        # send in simple format i.e. fn(protobuf) -> protobuf as opposed to fn(*args, **kwargs) -> (*results,)
        # so as to aid calling flink functions written in other frameworks that might not understand
        # how to deal with the concept of keyword arguments or tuples of arguments
        if isinstance(args, Message) and not kwargs:
            request = args
        else:
            args = args if _is_tuple(args) else (args,)
            request = ArgsAndKwargs()
            request.args.CopyFrom(_convert_to_proto(args))
            request.kwargs.CopyFrom(_convert_to_proto(kwargs))

        task_request.request.CopyFrom(_pack_any(request))
        task_request.state.CopyFrom(_pack_any(_convert_to_proto(state)))

    def deserialise_request(self, task_request: TaskRequest):
        request = _convert_from_proto(task_request.request, self._known_proto_types)

        # do the reverse from serialise_request
        if isinstance(request, ArgsAndKwargs):
            args = _convert_from_proto(request.args, self._known_proto_types)
            kwargs = _convert_from_proto(request.kwargs, self._known_proto_types)
        else:
            args = request
            kwargs = {}

        state = _convert_from_proto(task_request.state, self._known_proto_types)
        return args, kwargs, state

    def serialise_result(self, task_result: TaskResult, result, state):
        task_result.result.CopyFrom(_pack_any(_convert_to_proto(result)))
        task_result.state.CopyFrom(_pack_any(_convert_to_proto(state)))

    def deserialise_result(self, task_result: TaskResult):
        result = _convert_from_proto(task_result.result, self._known_proto_types)
        state = _convert_from_proto(task_result.state, self._known_proto_types)

        return result, state
