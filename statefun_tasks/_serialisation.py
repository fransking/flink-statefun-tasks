from .messages_pb2 import TaskRequest, TaskResult, TaskException
from ._protobuf import _convert_from_proto, _convert_to_proto, _wrap_any, _parse_any_from_bytes


class DefaultSerialiser(object):
    def __init__(self, known_proto_types=[]):
        self._known_proto_types = set(known_proto_types)

    def register_proto_types(self, proto_types):
        self._known_proto_types.update(proto_types)

    def to_proto(self, obj):
        return _convert_to_proto(obj)

    def from_proto(self, proto):
        return _convert_from_proto(proto, self._known_proto_types)

    def serialise_request(self, task_request: TaskRequest, args, kwargs):
        task_request.args.CopyFrom(_convert_to_proto(args))
        task_request.kwargs.CopyFrom(_convert_to_proto(kwargs))

    def deserialise_request(self, task_request: TaskRequest):
        args = _convert_from_proto(task_request.args, self._known_proto_types)
        kwargs = _convert_from_proto(task_request.kwargs, self._known_proto_types)
        return args, kwargs

    def serialise_result(self, task_result: TaskResult, result):
        task_result.result.CopyFrom(_convert_to_proto(result))

    def deserialise_result(self, task_result: TaskResult, unwrap_tuple=False):
        result = _convert_from_proto(task_result.result, self._known_proto_types)

        if unwrap_tuple and isinstance(result, (list, tuple)) and len(result) == 1:
            # single results are still returned as single element list/tuple and are thus unpacked
            return result[0]
        else:
            return result
