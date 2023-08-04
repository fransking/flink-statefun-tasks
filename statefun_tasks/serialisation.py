from statefun_tasks.messages_pb2 import TaskRequest, TaskResult, ArgsAndKwargs
from statefun_tasks.protobuf import pack_any, _convert_from_proto, _convert_to_proto, ObjectProtobufConverter, _generate_default_converters
from statefun_tasks.utils import _is_tuple
from google.protobuf.any_pb2 import Any
from google.protobuf.message import Message
from typing import Type, Iterable


class DefaultSerialiser(object):
    """
    Default protobuf serialiser for Flink Tasks
    
    :param known_proto_types: an array of known protobuf types that will not be packed inside Any
    """
    def __init__(
            self,
            known_proto_types: Iterable[Type[Message]] = None,
            protobuf_converters: Iterable[ObjectProtobufConverter] = None):

        known_proto_types = known_proto_types or []
        # prepend default converters
        protobuf_converters = [*_generate_default_converters(), *(protobuf_converters or [])]
        
        self._known_proto_types = set(known_proto_types)
        self._protobuf_converters = set(protobuf_converters)

    def register_proto_types(self, proto_types: Iterable[Type[Message]]):
        self._known_proto_types.update(proto_types)

    def register_converters(self, converters):
        for converter in converters:
            if converter not in self._protobuf_converters:
                self._protobuf_converters.add(converter)

    def to_proto(self, item) -> Message:
        """
        Converts from Python to protobuf
        
        :param item: the item to convert
        :return: the protobuf message possibly packed in an Any
        """
        return _convert_to_proto(item, self._protobuf_converters)

    def from_proto(self, proto: Message, default=None):
        """
        Converts from protobuf to Python
        
        :param proto: the protobuf message
        :param option default: an optional default value to return
        :return: the Python type or default value if deserialisation return None
        """

        result = _convert_from_proto(proto, self._known_proto_types, self._protobuf_converters)
        return result if result is not None else default

    def serialise_args_and_kwargs(self, args, kwargs) -> Any:
        """
        Serialises Python args and kwargs into protobuf
        
        If there is a single arg and no kwargs and the arg is already a protobuf it returns
        that instance packed inside an Any.  Otherwise it returns an ArgsAndKwargs packed in an Any.

        :param args: the Python args
        :param kwargs: the Python kwargs
        :return: protobuf Any
        """

        # if kwargs are empty and this is a single protobuf arguments then 
        # send in simple format i.e. fn(protobuf) -> protobuf as opposed to fn(*args, **kwargs) -> (*results,)
        # so as to aid calling flink functions written in other frameworks that might not understand
        # how to deal with the concept of keyword arguments or tuples of arguments
        if isinstance(args, Message) and not kwargs:
            request = args
        else:
            args = args if _is_tuple(args) else (args,)
            request = ArgsAndKwargs()
            request.args.CopyFrom(_convert_to_proto(args, self._protobuf_converters))
            request.kwargs.CopyFrom(_convert_to_proto(kwargs, self._protobuf_converters))

        return pack_any(request)

    def deserialise_args_and_kwargs(self, request: Any):
        """
        Deserialises Any into Python args and kwargs
        
        :param request: the protobuf message
        :return: tuple of args and kwargs
        """
        request = _convert_from_proto(request, self._known_proto_types, self._protobuf_converters)

        if isinstance(request, ArgsAndKwargs):
            args = _convert_from_proto(request.args, self._known_proto_types, self._protobuf_converters)
            kwargs = _convert_from_proto(request.kwargs, self._known_proto_types, self._protobuf_converters)
        else:
            args = request
            kwargs = {}

        return args, kwargs

    def serialise_request(self, task_request: TaskRequest, request: Any, state=None, retry_policy=None):
        """
        Serialises args, kwargs and optional state into a TaskRequest
        
        :param task_request: the TaskRequest
        :param request: request (proto format)
        :param optional state: task state
        :param optional retry_policy: task retry policy
        """
        task_request.request.CopyFrom(request)
        task_request.state.CopyFrom(pack_any(_convert_to_proto(state, self._protobuf_converters)))

        if retry_policy:
            task_request.retry_policy.CopyFrom(retry_policy)

    def deserialise_request(self, task_request: TaskRequest):
        """
        Dserialises a TaskRequest back into args, kwargs and state
        
        :param task_request: the TaskRequest
        :return: tuple of task args, kwargs and state
        """
        args, kwargs = self.deserialise_args_and_kwargs(task_request.request)

        state = _convert_from_proto(task_request.state, self._known_proto_types, self._protobuf_converters)
        return args, kwargs, state

    def serialise_result(self, task_result: TaskResult, result, state):
        """
        Serialises the result of a task invocation into a TaskResult
        
        :param task_result: the TaskResult
        :param result: task result
        :param state: task state
        """
        task_result.result.CopyFrom(pack_any(_convert_to_proto(result, self._protobuf_converters)))
        task_result.state.CopyFrom(pack_any(_convert_to_proto(state, self._protobuf_converters)))

    def deserialise_result(self, task_result: TaskResult):
        """
        Dserialises a TaskResult back into result and state
        
        :param task_result: the TaskResult
        :return: tuple of result and state
        """
        result = _convert_from_proto(task_result.result, self._known_proto_types, self._protobuf_converters)
        state = _convert_from_proto(task_result.state, self._known_proto_types, self._protobuf_converters)

        return result, state
