from statefun_tasks.messages_pb2 import TaskRequest, TaskResult, TaskException, ArgsAndKwargs, MapOfStringToAny, \
    TupleOfAny
from statefun_tasks.protobuf import pack_any, unpack_any, _convert_from_proto, _convert_to_proto
from statefun_tasks.utils import _is_tuple
from google.protobuf.any_pb2 import Any
from google.protobuf.message import Message
from typing import Union


class DefaultSerialiser(object):
    """
    Default protobuf serialiser for Flink Tasks
    
    :param known_proto_types: an array of known protobuf types that will not be packed inside Any
    """

    def __init__(self, known_proto_types=[]):
        self._known_proto_types = set(known_proto_types)

    def register_proto_types(self, proto_types):
        self._known_proto_types.update(proto_types)

    def to_proto(self, item) -> Message:
        """
        Converts from Python to protobuf
        
        :param item: the item to convert
        :return: the protobuf message possibly packed in an Any
        """
        return _convert_to_proto(item)

    def from_proto(self, proto: Message, default=None):
        """
        Converts from protobuf to Python
        
        :param proto: the protobuf message
        :param option default: an optional default value to return
        :return: the Python type or default value if deserialisation return None
        """

        result = _convert_from_proto(proto, self._known_proto_types)
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
            request.args.CopyFrom(_convert_to_proto(args))
            request.kwargs.CopyFrom(_convert_to_proto(kwargs))

        return pack_any(request)

    def deserialise_args_and_kwargs(self, request: Any):
        """
        Deserialises Any into Python args and kwargs
        
        :param request: the protobuf message
        :return: tuple of args and kwargs
        """
        request = _convert_from_proto(request, self._known_proto_types)

        if isinstance(request, ArgsAndKwargs):
            args = _convert_from_proto(request.args, self._known_proto_types)
            kwargs = _convert_from_proto(request.kwargs, self._known_proto_types)
        else:
            args = request
            kwargs = {}

        return args, kwargs

    def to_args_and_kwargs(self, request: Any) -> ArgsAndKwargs:
        """
        Unpacks Any into Protobuf ArgsAndKwargs
        
        :param request: the protobuf message
        :return: ArgsAndKwargs
        """
        unpacked = _convert_from_proto(request, self._known_proto_types)
        if isinstance(unpacked, ArgsAndKwargs):
            return unpacked
        else:
            args = TupleOfAny()
            args.items.append(request)
            return ArgsAndKwargs(args=args, kwargs=MapOfStringToAny())

    @staticmethod
    def merge_args_and_kwargs(task_result: Message, task_args_and_kwargs: ArgsAndKwargs) -> Any:
        """
        Merges args & kwargs passed explicity to a task entry in a pipeline with results from the previous task.
        If there are no args & kwargs to merge then the result of the previous task is returned unchanged

        :param task_result: the request payload proto
        :param task_args_and_kwargs: the args and kwargs from the task entry to merge
        :return: Any
        """
        args_to_merge = task_args_and_kwargs.args
        kwargs = task_args_and_kwargs.kwargs

        if not any(args_to_merge.items) and not any(kwargs.items):
            return pack_any(task_result)
        
        # task result may be a single proto in which case we have to wrap into TupleOfAny to be able to extend
        if not isinstance(task_result, TupleOfAny):
            args = TupleOfAny()
            args.items.append(pack_any(task_result))
        else:
            args = task_result

        args.items.extend(args_to_merge.items)

        return pack_any(ArgsAndKwargs(args=args, kwargs=kwargs))

    @staticmethod
    def serialise_request(task_request: TaskRequest, request: Any, state=None, retry_policy=None):
        """
        Serialises args, kwargs and optional state into a TaskRequest
        
        :param task_request: the TaskRequest
        :param request: request (proto format)
        :param optional state: task state
        :param optional retry_policy: task retry policy
        """
        task_request.request.CopyFrom(request)
        task_request.state.CopyFrom(pack_any(_convert_to_proto(state)))
        
        if retry_policy:
            task_request.retry_policy.CopyFrom(retry_policy)

    def deserialise_request(self, task_request: TaskRequest):
        """
        Dserialises a TaskRequest back into args, kwargs and state
        
        :param task_request: the TaskRequest
        :return: tuple of task args, kwargs and state
        """
        args, kwargs = self.deserialise_args_and_kwargs(task_request.request)

        state = _convert_from_proto(task_request.state, self._known_proto_types)
        return args, kwargs, state

    def serialise_result(self, task_result: TaskResult, result, state):
        """
        Serialises the result of a task invocation into a TaskResult
        
        :param task_result: the TaskResult
        :param result: task result
        :param state: task state
        """
        task_result.result.CopyFrom(pack_any(_convert_to_proto(result)))
        task_result.state.CopyFrom(pack_any(_convert_to_proto(state)))

    def deserialise_result(self, task_result: TaskResult):
        """
        Dserialises a TaskResult back into result and state
        
        :param task_result: the TaskResult
        :return: tuple of result and state
        """
        result = _convert_from_proto(task_result.result, self._known_proto_types)
        state = _convert_from_proto(task_result.state, self._known_proto_types)

        return result, state

    def unpack_response(self, task_result_or_exception: Union[TaskResult, TaskException]):
        """
        Unpacks a TaskResult or TaskException back into result and state.
        If a TaskException is provided as input then the return value will be TupleOfAny, TaskException.state
        
        :param task_result: the TaskResult
        :return: tuple of result and state. 
        """
        if isinstance(task_result_or_exception, TaskResult):
            task_result = unpack_any(task_result_or_exception.result, self._known_proto_types)
            task_state = task_result_or_exception.state
        elif isinstance(task_result_or_exception, TaskException):
            task_result = TupleOfAny()
            task_state = task_result_or_exception.state
        else:
            raise ValueError(f'task_result_or_exception was neither TaskResult or TaskException')
        return task_result, task_state
