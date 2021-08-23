import inspect
from uuid import uuid4
from typing import get_type_hints
from itertools import chain
from google.protobuf.message import Message
from google.protobuf.any_pb2 import Any


def _gen_id():
    return str(uuid4())


def _type_name(thing):
    if inspect.isclass(thing):
        return ".".join([thing.__module__, thing.__name__])
    else:
        return ".".join([thing.__class__.__module__, thing.__class__.__name__])


def _task_type_for(fun, module_name=None):
    module = fun.__module__ if module_name is None else module_name
    return ".".join([module, fun.__name__])


def _try_next(iterator):
    try:
        return next(iterator)
    except StopIteration:
        return None


def _try_peek(iterator):
    try:
        n = next(iterator)
        return n, chain([n], iterator)
    except StopIteration:
        return None, iterator


def _is_named_tuple(value):
    # duck test to see if a value is a NamedTuple and not just a tuple
    if not isinstance(value, tuple):
        return False

    return hasattr(type(value), '_fields')


def _is_tuple(value):
    return isinstance(value, tuple) and not _is_named_tuple(value)


def _annotated_protos_for(fn):
    args = []

    try:
        for _, hint in get_type_hints(fn).items():
            if inspect.isclass(hint):
                args.append(hint)
            else:
                try:
                    args.extend(hint._args_)
                except:
                    pass
    except:
        args = []

    return [arg for arg in args if inspect.isclass(arg) and issubclass(arg, Message) and arg != Any]
