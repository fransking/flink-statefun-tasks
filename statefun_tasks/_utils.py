import inspect
from uuid import uuid4
from google.protobuf.message import Message


def _gen_id():
    return str(uuid4())


def _type_name(thing):
    if inspect.isclass(thing):
        return ".".join([thing.__module__, thing.__name__])
    else:
        return ".".join([thing.__class__.__module__, thing.__class__.__name__])


def _task_type_for(fun):
    return ".".join([fun.__module__, fun.__name__])


def _try_next(iterator):
    try:
        return next(iterator)
    except StopIteration:
        return None


def _is_named_tuple(value):
    # duck test to see if a value is a NamedTuple and not just a tuple
    if not isinstance(value, tuple):
        return False

    return hasattr(type(value), '_fields')


def _is_tuple(value):
    return isinstance(value, tuple) and not _is_named_tuple(value)


def _protobuf_args(annotations):
    args = []

    for arg_name, annotation in annotations.items():
        if inspect.isclass(annotation):
            args.append(annotation)
        else:
            try:
                args.extend(annotation._args_)
            except:
                pass

    return [arg for arg in args if inspect.isclass(arg) and issubclass(arg, Message)]
