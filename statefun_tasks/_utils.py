from uuid import uuid4


def _gen_id():
    return str(uuid4())


def _task_type_for(fun):
    return ".".join([fun.__module__, fun.__name__])


def _try_next(iterator):
    try:
        return next(iterator)
    except StopIteration:
        return None
