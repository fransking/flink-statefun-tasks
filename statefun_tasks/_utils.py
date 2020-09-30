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


def _is_args_and_kwargs(test_args):
    try:
        args, kwargs = test_args
        return isinstance(kwargs, dict)
    except:
        return False


def _to_args_and_kwargs(test_args):
        if _is_args_and_kwargs(test_args):
            return test_args
        
        elif test_args is None:
            return (), {}
        else:
            return (test_args), {}
