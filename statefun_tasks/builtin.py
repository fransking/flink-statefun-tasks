from statefun_tasks.types import Task
from statefun_tasks.utils import _gen_id


def builtin(task_name, is_fruitful=True):  

    def to_task(args=None, kwargs=None):
        args = () if args is None else args
        kwargs = {} if kwargs is None else kwargs
        return Task.from_fields(_gen_id(), task_name, args, kwargs, is_fruitful=is_fruitful)

    def wrapper(function):
        builtin.__setattr__(function.__name__, function)

        function.task_name = task_name
        function.to_task = to_task
        return function
        
    return wrapper
