from statefun_tasks import FlinkTasks
import asyncio
import cloudpickle
import logging


_log = logging.getLogger('FlinkTasks')
__defaults = None


def enable_inline_tasks(tasks: FlinkTasks):
    """
    Enables inline tasks support.  Inline tasks work by sending pickled code as well as data 
    to a general purpose run_code task as an alternative to deploying functions decorated with 
    @tasks.bind()

    N.B. there are security considerations whenever using pickled code.  Only accept code from 
    trusted sources and consider implementing your own implementation of this extension with suitable
    checks - e.g. you might want to only accept signed code or restrict what global functions and 
    imports are available.  This extension is included as an example of what is possible.

    :param tasks: FlinkTasks to enable @inline_task() for
    """
    @tasks.bind(module_name='__builtins', with_context=True, with_state=True)
    async def run_code(context, state, __with_context, __with_state, __code, *args, **kwargs):
        
        fn = cloudpickle.loads(__code)
        fn_args = []

        if __with_context:
            fn_args.append(context)

        if __with_state:
            fn_args.append(state)

        fn_args.extend(args)

        safer_locals = {'fn': fn, 'args': fn_args, 'kwargs': kwargs}

        # sample restrictions on loaded code
        # ----------------------------------
        #
        # safer_builtins = {**fn.__globals__['__builtins__']}
        # fn.__globals__['__builtins__'] = safer_builtins
        # del safer_builtins['exit']

        # safer_open = open
        # def safer_open(file, *sargs, **skwargs):
        #     # you might check and raise error if file not in valid list of files for example...
        #     open(file, *sargs, **skwargs)
        # safer_builtins['open'] = safer_open

        exec('__res = fn(*args, **kwargs)', {}, safer_locals)
        res = safer_locals['__res']

        if asyncio.iscoroutinefunction(fn):
            res = await res

        if __with_state:
            return res
        else:
            return state, res

    global __defaults
    __defaults = run_code.defaults()

    _log.warning('Inline tasks enabled. This is a potential security risk')


def inline_task(include=None, with_context=False, with_state=False, **params):
    """
    Declares an inline Flink task
    :param include: list of modules to include with the pickled code
    :param with_context: If set the first parameter to the function is exepcted to be the task context
    :param with_state: If set the next parameter is expected to be the task state
    :param params: any additional parameters to the Flink Task (such as a retry policy)
    :return: inline Flink task
    """
    
    includes = include or []

    def pickle(fn):
        for module in includes:
            cloudpickle.register_pickle_by_value(module)

        code = cloudpickle.dumps(fn)

        for module in includes:
            cloudpickle.unregister_pickle_by_value(module)

        return code

    def decorator(fn):

        def send(*args, **kwargs):

            if __defaults is None:
                raise ValueError('Inline tasks should be enabled with enable_inline_tasks() first')

            code = pickle(fn)

            def run_code():
                pass

            fn_kwargs = {**kwargs, '__with_context': with_context, '__with_state': with_state, '__code': code}
            display_name = params.setdefault(f'{fn.__module__}.{fn.__name__}')
            return FlinkTasks.extend(run_code, **__defaults).send(*args, **fn_kwargs).set(display_name=display_name)

        def to_task(args, kwargs, is_finally=False, parameters=None):

            if __defaults is None:
                raise ValueError('Inline tasks should be enabled with enable_inline_tasks() first')

            code = pickle(fn)

            def run_code():
                pass

            fn_kwargs = {**kwargs, '__with_context': with_context, '__with_state': with_state, '__code': code}
            params.setdefault(f'{fn.__module__}.{fn.__name__}')
            return FlinkTasks.extend(run_code, **__defaults).to_task(args, fn_kwargs, is_finally, params)

        fn.send = send
        fn.to_task = to_task

        return fn
    
    return decorator
