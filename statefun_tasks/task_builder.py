from statefun_tasks.serialisation import DefaultSerialiser
from statefun_tasks.pipeline_builder import PipelineBuilder

from statefun_tasks.types import Task, RetryPolicy, \
    TASK_STATE_TYPE, TASK_REQUEST_TYPE, TASK_RESULT_TYPE, TASK_EXCEPTION_TYPE, PIPELINE_STATE_TYPE

from statefun_tasks.messages_pb2 import TaskResult, TaskException, TaskRequest, Address
from statefun_tasks.type_helpers import _create_task_result, _create_task_exception
from statefun_tasks.context import TaskContext
from statefun_tasks.utils import _task_type_for, _unpack_single_tuple_args, _gen_id
from statefun_tasks.pipeline import _Pipeline
from statefun_tasks.tasks import FlinkTask
from statefun_tasks.task_impl.handlers import TaskRequestHandler, TaskResponseHandler, TaskActionHandler, ChildPipelineHandler
from statefun_tasks.events import EventHandlers
from statefun_tasks.builtin_tasks import run_pipeline, flatten_results

from statefun import ValueSpec, Context, Message
from datetime import timedelta
import logging

_log = logging.getLogger('FlinkTasks')


class FlinkTasks(object):
    """
    Flink Tasks implementation

    An instance should be instantiated at the top of your api file and used to decorate functions
    to be exposed as tasks.

    :param default_namespace: namespace to expose functions under. Maps to Flink Statefun function namespace in module.yaml
    :param default_worker_name: worker name to expose.  Maps to Flink Statefun function type in module.yaml
    :param egress_type_name: egress type name.  Maps to Flink Statefun egress in module.yaml
    :param optional serialiser: serialiser to use (will use DefaultSerialiser if not set)
    """
    def __init__(self, default_namespace: str = None, default_worker_name: str = None, egress_type_name: str = None, serialiser = None):
        self._default_namespace = default_namespace
        self._default_worker_name = default_worker_name
        self._egress_type_name = egress_type_name
        self._serialiser = serialiser if serialiser is not None else DefaultSerialiser()
        self._bindings = {}
        self._events = EventHandlers()
        
        # to register in Flink's @functions.bind() attribute
        self._value_specs = [
            ValueSpec(name="task_request", type=TASK_REQUEST_TYPE),
            ValueSpec(name="task_result", type=TASK_RESULT_TYPE),
            ValueSpec(name="task_exception", type=TASK_EXCEPTION_TYPE),
            ValueSpec(name="task_state", type=TASK_STATE_TYPE),
            ValueSpec(name="pipeline_state", type=PIPELINE_STATE_TYPE),
        ]

        self.register_builtin(run_pipeline, with_context=True, with_state=True)
        self.register_builtin(flatten_results)

        self._handlers = [
            TaskRequestHandler(),
            TaskResponseHandler(),
            TaskActionHandler(),
            ChildPipelineHandler()
        ]

    @staticmethod
    def extend(function, retry_policy: RetryPolicy = None, **params):
        """
        Adds the extensions to make a function unsable by a PipelineBuilder -> fn.send(), fn.to_task() and fn.defaults()

        :param function: the function to wrap
        :param retry_policy: retry policy to use should the task throw an exception
        :param params: any additional parameters to the Flink Task (such as a retry policy)
        """
        def defaults():
            return {
                'retry_policy': None if retry_policy is None else retry_policy.to_proto(),
                **params
            }

        def send(*args, **kwargs):
            return PipelineBuilder().send(function, *args, **kwargs)

        def to_task(args, kwargs, is_finally=False, parameters={}):
            parameters = {**defaults(), **parameters}

            if is_finally:
                parameters['is_fruitful'] = False
            
            module_name = parameters.get('module_name', None)
            task_type = _task_type_for(function, module_name)

            task_id = parameters.pop('task_id') or _gen_id()
            args = _unpack_single_tuple_args(args)

            return Task.from_fields(task_id, task_type, args, kwargs, is_finally=is_finally, **parameters)

        function.send = send
        function.to_task = to_task
        function.defaults = defaults

        return function

    def bind(
        self, 
        namespace: str = None, 
        worker_name: str = None, 
        retry_policy: RetryPolicy = None, 
        with_state: bool = False, 
        is_fruitful: bool = True, 
        module_name: str = None,
        with_context: bool = False,
        display_name: str = None,
        task_id: str = None):
        """
        Decorator to bind a function as a Flink Task

        :param namespace: namespace to use in place of the default
        :param worker_name: worker name to use in place of the default
        :param retry_policy: retry policy to use should the task throw an exception
        :param with_state: whether to pass a state object as the first (second if with_context is also set) parameter.  The return value should be a tuple of state, result (default False)
        :param is_fruitful: whether the function produces a fruitful result or simply returns None (default True)
        :param module_name: if specified then the task type used in addressing will be module_name.function_name
                            otherwise the Python module containing the function will be used
        :param with_context: whether to pass a Flink context object as the first parameter (default false)
        :param display_name: optional friendly name for this task
        :param task_id: optional set the fixed id for this task in order to make it stateful
        """
        
        def wrapper(function):

            function = FlinkTasks.extend(function, 
                    namespace = self._default_namespace if namespace is None else namespace,
                    worker_name = self._default_worker_name if worker_name is None else worker_name,
                    retry_policy = retry_policy,
                    with_state = with_state,
                    is_fruitful = is_fruitful,
                    module_name = module_name,
                    with_context = with_context,
                    display_name = display_name,
                    task_id = task_id
            )

            self.register(function, wrapper=None, **function.defaults())
            return function

        return wrapper

    @property
    def events(self) -> EventHandlers:
        """
        EventHandler for this FlinkTasks instance
        """
        return self._events

    def value_specs(self):
        return self._value_specs

    def register_builtin(self, fun, **params):
        """
        Registers a built in Flink Task e.g. __builtins.run_pipeline
        This should only be used if you want to provide your own built in (pre-defined) tasks

        :param fun: a function, partial or lambda representing the built in
        :param params: any additional parameters to the Flink Task (such as a retry policy)
        """
        self._bindings[fun.task_name] = FlinkTask(fun, self._serialiser, self.events, **params)

    def register(self, fun, wrapper=None, module_name=None, **params):
        """
        Registers a Python function as a Flink Task.
        Normally you would attribute the function with @tasks.bind() instead

        :param fun: the python function
        :param optional wrapper: if wrapping a task function with e.g. functools.wraps then pass the wrapper here
        :param optional module_name: the module name to register the task under which by default is the Python module name containing the function
        :param params: any additional parameters to the Flink Task (such as a retry policy)
        """

        if fun is None:
            raise ValueError("function instance must be provided")

        fun.type_name = _task_type_for(fun, module_name)
        self._bindings[fun.type_name] = FlinkTask(wrapper or fun, self._serialiser, self.events, **params)


    def get_task(self, task_type) -> FlinkTask:
        """
        Returns the Flink Task instance for a given task type name

        :param task_type: task type name e.g. examples.multiply
        :return: the Flink Task
        """
        if task_type in self._bindings:
            return self._bindings[task_type]
        else:
            raise RuntimeError(f'{task_type} is not a registered FlinkTask')

    async def run_async(self, context: Context, message: Message):
        """
        Runs an async Flink Task

        :param context: context object provided by Flink
        :param message: the task input protobuf message
        """
        with TaskContext(context, self._egress_type_name, self._serialiser) as task_context:

            for handler in self._handlers:
                task_input = handler.unpack(task_context, message)
                if task_input is not None:
                    
                    try:
                        _log.info(f'Starting {task_context}')

                        await handler.handle_message(self, task_context, task_input)

                        _log.info(f'Finished {task_context}')

                    except Exception as ex:
                        _log.error(f'Error invoking {task_context} - {ex}')
                        self.fail(task_context, task_input, ex)
                        
                    finally:
                        return
            
            _log.error(f'Unsupported message type {message.typed_value.typename}')



    @staticmethod
    def send(func, *args, **kwargs) -> PipelineBuilder:
        """
        Returns a PipelineBuilder with this task as the first item in the pipeline

        :param func: a function decorated with @tasks.bind()
        :param args: task args
        :param kwargs: task kwargs
        :return: a pipeline builder
        """
        try:
            send_func = func.send
        except AttributeError:
            raise AttributeError(
                'Expected function to have a send attribute. Make sure it is decorated with @tasks.bind()')
        return send_func(*args, **kwargs)

    def clone_task_request(self, context: TaskContext) -> TaskRequest:
        """
        Returns a complete copy of the TaskRequest associated with this TaskContext with the 
        caller address details extracted as necessary from the context

        :param context: TaskContext
        :return: a TaskRequest
        """
        task_request = TaskRequest()
        task_request.CopyFrom(context.storage.task_request)

        # copy caller details from context unless explictly set already
        if not task_request.HasField('reply_address') and not task_request.HasField('reply_topic'):

            address, caller_id = self._get_caller_address_and_id(context, task_request)
            
            if address is not None and caller_id is not None:
                namespace, function_type = address.split('/')
                task_request.reply_address.CopyFrom(Address(namespace=namespace, type=function_type, id=caller_id))

        return task_request

    def unpack_task_request(self, task_request: TaskRequest) -> tuple:
        """
        Unpacks a TaskRequest into args, kwargs and state

        :param task_request: TaskRequest
        :return: args, kwargs and state from this task_request
        """
        args, kwargs, state = self._serialiser.deserialise_request(task_request)
        return args, kwargs, state

    def send_result(self, context: TaskContext, task_request: TaskRequest, result, state=Ellipsis, delay: timedelta=None, cancellation_token: str = ""):
        """
        Sends a result
        :param context: TaskContext
        :param task_request: the incoming TaskRequest
        :param result: the result(s) to return
        :param state: the state to include in the result.  If not specified it will be copied from the TaskRequest
        :param optional delay: the delay before Flink sends the result
        :param optional cancellation_token: a cancellation token to associate with this message
        """
        task_result = _create_task_result(task_request)

        if state == Ellipsis:  # default value since None is perfectly valid state
            state = task_request.state

        self._serialiser.serialise_result(task_result, result, state)

        self.emit_result(context, task_request, task_result, delay,  cancellation_token)

    def fail(self, context, task_input, ex, delay: timedelta=None, cancellation_token: str = ""):
        """
        Sends a failure
        :param context: TaskContext
        :param task_input: the incoming TaskRequest or TaskActionRequest
        :param ex: the exception to return
        :param optional delay: the delay before Flink sends the result
        :param optional cancellation_token: a cancellation token to associate with this message
        """
        task_exception = _create_task_exception(task_input, ex)
        self.emit_result(context, task_input, task_exception, delay,  cancellation_token)

    def get_pipeline(self, context):
        pipeline_protos = context.pipeline_state.pipeline

        if pipeline_protos is not None:
            return _Pipeline.from_proto(pipeline_protos, self._serialiser, self._events)
        else:
            raise ValueError(f'Missing pipeline for task_id - {context.get_task_id()}')

    def try_get_pipeline(self, context):
        try:
            return self.get_pipeline(context)
        except:
            return None

    def emit_result(self, context, task_input, task_result, delay: timedelta=None, cancellation_token: str = None):
        # copy over invocation id
        if isinstance(task_result, (TaskResult, TaskException)):
            task_result.invocation_id = task_input.invocation_id
            
        # either send a message to egress if reply_topic was specified
        if task_input.HasField('reply_topic'):
            context.send_egress_message(topic=task_input.reply_topic, value=task_result)

        # or call back to a particular flink function if reply_address was specified
        elif task_input.HasField('reply_address'):
            address, identifer = context.to_address_and_id(task_input.reply_address)
            context.send_message(address, identifer, task_result, delay, cancellation_token)

        elif isinstance(task_input, TaskRequest):
            address, caller_id = self._get_caller_address_and_id(context, task_input)
            if address is not None and caller_id is not None:
                context.send_message(address, caller_id, task_result, delay, cancellation_token)

        # clean up
        if task_input.uid in context.task_state.by_uid:
            del context.task_state.by_uid[task_input.uid]

    def _get_caller_address_and_id(self, context, task_request):
        task_state = context.task_state.by_uid[task_request.uid]
        
        # either as original caller (e.g. if this is a result of a retry) or context.get_caller_...() otherwise
        address = task_state.original_caller_address if task_state.original_caller_address != '' else context.get_caller_address()
        caller_id = task_state.original_caller_id if task_state.original_caller_id != '' else context.get_caller_id()

        return address, caller_id
