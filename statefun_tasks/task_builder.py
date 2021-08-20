from statefun_tasks.serialisation import DefaultSerialiser
from statefun_tasks.pipeline_builder import PipelineBuilder

from statefun_tasks.types import RetryPolicy, TaskAlreadyExistsException, \
    TASK_STATE_TYPE, TASK_REQUEST_TYPE, TASK_RESULT_TYPE, TASK_EXCEPTION_TYPE, TASK_ACTION_REQUEST_TYPE, PIPELINE_STATE_TYPE

from statefun_tasks.messages_pb2 import TaskRequest, TaskResult, TaskException, TaskAction

from statefun_tasks.type_helpers import _create_task_result, _create_task_exception
from statefun_tasks.context import TaskContext
from statefun_tasks.utils import _task_type_for
from statefun_tasks.pipeline import _Pipeline

from statefun_tasks.tasks import _FlinkTask
from statefun_tasks.actions import _FlinkAction

from statefun import ValueSpec, Context, Message
from datetime import timedelta
from typing import Union
import logging

_log = logging.getLogger('FlinkTasks')


def _run_pipeline(pipeline_proto):
    return PipelineBuilder.from_proto(pipeline_proto)


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

        # state types to register in Flink's @functions.bind() attribute
        self._value_specs = [
            ValueSpec(name="task_request", type=TASK_REQUEST_TYPE),
            ValueSpec(name="task_result", type=TASK_RESULT_TYPE),
            ValueSpec(name="task_exception", type=TASK_EXCEPTION_TYPE),
            ValueSpec(name="task_state", type=TASK_STATE_TYPE),
            ValueSpec(name="pipeline_state", type=PIPELINE_STATE_TYPE),
        ]

        self.register_builtin('run_pipeline', _run_pipeline)

    def value_specs(self):
        return self._value_specs

    def register_builtin(self, type_name, fun, **params):
        """
        Registers a built in Flink Task e.g. __builtins.run_pipeline
        This should only be used if you want to provide your own built in (pre-defined) tasks

        :param type_name: task type to expose the built in as
        :param fun: a function, partial or lambda representing the built in
        :param params: any additional parameters to the built in
        """
        self._bindings[f'__builtins.{type_name}'] = _FlinkTask(fun, self._serialiser, **params)

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
        self._bindings[fun.type_name] = _FlinkTask(wrapper or fun, self._serialiser, **params)

    def bind(
        self, 
        namespace: str = None, 
        worker_name: str = None, 
        retry_policy: RetryPolicy = None, 
        with_state: bool = False, 
        is_fruitful: bool = True, 
        module_name: str = None,
        with_context: bool = False):
        """
        Binds a function as a Flink Task

        :param namespace: namespace to use in place of the default
        :param worker_name: worker name to use in place of the default
        :param retry_policy: retry policy to use should the task throw an exception
        :param with_state: whether to pass a state object as the first (second if with_context is also set) parameter.  The return value should be a tuple of state, result (default False)
        :param is_fruitful: whether the function produces a fruitful result or simply returns None (default True)
        :param module_name: if specified then the task type used in addressing will be module_name.function_name
                            otherwise the Python module containing the function will be used
        :param with_context: whether to pass a Flink context object as the first parameter (default false)
        """
        def wrapper(function):
            def defaults():
                return {
                    'namespace': self._default_namespace if namespace is None else namespace,
                    'worker_name': self._default_worker_name if worker_name is None else worker_name,
                    'retry_policy': None if retry_policy is None else retry_policy.to_proto(),
                    'with_state': with_state,
                    'is_fruitful': is_fruitful,
                    'module_name': module_name,
                    'with_context': with_context
                }

            def send(*args, **kwargs):
                return PipelineBuilder().send(function, *args, **kwargs)

            function.defaults = defaults
            function.send = send

            self.register(function, **defaults())
            return function

        return wrapper

    def get_task(self, task_type):
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

            try:

                if message.is_type(TASK_REQUEST_TYPE):
                    task_input = message.as_type(TASK_REQUEST_TYPE)
                    task_context.task_name = task_input.type
                    task_context.apply_task_meta(task_input)
                    await self._invoke_task(task_context, task_input)

                elif message.is_type(TASK_ACTION_REQUEST_TYPE):
                    task_input = message.as_type(TASK_ACTION_REQUEST_TYPE)
                    task_context.task_name = f'Action [{TaskAction.Name(task_input.action)}]'
                    self._invoke_action(task_context, task_input)
                    
                elif message.is_type(TASK_RESULT_TYPE):
                    task_input = message.as_type(TASK_RESULT_TYPE)
                    task_context.task_name = task_input.type
                    self._resume_pipeline(task_context, task_input)

                elif message.is_type(TASK_EXCEPTION_TYPE):
                    task_input = message.as_type(TASK_EXCEPTION_TYPE)
                    task_context.task_name = task_input.type
                    self._resume_pipeline(task_context, task_input)

                else:
                    _log.error(f'Unsupported message type {message.typed_value.typename}')
                    return

            except Exception as ex:
                _log.error(f'Error invoking {task_context} - {ex}')
                self._fail(task_context, task_input, ex)

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

    def _get_pipeline(self, context):
        pipeline_protos = context.pipeline_state.pipeline

        if pipeline_protos is not None:
            return _Pipeline.from_proto(pipeline_protos, self._serialiser)
        else:
            raise ValueError(f'Missing pipeline for task_id - {context.get_task_id()}')

    def _try_get_pipeline(self, context):
        try:
            return self._get_pipeline(context)
        except:
            return None

    def _resume_pipeline(self, context, task_result_or_exception: Union[TaskResult, TaskException]):
        _log.info(f'Started {task_result_or_exception.type}, {context}')

        pipeline = self._get_pipeline(context)
        pipeline.resume(context, task_result_or_exception)
        
        _log.info(f'Finished {task_result_or_exception.type}, {context}')

    async def _invoke_task(self, context, task_request):
        _log.info(f'Started {task_request.type}, {context}')

        if context.storage.task_request is not None:
            # don't allow tasks to be overwritten
            raise TaskAlreadyExistsException(f'Task already exists: {task_request.id}')

        context.storage.task_request = task_request

        flink_task = self.get_task(task_request.type)
        task_result, task_exception, pipeline, task_state = await flink_task.run(context, task_request)

        # if task returns a pipeline then start it
        if pipeline is not None and not pipeline.is_empty():
            pipeline.begin(context, task_request, task_state) 

        # else if we have a task result return it
        elif task_result is not None:
            context.storage.task_result = task_result
            self._emit_result(context, task_request, task_result)

        # else if we have an task exception, attempt retry or return the error
        elif task_exception is not None:
            if self._attempt_retry(context, task_request, task_exception):
                return  # we have triggered a retry so ignore the result of this invocation

            context.storage.task_exception = task_exception
            self._emit_result(context, task_request, task_exception)

        _log.info(f'Finished {task_request.type}, {context}')

    def _invoke_action(self, context, action_request):
        _log.info(f'Started Action [{TaskAction.Name(action_request.action)}], task_id: {context.get_task_id()}')

        try:
            pipeline = self._try_get_pipeline(context)
            flink_action = _FlinkAction(context, pipeline)
            
            result = flink_action.run(action_request)
            self._emit_result(context, action_request, _create_task_result(action_request, result))

        except Exception as ex:
            self._emit_result(context, action_request, _create_task_exception(action_request, ex))

        _log.info(f'Finished Action [{TaskAction.Name(action_request.action)}], task_id: {context.get_task_id()}')

    def _emit_result(self, context, task_input, task_result):
        # either send a message to egress if reply_topic was specified
        if task_input.HasField('reply_topic'):
            context.send_egress_message(topic=task_input.reply_topic, value=task_result)

        # or call back to a particular flink function if reply_address was specified
        elif task_input.HasField('reply_address'):
            address, identifer = context.to_address_and_id(task_input.reply_address)
            context.send_message(address, identifer, task_result)

        elif isinstance(task_input, TaskRequest):
            # if this was a task request it could be part of a pipeline - i.e. called from another tasks so
            # either call back to original caller (e.g. if this is a result of a retry)
            if context.task_state.original_caller_id != '':
                context.send_message(context.task_state.original_caller_address, context.task_state.original_caller_id, task_result)

            # or call back to our caller (if there is one)
            elif context.get_caller_id() is not None:
                context.send_message(context.get_caller_address(), context.get_caller_id(), task_result)

    def _attempt_retry(self, context, task_request, task_exception):
        if task_exception.maybe_retry and task_exception.retry_policy is not None:           
            if context.task_state.retry_count >= task_exception.retry_policy.max_retries:
                return False

            # save the original caller address and id for this task_request prior to calling back to ourselves which would overwrite
            if context.task_state.original_caller_id == '':
                context.task_state.original_caller_id = context.get_caller_id()
                context.task_state.original_caller_address = context.get_caller_address()

            # remove previous task_request from state, increment retry count
            del context.storage.task_request
            context.task_state.retry_count += 1

            # send retry
            delay = timedelta(milliseconds=task_exception.retry_policy.delay_ms)

            if task_exception.retry_policy.exponential_back_off:
                delay = timedelta(milliseconds=task_exception.retry_policy.delay_ms ^ float(context.task_state.retry_count))

            if delay:
                context.send_message_after(delay, context.get_address(), context.get_task_id(), task_request)
            else:
                context.send_message(context.get_address(), context.get_task_id(), task_request)

            return True

        return False

    def _fail(self, context, task_input, ex):
        task_exception = _create_task_exception(task_input, ex)

        del context.storage.task_result
        context.storage.task_exception = task_exception

        self._emit_result(context, task_input, task_exception)
