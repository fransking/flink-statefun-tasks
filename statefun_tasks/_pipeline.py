from ._utils import _gen_id, _task_type_for, _is_tuple
from ._serialisation import DefaultSerialiser
from ._types import _TaskEntry, _GroupEntry, RetryPolicy
from ._context import _TaskContext
from ._pipeline_utils import _get_initial_tasks, _mark_task_complete, _get_task_entry, _get_next_step_in_pipeline, \
    _extend_args, _save_task_result_or_exception, _aggregate_group_results, _try_get_finally_task
from .messages_pb2 import TaskRequest, TaskResult, TaskException, GroupEntry, TaskEntry, Pipeline

from typing import Union, Iterable
from uuid import uuid4


class _Pipeline(object):
    def __init__(self, pipeline: list, serialiser=None):
        self._pipeline = pipeline
        self._serialiser = serialiser if serialiser is not None else DefaultSerialiser()

    def to_proto(self) -> Pipeline:
        pipeline = Pipeline(entries=[p.to_proto(self._serialiser) for p in self._pipeline])
        return pipeline

    @staticmethod
    def from_proto(pipeline_proto: Pipeline, serialiser):
        pipeline = []

        for proto in pipeline_proto.entries:
            if proto.HasField('task_entry'):
                pipeline.append(_TaskEntry.from_proto(proto, serialiser))
            elif proto.HasField('group_entry'):
                pipeline.append(_GroupEntry.from_proto(proto, serialiser))

        return _Pipeline(pipeline, serialiser)
        

    def begin(self, context: _TaskContext):
        # 1. record all the continuations into a pipeline and save into state with caller id and address

        state = {
            'pipeline': self.to_proto(),
            'address': context.get_address(),
            'caller_id': context.get_caller_id(),
            'caller_address': context.get_caller_address()
        }

        context.set_state(state)

        # 2. get initial tasks(s) to call - might be single start of chain task or a group of tasks to call in parallel
        tasks = _get_initial_tasks(self._pipeline[0])

        # 3. call each task
        for task in tasks:

            task_id, task_type, args, kwargs, parameters = task.to_tuple()
            request = TaskRequest(id=task_id, type=task_type, parameters=self._serialiser.to_proto(parameters))
            self._serialiser.serialise_request(request, args, kwargs)

            context.pack_and_send(task.get_destination(), task_id, request)

    @staticmethod
    def _emit_result(context, task_request, task_result_or_exception):
        # the result of this task is the result of the pipeline
        key = 'task_result' if isinstance(task_result_or_exception, TaskResult) else 'task_exception'
        context.pack_and_save(key, task_result_or_exception)

        # either send a message to egress if reply_topic was specified
        if task_request.HasField('reply_topic'):
            context.pack_and_send_egress(topic=task_request.reply_topic, value=task_result_or_exception)

        # or call back to a particular flink function if reply_address was specified
        elif task_request.HasField('reply_address'):
            address, identifer = context.to_address_and_id(task_request.reply_address)
            context.pack_and_send(address, identifer, task_result_or_exception)

        # or call back to our caller (if there is one)
        else:
            state = context.get_state()
            caller_id = context.get_caller_id()

            if 'caller_id' in state and state['caller_id'] != caller_id:  # don't call back to self
                context.pack_and_send(state['caller_address'], state['caller_id'], task_result_or_exception)

    def resume(self, context: _TaskContext, task_result_or_exception: Union[TaskResult, TaskException]):
        caller_id = context.get_caller_id()
        state = context.get_state()

        # mark pipeline step as complete
        _mark_task_complete(caller_id, self._pipeline)

        # record the task result / exception - returns current map of task_id to task_result
        task_results =_save_task_result_or_exception(caller_id, state, task_result_or_exception)

        # save updated pipeline state
        context.update_state({'pipeline': self.to_proto()})

        # get the next step of the pipeline to run (if any)
        _, next_step, group = _get_next_step_in_pipeline(caller_id, self._pipeline)

        # if the group is complete the create the aggregate results as a list
        if group is not None and group.is_complete():
            task_result_or_exception = _aggregate_group_results(group, task_results, self._serialiser)

        # if we got an exception then the next step is the finally_task if there is one (or none otherwise)
        if isinstance(task_result_or_exception, TaskException):
            next_step = _try_get_finally_task(caller_id, self._pipeline)

        # turn next step into remainder of tasks to call
        if isinstance(next_step, _TaskEntry):
            remainder = [next_step]
            if next_step.is_finally:
                # record the result of the task prior to the finally task so we can return it once the finally task completes
                context.update_state({'result_before_finally': task_result_or_exception})
        elif isinstance(next_step, _GroupEntry):
            remainder = _get_initial_tasks(next_step)
        else:
            remainder = []

        # call next steps (if any)
        if any(remainder):

            for task in remainder:
                task_id, task_type, task_args, kwargs, parameters = task.to_tuple()

                if task.is_finally:
                    args, state = (), self._serialiser.from_proto(task_result_or_exception.state)
                else:
                    args, state = self._serialiser.deserialise_result(task_result_or_exception)

                # extend with any args passed to the task explicitly
                args = _extend_args(args, task_args)

                request = TaskRequest(id=task_id, type=task_type, parameters=self._serialiser.to_proto(parameters))
                self._serialiser.serialise_request(request, args, kwargs, state)
                
                context.pack_and_send(task.get_destination(), task_id, request)
        else:
            last_step = self._pipeline[-1]

            if last_step.is_complete():
                # if we are at the last step in the pipeline and it is complete then terminate and emit result
                self.terminate(context, task_result_or_exception)

            elif isinstance(task_result_or_exception, TaskException):
                if group is None or group.is_complete():
                    # else if have an exception then terminate but waiting for any parallel tasks in the group to complete first
                    self.terminate(context, task_result_or_exception)


    def terminate(self, context: _TaskContext, task_result_or_exception: Union[TaskResult, TaskException]):
        task_request = context.unpack('task_request', TaskRequest)

        result_before_finally = context.get_state().get('result_before_finally')
        if result_before_finally is not None and isinstance(task_result_or_exception, TaskResult):
            # finally ran successfully, so return the result of the previous task (rather than cleanup result from finally)
            task_result_or_exception = result_before_finally

        # set basic message properties
        task_result_or_exception.id = task_request.id
        task_result_or_exception.type = f'{task_request.type}.' + (
            'result' if isinstance(task_result_or_exception, TaskResult) else 'error')

        # pass back any state that we were given at the start of the pipeline
        task_result_or_exception.state.CopyFrom(task_request.state)

        # finally emit the result (to egress, destination address or caller address)
        self._emit_result(context, task_request, task_result_or_exception)


class PipelineBuilder(object):
    """
    Builder class for creating pipelines of Flink Tasks

    :param optional pipeline: list of initial pipeline entries e.g. from another builder
    """
    def __init__(self, pipeline: list = None):
        self._pipeline = [] if pipeline is None else pipeline
        self._builder_id = _gen_id()

    @property
    def id(self):
        """
        The ID of this pipeline
        """
        return self._builder_id

    def append_to(self, other: 'PipelineBuilder') -> 'PipelineBuilder':
        """
        Appends tasks from another pipeline builder into this one

        :param other: the other pipeline builder
        :return: the builder
        """
        other._pipeline.extend(self._pipeline)
        return self

    def append_group(self, pipelines: Iterable['PipelineBuilder']) -> 'PipelineBuilder':
        """
        Appends tasks from another pipeline builder into a new in_parallel group inside this one

        :param other: the other pipeline builder
        :return: the builder
        """
        group = _GroupEntry(_gen_id())

        for pipeline in pipelines:
            pipeline._add_to_group(group)

        self._pipeline.append(group)
        return self

    def _add_to_group(self, group: _GroupEntry):
        group.add_to_group(self._pipeline)

    @staticmethod
    def _unpack_single_tuple_args(args):
        # send a single argument by itself instead of wrapped inside a tuple
        if _is_tuple(args) and len(args) == 1:
            args = args[0]

        return args

    def send(self, fun, *args, **kwargs) -> 'PipelineBuilder':
        """
        Adds a task entry for the given Flink Task and arguments

        :param fun: the python function which should be decorated with @tasks.bind()
        :param args: the task args
        :param kwargs: the task kwargs
        :return: the builder
        """
        args = self._unpack_single_tuple_args(args)
        task_type, parameters = self._task_type_and_parameters_for(fun)
        self._pipeline.append(_TaskEntry(_gen_id(), task_type, args, kwargs, parameters=parameters))
        return self

    def set(self, retry_policy:RetryPolicy = None, **params) -> 'PipelineBuilder':
        """
        Sets task properties on the last entry added to the builder

        :param option retry_policy: the task retry policy to use
        :param params: any other parameters to the task
        :return: the builder
        """
        if retry_policy is not None:
            params['retry_policy'] = retry_policy.to_proto()

        if any(self._pipeline):
            entry = self._pipeline[-1]
            entry.set_parameters(params)

        return self

    def continue_with(self, continuation, *args, **kwargs) -> 'PipelineBuilder':
        """
        Adds continuation to the pipeline

        :param continuation: the python function which should be decorated with @tasks.bind()
        :param args: the task args
        :param kwargs: the task kwargs
        :return: the builder
        """
        if isinstance(continuation, PipelineBuilder):
            continuation.append_to(self)
        else:
            args = self._unpack_single_tuple_args(args)
            task_type, parameters = self._task_type_and_parameters_for(continuation)
            self._pipeline.append(_TaskEntry(_gen_id(), task_type, args, kwargs, parameters=parameters))
        return self

    def get_inital_destination(self):
        """
        Returns the initial destination of the pipeline - i.e. where the first task should be sent

        :return: the initial desintation (e.g. example/worker)
        """
        return None if not any(self._pipeline) else self._pipeline[0].get_destination()

    def to_task_request(self, serialiser) -> TaskRequest:
        """
        Serialises the pipeline as a TaskRequest with a task type of '__builtins.run_pipeline'

        :param serialiser: the serialiser to use such as DefaultSerialiser
        :return: TaskRequest protobuf message
        """
        task_id = self._builder_id
        task_type = '__builtins.run_pipeline'
        args = self.validate().to_proto(serialiser=serialiser)
        kwargs = {}
        parameters = {}    

        # send a single argument by itself instead of wrapped inside a tuple
        if _is_tuple(args) and len(args) == 1:
            args = args[0]

        task_request = TaskRequest(id=task_id, type=task_type, parameters=serialiser.to_proto(parameters))
        serialiser.serialise_request(task_request, args, kwargs)
        
        return task_request

    def finally_do(self, finally_action, *args, **kwargs) -> 'PipelineBuilder':
        """
        Adds finally to the pipeline

        :param continuation: the python function which should be decorated with @tasks.bind()
        :param args: the task args
        :param kwargs: the task kwargs
        :return: the builder
        """
        args = self._unpack_single_tuple_args(args)
        task_type, parameters = self._task_type_and_parameters_for(finally_action)
        task_entry = _TaskEntry(_gen_id(), task_type, args, kwargs, parameters=parameters, is_finally=True)
        task_entry.set_parameters({'is_fruitful': False})
        self._pipeline.append(task_entry)
        return self

    def to_pipeline(self, serialiser=None):
        """
        Concretises the builder into a pipeline

        :param option serialiser: the serialiser to use such as DefaultSerialiser
        :return: a Flink Tasks pipeline
        """
        self.validate()
        return _Pipeline(self._pipeline, serialiser=serialiser)

    def validate(self) -> 'PipelineBuilder':
        """
        Validates the pipeline

        :return: the builder
        """
        errors = []

        for entry in self._pipeline:
            entry.validate(errors)

        finally_tasks = [task for task in self._pipeline if isinstance(task, _TaskEntry) and task.is_finally]
        if len(finally_tasks) > 1:
            errors.append('Cannot have more than one "finally_do" method')
        if len(finally_tasks) == 1 and finally_tasks[0] != self._pipeline[-1]:
            errors.append('"finally_do" must be called at the end of a pipeline')
        
        if any(errors):
            error = ', '.join(errors)
            raise ValueError(f'Invalid pipeline: {error}')

        return self

    def to_proto(self, serialiser=None) -> Pipeline:
        """
        Serialises the pipeline to protobuf

        :param serialiser: the serialiser to use such as DefaultSerialiser
        :return: Pipeline protobuf message
        """
        return _Pipeline(self._pipeline, serialiser=serialiser).to_proto()

    @staticmethod
    def from_proto(pipeline_proto: Pipeline, serialiser) -> 'PipelineBuilder':
        """
        Deserialises the pipeline from protobuf

        :param pipeline_proto: the pipeline as protobuf
        :param serialiser: the serialiser to use such as DefaultSerialiser
        :return: Pipeline protobuf message
        """
        pipeline = []

        for proto in pipeline_proto.entries:
            if proto.HasField('task_entry'):
                pipeline.append(_TaskEntry.from_proto(proto, serialiser))
            elif proto.HasField('group_entry'):
                pipeline.append(_GroupEntry.from_proto(proto, serialiser))

        return PipelineBuilder(pipeline)

    @staticmethod
    def _task_type_and_parameters_for(fun):
        try:
            parameters = fun.defaults()
            module_name = parameters.get('module_name', None)
            task_type = _task_type_for(fun, module_name)

            return task_type, parameters
        except AttributeError:
            raise AttributeError(f'Function {fun.__module__}.{fun.__name__} should be decorated with tasks.bind')
