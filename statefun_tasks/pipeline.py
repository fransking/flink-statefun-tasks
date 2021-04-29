from statefun_tasks.serialisation import DefaultSerialiser
from statefun_tasks.messages_pb2 import TaskRequest, TaskResult, TaskResults, TaskException, TaskEntry, Pipeline
from statefun_tasks.protobuf import pack_any, unpack_any
from statefun_tasks.context import TaskContext
from statefun_tasks.types import Task, Group, RetryPolicy
from statefun_tasks.utils import _extend_args
from statefun_tasks.pipeline_helper import _PipelineHelper

from typing import Union, Iterable


class _Pipeline(object):
    def __init__(self, pipeline: list, serialiser=None):
        self._pipeline = pipeline
        self._serialiser = serialiser if serialiser is not None else DefaultSerialiser()
        self._pipeline_helper = _PipelineHelper(self._pipeline, self._serialiser)

    def to_proto(self) -> Pipeline:
        pipeline = Pipeline(entries=[p.to_proto(self._serialiser) for p in self._pipeline])
        return pipeline

    @staticmethod
    def from_proto(pipeline_proto: Pipeline, serialiser):
        pipeline = []

        for proto in pipeline_proto.entries:
            if proto.HasField('task_entry'):
                pipeline.append(Task.from_proto(proto, serialiser))
            elif proto.HasField('group_entry'):
                pipeline.append(Group.from_proto(proto, serialiser))

        return _Pipeline(pipeline, serialiser)
        
    def begin(self, context: TaskContext):
        # 1. record all the continuations into a pipeline and save into state with caller id and address

        state = {
            'pipeline_id': context.get_task_id(),
            'pipeline': self.to_proto(),
            'address': context.get_address(),
            'caller_id': context.get_caller_id(),
            'caller_address': context.get_caller_address()
        }

        context.set_state(state)

        # 2. get initial tasks(s) to call - might be single start of chain task or a group of tasks to call in parallel
        tasks = self._pipeline_helper.get_initial_tasks()

        # 3. call each task
        for task in tasks:

            task_id, task_type, args, kwargs, parameters = task.to_tuple()

            # set extra pipeline related parameters
            self._add_initial_pipeline_meta(context, parameters)

            request = TaskRequest(id=task_id, type=task_type, parameters=self._serialiser.to_proto(parameters))
            self._serialiser.serialise_request(request, args, kwargs)

            context.send_message(task.get_destination(), task_id, request)

    def resume(self, context: TaskContext, task_result_or_exception: Union[TaskResult, TaskException]):
        caller_id = context.get_caller_id()
        state = context.get_state()

        # mark pipeline step as complete
        self._pipeline_helper.mark_task_complete(caller_id)

        # record the task result / exception - returns current map of task_id to task_result
        task_results = state.setdefault('task_results', TaskResults())
        task_results.by_id[caller_id].CopyFrom(pack_any(task_result_or_exception))
        
        # save updated pipeline state
        context.update_state({'pipeline': self.to_proto()})

        # get the next step of the pipeline to run (if any)
        _, next_step, group = self._pipeline_helper.get_next_step_in_pipeline(caller_id)

        # if the group is complete the create the aggregate results as a list
        if group is not None and group.is_complete():
            task_result_or_exception = self._pipeline_helper.aggregate_group_results(group, task_results)

        # if we got an exception then the next step is the finally_task if there is one (or none otherwise)
        if isinstance(task_result_or_exception, TaskException):
            next_step = self._pipeline_helper.try_get_finally_task(caller_id)

        # turn next step into remainder of tasks to call
        if isinstance(next_step, Task):
            remainder = [next_step]
            if next_step.is_finally:
                # record the result of the task prior to the finally task so we can return it once the finally task completes
                context.update_state({'result_before_finally': task_result_or_exception})
        elif isinstance(next_step, Group):
            remainder = self._pipeline_helper.get_initial_tasks(next_step)
        else:
            remainder = []

        # call next steps (if any)
        if any(remainder):
            args, task_state = self._serialiser.deserialise_response(task_result_or_exception)

            for task in remainder:
                task_id, task_type, task_args, kwargs, parameters = task.to_tuple()

                # set extra pipeline related parameters
                self._add_pipeline_meta(context, state, caller_id, parameters)

                # extend with any args passed to the task explicitly
                # noting that args from previous tasks are not passed to finally 
                args = _extend_args(() if task.is_finally else args, task_args)

                request = TaskRequest(id=task_id, type=task_type, parameters=self._serialiser.to_proto(parameters))
                self._serialiser.serialise_request(request, args, kwargs, task_state)
                
                context.send_message(task.get_destination(), task_id, request)
        else:
            last_step = self._pipeline[-1]

            if last_step.is_complete():
                # if we are at the last step in the pipeline and it is complete then terminate and emit result
                self.terminate(context, task_result_or_exception)

            elif isinstance(task_result_or_exception, TaskException):
                if group is None or group.is_complete():
                    # else if have an exception then terminate but waiting for any parallel tasks in the group to complete first
                    self.terminate(context, task_result_or_exception)


    def terminate(self, context: TaskContext, task_result_or_exception: Union[TaskResult, TaskException]):
        task_request = context.storage.task_request or TaskRequest()

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

    @staticmethod
    def _emit_result(context, task_request, task_result_or_exception):
        # the result of this task is the result of the pipeline
        if isinstance(task_result_or_exception, TaskResult):
            context.storage.task_result = task_result_or_exception
        else:
            context.storage.task_exception = task_result_or_exception

        # either send a message to egress if reply_topic was specified
        if task_request.HasField('reply_topic'):
            context.send_egress_message(topic=task_request.reply_topic, value=task_result_or_exception)

        # or call back to a particular flink function if reply_address was specified
        elif task_request.HasField('reply_address'):
            address, identifer = context.to_address_and_id(task_request.reply_address)
            context.send_message(address, identifer, task_result_or_exception)

        # or call back to our caller (if there is one)
        else:
            state = context.get_state()
            caller_id = context.get_caller_id()

            if 'caller_id' in state and state['caller_id'] != caller_id:  # don't call back to self
                context.send_message(state['caller_address'], state['caller_id'], task_result_or_exception)

    @staticmethod
    def _add_initial_pipeline_meta(context, parameters):
        parameters['pipeline_address'] = context.get_address()
        parameters['pipeline_id'] = context.get_task_id()
    
    @staticmethod
    def _add_pipeline_meta(context, state, caller_id, parameters):
        parameters['pipeline_address'] = state.get('pipeline_address', None)
        parameters['pipeline_id'] = state.get('pipeline_id', None)

        if caller_id is not None:
            parameters['parent_task_address'] = context.get_caller_address()
            parameters['parent_task_id'] = caller_id
