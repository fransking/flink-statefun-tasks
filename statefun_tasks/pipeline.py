from typing import Union

from statefun_tasks.context import TaskContext
from statefun_tasks.messages_pb2 import TaskRequest, TaskResult, TaskException, Pipeline, TupleOfAny
from statefun_tasks.pipeline_helper import _PipelineHelper
from statefun_tasks.serialisation import DefaultSerialiser
from statefun_tasks.types import Task, Group


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
                pipeline.append(Task.from_proto(proto))
            elif proto.HasField('group_entry'):
                pipeline.append(Group.from_proto(proto))

        return _Pipeline(pipeline, serialiser)

    def begin(self, context: TaskContext):
        caller_id = context.get_caller_id()

        # 1. record all the continuations into a pipeline and save into state with caller id and address
        context.pipeline_state.address = context.get_address()
        context.pipeline_state.pipeline.CopyFrom(self.to_proto())

        if context.get_caller_id() is not None:
            context.pipeline_state.caller_id = context.get_caller_id()
            context.pipeline_state.caller_address = context.get_caller_address()

        # 2. get initial tasks(s) to call - might be single start of chain task or a group of tasks to call in parallel
        tasks = self._pipeline_helper.get_initial_tasks()

        # 3. call each task
        for task in tasks:
            request = TaskRequest(id=task.task_id, type=task.task_type)

            # set extra pipeline related parameters
            self._add_pipeline_meta(context, caller_id, request)

            self._serialiser.serialise_request(request, task.request, retry_policy=task.retry_policy)

            context.send_message(task.get_destination(), task.task_id, request)

    def resume(self, context: TaskContext, task_result_or_exception: Union[TaskResult, TaskException]):
        caller_id = context.get_caller_id()
        task_results = context.pipeline_state.task_results

        # mark pipeline step as complete & record task result
        self._pipeline_helper.mark_task_complete(caller_id, task_result_or_exception, task_results)

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

                before_finally = context.pipeline_state.exception_before_finally \
                    if isinstance(task_result_or_exception, TaskException) \
                    else context.pipeline_state.result_before_finally

                before_finally.CopyFrom(task_result_or_exception)

        elif isinstance(next_step, Group):
            remainder = self._pipeline_helper.get_initial_tasks(next_step)
        else:
            remainder = []

        # call next steps (if any)
        if any(remainder):
            task_result, task_state = self._serialiser.unpack_response(task_result_or_exception)

            for task in remainder:
                task_id = task.task_id
                task_args_and_kwargs = self._serialiser.to_args_and_kwargs(task.request)
                
                # extend task_result with any args & kwargs passed to the task explicitly
                # noting that args from previous tasks are not passed to finally
                task_request = self._serialiser.merge_args_and_kwargs(task_result if not task.is_finally else TupleOfAny(), task_args_and_kwargs)

                request = TaskRequest(id=task_id, type=task.task_type)

                # set extra pipeline related parameters
                self._add_pipeline_meta(context, caller_id, request)

                self._serialiser.serialise_request(request, task_request, state=task_state, retry_policy=task.retry_policy)

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

        # if result_before_finally or exception_before_finally are set then we are in a finally block
        if context.pipeline_state.HasField('result_before_finally'):
            result_before_finally = context.pipeline_state.result_before_finally
        elif context.pipeline_state.HasField('exception_before_finally'):
            result_before_finally = context.pipeline_state.exception_before_finally
        else:
            result_before_finally = None

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
        elif context.pipeline_state.caller_id is not None:
            if context.pipeline_state.caller_id != context.get_caller_id():  # don't call back to self
                context.send_message(context.pipeline_state.caller_address, context.pipeline_state.caller_id,
                                     task_result_or_exception)

    @staticmethod
    def _add_pipeline_meta(context, caller_id, task_request: TaskRequest):
        task_request.meta['pipeline_address'] = context.pipeline_state.address
        task_request.meta['pipeline_id'] = context.pipeline_state.pipeline_id

        if caller_id is not None:
            task_request.meta['parent_task_address'] = context.get_caller_address()
            task_request.meta['parent_task_id'] = caller_id
