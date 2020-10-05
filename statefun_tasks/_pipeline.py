from ._utils import _gen_id, _task_type_for, _try_next
from ._serialisation import deserialise, serialise, try_serialise_json_then_pickle
from ._types import _TaskEntry, _GroupEntry, _GroupResult
from ._context import _TaskContext
from .messages_pb2 import TaskRequest, TaskResult, TaskException

from datetime import timedelta
from google.protobuf.any_pb2 import Any
from typing import Union
import json


class _Pipeline(object):
    def __init__(self, fun=None, pipeline=None):
        self._fun = fun
        self._pipeline = []

        if pipeline is not None:
            self._pipeline.extend(pipeline)

    def append_to(self, other):
        other._pipeline.extend(self._pipeline)

    def add_to_group(self, group: _GroupEntry):
        group.add_to_group(self._pipeline)

    def to_json_dict(self, verbose=False):
        return [entry.to_json_dict(verbose) for entry in self._pipeline]

    def to_json(self, verbose=False):
        return [entry.to_json(verbose) for entry in self._pipeline]

    def send(self, *args, **kwargs):
        task_type = _task_type_for(self._fun)
        self._pipeline.append(_TaskEntry(_gen_id(), task_type, args, kwargs))
        return self

    def set(self, **kwargs):
        if any(self._pipeline):
            entry = self._pipeline[-1]
            entry.set_parameters(kwargs)

        return self

    def continue_with(self, continuation, *args, **kwargs):
        if isinstance(continuation, _Pipeline):
            continuation.append_to(self)
        else:
            task_type = _task_type_for(continuation)
            self._pipeline.append(_TaskEntry(_gen_id(), task_type, args, kwargs, parameters=continuation.defaults()))
        return self

    def resolve(self, context: _TaskContext, extra_args):
        # 1. record all the continuations into a pipeline and save into state with caller id and address
        state = {
            'pipeline': self._pipeline,
            'caller_id': context.get_caller_id(),
            'address': context.get_address()
        }

        context.set_state(state)

        # 2. get initial tasks(s) to call - might be single start of chain task or a group of tasks to call in parallel
        if isinstance(self._pipeline[0], _TaskEntry):
            tasks = [self._pipeline[0]]
        elif isinstance(self._pipeline[0], _GroupEntry):
            tasks = [t[0] for t in self._pipeline[0]]
        else:
            raise ValueError(f'Expected either a task or a group as the start of the pipeline')

        # 3. call each task
        for task in tasks:
            task_id, task_type, args, kwargs = task.to_tuple()
            # merge args and extra_args
            args = args + extra_args

            task_data = (args, kwargs)

            request = TaskRequest(id=task_id, type=task_type)
            serialise(request, task_data, content_type=task.get_parameter('content_type'))

            destination = self._get_destination(task)
            context.pack_and_send(destination, task_id, request)

    def _get_destination(self, task: _TaskEntry):
        # task destination of form 'namespace/worker_name'
        return f'{task.get_parameter("namespace")}/{task.get_parameter("worker_name")}'

    def _get_task_entry(self, task_id, pipeline=None):
        for entry in pipeline if pipeline is not None else self._pipeline:
            if isinstance(entry, _GroupEntry):
                for pipeline_in_group in entry:
                    task_entry = self._get_task_entry(task_id, pipeline_in_group)
                    if task_entry is not None:
                        return task_entry
            else:
                if entry.task_id == task_id:
                    return entry

    def _mark_complete(self, task_id):
        task_entry = self._get_task_entry(task_id)

        # defensive
        if task_entry is not None:
            task_entry.mark_complete()

    def _get_next_step(self, task_id, pipeline=None):
        # figure out the next step for the pipeline
        # a group must be complete before you can move onto its continuation
        # groups are executed in parallel so a pipeline_in_group that is complete should jump
        # to the continuation of the group (if any) but only if the group as a whole is complete
        
        iterator = iter(pipeline if pipeline is not None else self._pipeline)

        while True:
            try:
                entry = next(iterator)

                if isinstance(entry, _GroupEntry):
                    for pipeline_in_group in entry:
                        current_t, next_t, _ = self._get_next_step(task_id, pipeline_in_group)

                        if current_t is not None:

                            if next_t is None: 
                                # at the end of pipeline_in_group so roll onto the next task but only if the group is complete
                                next_t = _try_next(iterator) if entry.is_complete() else None

                            return current_t, next_t, entry
                else:
                    if entry.task_id == task_id:
                        next_entry = _try_next(iterator)
                        return entry, next_entry, None

            except StopIteration:
                return None, None, None
                
    def _save_group_result(self, group: _GroupEntry, caller_id, state: dict, task_result: TaskResult):
        group_results = state.setdefault(group.group_id, {})
        group_results[caller_id] = _GroupResult(task_result.data, task_result.content_type)
        return group_results


    def _aggregate_group_results(self, group: _GroupEntry, group_results: dict):
        aggregated_results = []
        for pipeline in group:
            group_result = group_results[pipeline[-1].task_id]
            aggregated_results.append(deserialise(group_result))

        # ensure we send a tuple
        aggregated_results = (aggregated_results,)

        task_result = TaskResult(id=_gen_id())
        try_serialise_json_then_pickle(task_result, aggregated_results)

        return task_result

    def resume(self, context: _TaskContext, task_result: TaskResult):
        task_id = context.get_task_id()
        caller_id = context.get_caller_id()
        state = context.get_state()

        # mark pipeline step as complete
        self._mark_complete(caller_id)

        # save updated pipeline state
        context.update_state({'pipeline': self._pipeline})

        # get the next step of the pipeline to run (if any)
        _, next_step, group = self._get_next_step(caller_id)

        # need to aggregate task results into group state
        if group is not None:
            # save each result from the pipeline
            group_results = self._save_group_result(group, caller_id, state, task_result)

            # if the group is complete the create the aggregate results as a list
            if group.is_complete():
                task_result = self._aggregate_group_results(group, group_results)

        if isinstance(next_step, _TaskEntry):
            remainder = [next_step]
        elif isinstance(next_step, _GroupEntry):
            remainder = [t[0] for t in next_step]
        else:
            remainder = []

        # call next steps (if any)
        if any(remainder):

            for task in remainder:
                task_id, task_type, _, kwargs = task.to_tuple()

                args = deserialise(task_result)
                task_data = (args, kwargs)

                request = TaskRequest(id=task_id, type=task_type)

                # honour the task_result content_type - pickled data might not be json serialisable
                serialise(request, task_data, content_type=task_result.content_type)

                destination = self._get_destination(task)
                context.pack_and_send(destination, task_id, request)
        else:
            # if we are at the last step in the pipeline and it is complete then terminate and emit result
            last_step = self._pipeline[-1]

            if last_step.is_complete():
                self.terminate(context, task_result)

    def attempt_retry(self, context: _TaskContext, task_exception: TaskException):
        task_id = context.get_caller_id()
        task_entry = self._get_task_entry(task_id)

        # defensive
        if task_entry is None:
            return False

        retries = context.get_state().get('retries', {})
        retry_count = retries.get(task_id, 1)
        retry_policy = task_entry.get_parameter('retry_policy')

        if retry_policy is None:
            return False

        if retry_count > retry_policy.max_retries:
            return False

        delay = retry_policy.delay

        if retry_policy.exponential_back_off:
            delay_ms = int(delay.total_seconds() * 1000.0)
            delay = timedelta(milliseconds=delay_ms^retry_count)

        request = task_exception.original_request
        destination = context.get_caller_address()
        context.pack_and_send_after(delay, destination, task_id, request)

        retries[task_id] = retry_count + 1
        context.update_state({'retries': retries})

        return True

    def terminate(self, context: _TaskContext, task_result_or_exception: Union[TaskResult, TaskException]):
        task_request = context.unpack('task_request', TaskRequest)
        reply_topic = task_request.reply_topic            
        task_result_or_exception.correlation_id = task_request.id
        task_result_or_exception.type = f'{task_request.type}.' + ('result' if isinstance(task_result_or_exception, TaskResult) else 'error')

        if reply_topic is not None and reply_topic != "":
            any = Any()
            any.Pack(task_result_or_exception)
            context.pack_and_send_egress(topic=reply_topic, value=task_result_or_exception)
        else:
            state = context.get_state()
            caller_id = context.get_caller_id()

            if 'caller_id' in state and state['caller_id'] != caller_id:
                context.pack_and_send(state['address'], state['caller_id'], task_result_or_exception)
