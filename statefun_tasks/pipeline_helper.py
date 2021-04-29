from statefun_tasks.types import Group, Task
from statefun_tasks.utils import _try_next, _is_tuple, _gen_id
from statefun_tasks.messages_pb2 import TaskResults, TaskResult, TaskException

from typing import Union


class _PipelineHelper(object):
    def __init__(self, pipeline: list, serialiser):
        self._pipeline = pipeline
        self._serialiser = serialiser

    def get_initial_tasks(self, entry=None):
        entry = entry or self._pipeline[0]

        tasks = []
        if isinstance(entry, Group):
            for t in entry:
                tasks.extend(self.get_initial_tasks(t[0]))
        elif isinstance(entry, Task):
            tasks.append(entry)
        else:
            raise ValueError(f'Expected either a task or a group at the start of each pipeline')

        return tasks

    def get_task_entry(self, task_id, pipeline=None):
        pipeline = pipeline or self._pipeline

        for entry in pipeline:
            if isinstance(entry, Group):
                for pipeline_in_group in entry:
                    task_entry = self.get_task_entry(task_id, pipeline_in_group)
                    if task_entry is not None:
                        return task_entry
            else:
                if entry.task_id == task_id:
                    return entry

    def mark_task_complete(self, task_id):
        task_entry = self.get_task_entry(task_id)

        # defensive
        if task_entry is not None:
            task_entry.mark_complete()

    def try_get_finally_task(self, task_id):
        finally_task = next((task for task in self._pipeline if isinstance(task, Task) and task.is_finally), None)
        return None if finally_task is None or finally_task.task_id == task_id else finally_task

    def get_next_step_in_pipeline(self, task_id, pipeline=None):
        # figure out the next step for the pipeline
        # a group must be complete before you can move onto its continuation
        # groups are executed in parallel so a pipeline_in_group that is complete should jump
        # to the continuation of the group (if any) but only if the group as a whole is complete

        pipeline = pipeline or self._pipeline
        iterator = iter(pipeline)

        while True:
            try:
                entry = next(iterator)

                if isinstance(entry, Group):
                    for pipeline_in_group in entry:
                        current_t, next_t, _ = self.get_next_step_in_pipeline(task_id, pipeline_in_group)

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

    def aggregate_group_results(self, group: Group, task_results: TaskResults):

        def aggregate_results(grp):
            aggregated_results = []
            aggregated_states = []
            aggregated_errors = []

            for pipeline in grp:
                last_task = pipeline[-1]
                if isinstance(last_task, Group):
                    results, states, errors = aggregate_results(last_task)
                    aggregated_results.append(results)
                    aggregated_states.append(states)
                    aggregated_states.append(errors)
                else:
                    result, state, error = None, {}, None

                    proto = task_results.by_id[pipeline[-1].task_id]  # Any
                    task_result_or_exception = self._serialiser.from_proto(proto)

                    if isinstance(task_result_or_exception, TaskResult):
                        result, state = self._serialiser.deserialise_result(task_result_or_exception)
                    elif isinstance(task_result_or_exception, TaskException):
                        error = task_result_or_exception

                    aggregated_results.append(result)
                    aggregated_states.append(state)
                    aggregated_errors.append(error)
            
            return aggregated_results, aggregated_states, aggregated_errors

        aggregated_results, aggregated_states, aggregated_errors = aggregate_results(group)
        aggregated_errors = [error for error in aggregated_errors if error is not None]

        if any(aggregated_errors):
            task_exception = TaskException(            
                id=group.group_id,
                type=f'__aggregate.error',
                exception_type='statefun_tasks.AggregatedError',
                exception_message='|'.join([f'{e.id}, {e.type}, {e.exception_message}' for e in aggregated_errors]),
                stacktrace='|'.join([f'{e.id}, {e.stacktrace}' for e in aggregated_errors]))
            return task_exception
            
        else:
            task_result = TaskResult(id=group.group_id)
            self._serialiser.serialise_result(task_result, aggregated_results, aggregated_states)
            return task_result
