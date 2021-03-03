from ._types import _GroupEntry, _TaskEntry
from ._utils import _try_next, _is_tuple, _gen_id
from ._protobuf import _pack_any
from .messages_pb2 import TaskResult, TaskException, TaskResults
from typing import Union


def _get_initial_tasks(entry):
    tasks = []
    if isinstance(entry, _GroupEntry):
        for t in entry:
            tasks.extend(_get_initial_tasks(t[0]))
    elif isinstance(entry, _TaskEntry):
        tasks.append(entry)
    else:
        raise ValueError(f'Expected either a task or a group at the start of each pipeline')

    return tasks

def _get_task_entry(task_id, pipeline):
    for entry in pipeline:
        if isinstance(entry, _GroupEntry):
            for pipeline_in_group in entry:
                task_entry = _get_task_entry(task_id, pipeline_in_group)
                if task_entry is not None:
                    return task_entry
        else:
            if entry.task_id == task_id:
                return entry

def _mark_task_complete(task_id, pipeline):
    task_entry = _get_task_entry(task_id, pipeline)

    # defensive
    if task_entry is not None:
        task_entry.mark_complete()


def _try_get_finally_task(task_id, pipeline):
    finally_task = next((task for task in pipeline if isinstance(task, _TaskEntry) and task.is_finally), None)
    return None if finally_task is None or finally_task.task_id == task_id else finally_task


def _get_next_step_in_pipeline(task_id, pipeline):
    # figure out the next step for the pipeline
    # a group must be complete before you can move onto its continuation
    # groups are executed in parallel so a pipeline_in_group that is complete should jump
    # to the continuation of the group (if any) but only if the group as a whole is complete

    iterator = iter(pipeline)

    while True:
        try:
            entry = next(iterator)

            if isinstance(entry, _GroupEntry):
                for pipeline_in_group in entry:
                    current_t, next_t, _ = _get_next_step_in_pipeline(task_id, pipeline_in_group)

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


def _extend_args(args, task_args):
    if not any(task_args):
        return args

    if not _is_tuple(args):
        args = (args,)

    return args + task_args


def _save_task_result_or_exception(task_id, state: dict, task_result_or_exception: Union[TaskResult, TaskException]) -> TaskResults:
    task_results = state.setdefault('task_results', TaskResults())
    task_results.by_id[task_id].CopyFrom(_pack_any(task_result_or_exception))
    return task_results

def _save_group_result(group: _GroupEntry, caller_id, state: dict, task_result: TaskResult):
    group_results = state.setdefault(group.group_id, GroupResults())
    group_results.results[caller_id].CopyFrom(task_result)
    return group_results


def _aggregate_group_results(group: _GroupEntry, task_results: TaskResults, serialiser):

    def aggregate_results(grp):
        aggregated_results = []
        aggregated_states = []
        aggregated_errors = []

        for pipeline in grp:
            last_task = pipeline[-1]
            if isinstance(last_task, _GroupEntry):
                results, states, errors = aggregate_results(last_task)
                aggregated_results.append(results)
                aggregated_states.append(states)
                aggregated_states.append(errors)
            else:
                result, state, error = None, {}, None

                proto = task_results.by_id[pipeline[-1].task_id]  # Any
                task_result_or_exception = serialiser.from_proto(proto)

                if isinstance(task_result_or_exception, TaskResult):
                    result, state = serialiser.deserialise_result(task_result_or_exception)
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
        serialiser.serialise_result(task_result, aggregated_results, aggregated_states)
        return task_result
