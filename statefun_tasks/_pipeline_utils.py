from ._types import _GroupEntry, _TaskEntry
from ._utils import _try_next, _is_tuple, _gen_id
from .messages_pb2 import GroupResults, TaskResult


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


def _try_get_finally_task(pipeline):
    return next((task for task in pipeline if isinstance(task, _TaskEntry) and task.is_finally), None)


def _get_next_step_in_pipeline(task_id, previous_task_failed: bool, pipeline):
    # figure out the next step for the pipeline
    # a group must be complete before you can move onto its continuation
    # groups are executed in parallel so a pipeline_in_group that is complete should jump
    # to the continuation of the group (if any) but only if the group as a whole is complete

    if previous_task_failed:
        # ignore the rest of the pipeline, but execute the "finally" task if it exists
        finally_task = _try_get_finally_task(pipeline)
        previous_task_was_finally = finally_task is not None and finally_task.task_id == task_id
        next_task = None if previous_task_was_finally else finally_task
        return None, next_task, None

    iterator = iter(pipeline)

    while True:
        try:
            entry = next(iterator)

            if isinstance(entry, _GroupEntry):
                for pipeline_in_group in entry:
                    current_t, next_t, _ = _get_next_step_in_pipeline(task_id, False, pipeline_in_group)

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


def _save_group_result(group: _GroupEntry, caller_id, state: dict, task_result: TaskResult):
    group_results = state.setdefault(group.group_id, GroupResults())
    group_results.results[caller_id].CopyFrom(task_result)
    return group_results


def _aggregate_group_results(group: _GroupEntry, group_results: GroupResults, serialiser):

    def aggregate_results(grp):
        aggregated_results = []
        aggregated_states = []

        for pipeline in grp:
            last_task = pipeline[-1]
            if isinstance(last_task, _GroupEntry):
                results, states = aggregate_results(last_task)
                aggregated_results.append(results)
                aggregated_states.append(states)
            else:
                group_result = group_results.results[pipeline[-1].task_id]
                result, state = serialiser.deserialise_result(group_result)

                aggregated_results.append(result)
                aggregated_states.append(state)
        
        return aggregated_results, aggregated_states

    aggregated_results, aggregated_states = aggregate_results(group)

    task_result = TaskResult(id=_gen_id())
    serialiser.serialise_result(task_result, aggregated_results, aggregated_states)

    return task_result
