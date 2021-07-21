from ._types import Group, Task
from ._utils import _try_next, _is_tuple, _gen_id
from ._protobuf import _pack_any
from .messages_pb2 import TaskResult, TaskException, TaskResults
from typing import Union
from collections import ChainMap
from collections import deque


def _walk_pipeline(pipeline, func, state):
    stack = deque([pipeline])  # FIFO

    while len(stack) > 0:
        pipeline = stack.popleft()
        
        for entry in pipeline:
            if isinstance(entry, Group):
                for pipeline_in_group in entry:
                    stack.append(pipeline_in_group)
            else:
                state, end = func(state, pipeline, entry)

                if end:
                    return


def _get_initial_tasks(entry):
    stack = deque([entry])  # FIFO
    tasks = []

    while len(stack) > 0:
        entry = stack.popleft()

        if isinstance(entry, Group):
            for pipeline_in_group in entry:
                stack.append(pipeline_in_group[0])
        elif isinstance(entry, Task):
            tasks.append(entry)
        else:
            raise ValueError(f'Expected either a task or a group at the start of each pipeline')

    return tasks


def _yield_tasks(pipeline):
    for task_or_group in pipeline:
        if isinstance(task_or_group, Group):
            for group_entry in task_or_group:
                yield from _yield_tasks(group_entry)
        else:
            yield task_or_group


def _get_task_chain(pipeline, task_id):

    def extract_task_chain(state, pipeline, entry):
        tasks = state

        if entry.task_id == task_id:
            matched = False

            for task in _yield_tasks(pipeline):
                if matched or task.task_id == task_id:
                    matched = True
                    tasks.append(task)

            return tasks, True # break

        return tasks, False # continue

    task_chain = []
    _walk_pipeline(pipeline, extract_task_chain, task_chain)

    return task_chain


def _mark_task_complete(task_id, pipeline, task_result_or_exception):
    failed = isinstance(task_result_or_exception, TaskException)

    tasks = _get_task_chain(pipeline, task_id)
    
    if failed:
        # mark this task complete and its children
        for task in tasks:
            task.mark_complete()
    else:
        tasks[0].mark_complete()


def _add_to_task_results(task_id, pipeline, task_result_or_exception, task_results):
    failed = isinstance(task_result_or_exception, TaskException)

    task_chain = _get_task_chain(pipeline, task_id)
    last_task = task_chain[-1]
    
    if task_id == last_task.task_id:
        # if we are the last task in this chain in the group then record this result so we can aggregate laster
        task_results.by_id[last_task.task_id].CopyFrom(_pack_any(task_result_or_exception))

    elif failed:
        # additionally propogate the error onto the last stage of this chain
        task_results.by_id[last_task.task_id].CopyFrom(_pack_any(task_result_or_exception))


def _try_get_finally_task(task_id, pipeline):
    finally_task = next((task for task in pipeline if isinstance(task, Task) and task.is_finally), None)
    return None if finally_task is None or finally_task.task_id == task_id else finally_task


def _get_next_step_in_pipeline(task_id, pipeline):
    stack = deque([(pipeline, None, None, None)])  # FIFO (pipeline, entry after group, group, parent_group)

    while len(stack) > 0:
        pipeline, entry_after_group, group_entry, parent_group = stack.popleft()
        iterator = iter(pipeline)

        for entry in pipeline:
            _try_next(iterator)

            if isinstance(entry, Group):
                stack_entry_after_group = _try_next(iterator)
                stack_group_entry = entry

                # don't overwrite the parent if already recorded on stack
                # we need the top most parent for tasks inside groups nested inside groups
                parent_group = parent_group or group_entry  
                
                for pipeline_in_group in entry:
                    stack.append((pipeline_in_group, stack_entry_after_group, stack_group_entry, parent_group))

            else:
                if entry.task_id == task_id:
                    next_entry = _try_next(iterator)

                    if next_entry is None and parent_group is not None and parent_group.is_complete():
                        # bubble up to the top most parent
                        return entry, entry_after_group, parent_group
                    else:
                        # otherwise if there is no next task and the group containing this task is complete 
                        # bubble up to the task after the group
                        if group_entry is not None and group_entry.is_complete() and next_entry is None:
                            next_entry = entry_after_group

                    return entry, next_entry, group_entry

    return None, None, None


def _extend_args(args, task_args):
    if not any(task_args):
        return args

    if not _is_tuple(args):
        args = (args,)

    return args + task_args

def _aggregate_group_results(group: Group, task_results: TaskResults, serialiser):    
    aggregated_results, aggregated_states, aggregated_errors = [], [], []
    stack = deque([(group, aggregated_results, aggregated_states)])  # FIFO, errors are flat not nested so don't get stacked

    while len(stack) > 0:
        group, results, states = stack.popleft()
        
        for pipeline in group:
            last_entry = pipeline[-1]

            if isinstance(last_entry, Group):
                stack_results, stack_states = [], []

                results.append(stack_results)
                states.append(stack_states)

                stack.append((last_entry, stack_results, stack_states))
            else:
                result, state, error = None, {}, None

                proto = task_results.by_id[pipeline[-1].task_id]  # Any
                del task_results.by_id[pipeline[-1].task_id]  # We don't need the individual task results anymore so remove to save space / reduce message size

                task_result_or_exception = serialiser.from_proto(proto)

                if isinstance(task_result_or_exception, TaskResult):
                    result, state = serialiser.deserialise_result(task_result_or_exception)
                elif isinstance(task_result_or_exception, TaskException):
                    error = task_result_or_exception

                results.append(result)
                states.append(state)
                aggregated_errors.append(error)  # errors are flat not nested so don't get stacked

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

        # if the group tasks all return dictionaries then attempt to flatten the state into a single dictionary
        if all([isinstance(state, dict) for state in aggregated_states]):
            aggregated_state = dict(ChainMap(*aggregated_states))
        else: 
            # otherwise return state of the first task in group as we only support state mutation in groups if the state is a mergable dictionary
            aggregated_state = aggregated_states[0]

        task_result = TaskResult(id=group.group_id)
        serialiser.serialise_result(task_result, aggregated_results, aggregated_state)
        return task_result
