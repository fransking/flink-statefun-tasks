from ._types import Group, Task
from ._utils import _try_next, _is_tuple, _gen_id
from ._protobuf import _pack_any
from .messages_pb2 import TaskResult, TaskException, TaskResults
from typing import Union
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

def _get_task_chain(pipeline, parent_task_id):
    
    def extract_task_chain(state, pipeline, entry):
        tasks, matched = state

        if matched or entry.task_id == parent_task_id:
            matched = True
            tasks.append(entry)

        return (tasks, matched), False # continue

    task_chain = []
    _walk_pipeline(pipeline, extract_task_chain, (task_chain, False))

    return task_chain


def _mark_task_complete(task_id, pipeline, task_result_or_exception, task_results):
    packed_result_or_exception = _pack_any(task_result_or_exception)
    failed = isinstance(task_result_or_exception, TaskException)

    def mark_complete(state, pipeline, entry):
        if entry.task_id == task_id:
            tasks = _get_task_chain(pipeline, task_id)
            
            if failed:
                # mark this task complete and its children
                for task in tasks:
                    task.mark_complete()
                    task_results.by_id[task.task_id].CopyFrom(packed_result_or_exception)
            else:
                tasks[0].mark_complete()
                task_results.by_id[task_id].CopyFrom(packed_result_or_exception)

            return None, True # stop

        return None, False # continue

    _walk_pipeline(pipeline, mark_complete, None)  


def _try_get_finally_task(task_id, pipeline):
    finally_task = next((task for task in pipeline if isinstance(task, Task) and task.is_finally), None)
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

            if isinstance(entry, Group):
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
        task_result = TaskResult(id=group.group_id)
        serialiser.serialise_result(task_result, aggregated_results, aggregated_states)
        return task_result
