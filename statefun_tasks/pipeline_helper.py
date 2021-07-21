from statefun_tasks import pipeline_builder
from statefun_tasks.types import Group, Task
from statefun_tasks.utils import _try_next, _is_tuple, _gen_id
from statefun_tasks.protobuf import pack_any
from statefun_tasks.messages_pb2 import TaskResults, TaskResult, TaskException
from collections import ChainMap
from collections import deque


class _PipelineHelper(object):
    def __init__(self, pipeline: list, serialiser):
        self._pipeline = pipeline
        self._serialiser = serialiser

    @staticmethod
    def walk_pipeline(pipeline, func, state):
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

    @staticmethod
    def yield_tasks(pipeline):
        for task_or_group in pipeline:
            if isinstance(task_or_group, Group):
                for group_entry in task_or_group:
                    yield from _PipelineHelper.yield_tasks(group_entry)
            else:
                yield task_or_group

    @staticmethod
    def get_task_chain(pipeline, task_id):
        
        def extract_task_chain(state, pipeline, entry):
            tasks = state

            if entry.task_id == task_id:
                matched = False

                for task in _PipelineHelper.yield_tasks(pipeline):
                    if matched or task.task_id == task_id:
                        matched = True
                        tasks.append(task)

                return tasks, True # break

            return tasks, False # continue

        task_chain = []
        _PipelineHelper.walk_pipeline(pipeline, extract_task_chain, task_chain)

        return task_chain

    def get_initial_tasks(self, entry=None):
        stack = deque([entry or self._pipeline[0]])  # FIFO
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

    def mark_task_complete(self, task_id, task_result_or_exception):
        failed = isinstance(task_result_or_exception, TaskException)

        tasks = self.get_task_chain(self._pipeline, task_id)

        if failed:
            # mark this task complete and its children
            for task in tasks:
                task.mark_complete()
        else:
            tasks[0].mark_complete()
                        
    def add_to_task_results(self, task_id, task_result_or_exception, task_results):
        failed = isinstance(task_result_or_exception, TaskException)

        task_chain = self.get_task_chain(self._pipeline, task_id)
        last_task = task_chain[-1]
        
        if task_id == last_task.task_id:
            # if we are the last task in this chain in the group then record this result so we can aggregate laster
            task_results.by_id[last_task.task_id].CopyFrom(pack_any(task_result_or_exception))

        elif failed:
            # additionally propogate the error onto the last stage of this chain
            task_results.by_id[last_task.task_id].CopyFrom(pack_any(task_result_or_exception))

    def try_get_finally_task(self, task_id):
        finally_task = next((task for task in self._pipeline if isinstance(task, Task) and task.is_finally), None)
        return None if finally_task is None or finally_task.task_id == task_id else finally_task

    def get_next_step_in_pipeline(self, task_id):
        stack = deque([(self._pipeline, None, None, None)])  # FIFO (pipeline, entry after group, group, parent_group)

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


    def aggregate_group_results(self, group: Group, task_results: TaskResults):
        
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

                    task_result_or_exception = self._serialiser.from_proto(proto)

                    if isinstance(task_result_or_exception, TaskResult):
                        result, state = self._serialiser.deserialise_result(task_result_or_exception)
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
            self._serialiser.serialise_result(task_result, aggregated_results, aggregated_state)

            return task_result
