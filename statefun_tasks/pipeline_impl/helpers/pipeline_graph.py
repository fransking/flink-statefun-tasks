from statefun_tasks.types import Group, Task
from statefun_tasks.utils import _try_next, _try_peek
from statefun_tasks.messages_pb2 import TaskException
from collections import deque


class PipelineGraph(object):
    def __init__(self, pipeline: list):
        self._pipeline = pipeline

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

    def yield_tasks(self, pipeline=None):
        for task_or_group in pipeline or self._pipeline:
            if isinstance(task_or_group, Group):
                for group_entry in task_or_group:
                    yield from self.yield_tasks(group_entry)
            else:
                yield task_or_group

    def get_task(self, task_id):
        for task in self.yield_tasks():
            if task_id == task.task_id:
                return task

        raise ValueError(f'Task {task_id} not found')

    def is_empty(self):
        return not any(self.get_initial_tasks())
        
    def get_task_chain(self, task_id):
        
        def extract_task_chain(state, pipeline, entry):
            tasks = state

            if entry.task_id == task_id:
                matched = False

                for task in self.yield_tasks(pipeline):
                    if matched or task.task_id == task_id:
                        matched = True
                        tasks.append(task)

                return tasks, True # break

            return tasks, False # continue

        task_chain = []
        PipelineGraph.walk_pipeline(self._pipeline, extract_task_chain, task_chain)

        return task_chain

    def get_initial_tasks(self, group=None, slice=0):
        max_parallelism = 0 # unlimited

        stack = deque([group or self._pipeline[slice]])  # FIFO
        tasks = []

        while len(stack) > 0:
            entry = stack.popleft()

            if isinstance(entry, Group):

                if entry.max_parallelism is not None and entry.max_parallelism > 0:
                    max_parallelism = entry.max_parallelism if max_parallelism == 0 else min(max_parallelism, entry.max_parallelism)

                for pipeline_in_group in entry:
                    stack.append(pipeline_in_group[0])
            elif isinstance(entry, Task):
                tasks.append(entry)
            else:
                raise ValueError(f'Expected either a task or a group at the start of each pipeline')

        # deal with empty groups at the start of the pipeline by incrementing the slice
        if group is None and not any(tasks):
            if slice < len(self._pipeline) -1:
                return self.get_initial_tasks(slice=slice+1)

        return tasks, None if max_parallelism < 1 else max_parallelism, slice

    def get_last_task_in_chain(self, task_id):
        task_chain = self.get_task_chain(task_id)
        return task_chain[-1]

    def mark_task_complete(self, task_id, task_result_or_exception):
        failed = isinstance(task_result_or_exception, TaskException)

        tasks = self.get_task_chain(task_id)

        if failed:
            # mark this task complete and its children
            for task in tasks:
                task.mark_complete()
        elif any(tasks):
            tasks[0].mark_complete()
         
    def try_get_finally_task(self, task_id):
        finally_task = next((task for task in self._pipeline if isinstance(task, Task) and task.is_finally), None)
        return None if finally_task is None or finally_task.task_id == task_id else finally_task

    def is_finally_task(self, task_id):
        finally_task = next((task for task in self._pipeline if isinstance(task, Task) and task.is_finally), None)
        return finally_task is not None and finally_task.task_id == task_id

    def get_next_step_in_pipeline(self, task_id):
        stack = deque([(self._pipeline, None, None, None, None)])  # FIFO (pipeline, entry after group, group, parent_group, entry after parent group)

        while len(stack) > 0:
            pipeline, entry_after_group, group_entry, parent_group, entry_after_parent_group = stack.popleft()
            iterator = iter(pipeline)

            for entry in pipeline:
                _try_next(iterator)

                if isinstance(entry, Group):
                    stack_entry_after_group, iterator = _try_peek(iterator)
                    stack_group_entry = entry

                    # don't overwrite the parent if already recorded on stack
                    # we need the top most parent for tasks inside groups nested inside groups
                    stack_parent_group = parent_group or group_entry
                    stack_entry_after_parent_group = entry_after_group
                    
                    for pipeline_in_group in entry:
                        stack.append((pipeline_in_group, stack_entry_after_group, stack_group_entry, stack_parent_group, stack_entry_after_parent_group))

                else:
                    if entry.task_id == task_id:
                        next_entry = _try_next(iterator)

                        if next_entry is None and parent_group is not None and parent_group.is_complete():
                            # bubble up to the top most parent
                            return entry, entry_after_parent_group, parent_group, None
                        else:
                            # otherwise if there is no next task and the group containing this task is complete 
                            # bubble up to the task after the group
                            if next_entry is None and group_entry is not None and group_entry.is_complete() and next_entry is None:
                                next_entry = entry_after_group

                        # skip over empty groups
                        empty_group_entry = None
                        while isinstance(next_entry, Group) and not any(next_entry):
                            empty_group_entry = next_entry
                            next_entry = _try_next(iterator)
                            
                        return entry, next_entry, group_entry, empty_group_entry

        # entry, next_entry, group entry is part of, next_entry group skipped over because it was empty
        return None, None, None, None
