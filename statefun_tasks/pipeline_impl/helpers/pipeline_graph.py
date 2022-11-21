from statefun_tasks.types import Group, Task
from statefun_tasks.utils import _try_next, _try_peek
from statefun_tasks.messages_pb2 import TaskException
from collections import deque


class PipelineGraph(object):
    __slots__ = ('_pipeline')

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

    def is_top_level_group(self, group):
        for task_or_group in self._pipeline:
            if isinstance(task_or_group, Group) and task_or_group.group_id == group.group_id:
                return True
        return False

    def get_task(self, task_id):
        for task in self.yield_tasks():
            if task_id == task.uid:
                return task

        raise ValueError(f'Task {task_id} not found')

    def is_empty(self):
        tasks, _ ,_  = self.get_initial_tasks()
        return not any(tasks)
        
    def get_task_chain(self, task_id):
        
        def extract_task_chain(state, pipeline, entry):
            tasks = state

            if entry.uid == task_id:
                matched = False

                for task in self.yield_tasks(pipeline):
                    if matched or task.uid == task_id:
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

    def get_last_tasks_in_chain(self, task_id):
        """
        Returns the last tasks in the chain that this task_id is part of
        
        Examples:
        
        a -> [a, b, c] => [c]
        b -> [a, b, c] => [c]

        Exceptionally tasks are included but only if they form the remainder of the chain

        Examples:
        b -> [a, b, c, ex(d)] => [c, d]
        b -> [a, b, c, ex(d), ex(e)] => [c, d, e]
        b -> [a, b, c, ex(d), ex(e), f] => [f]
        """
        task_chain = self.get_task_chain(task_id)

        tasks = []

        for task in reversed(task_chain):
            tasks.insert(0, task)

            if not task.is_exceptionally:
                break
            
        return tasks

    def mark_task_complete(self, task_id, task_result_or_exception, remark=False):
        failed = isinstance(task_result_or_exception, TaskException)

        tasks = self.get_task_chain(task_id)

        if remark:
            for task in tasks:
                task.mark_complete(False)

        if any(tasks):
            if tasks[0].is_complete():
                return False  # task already complete - ignore

            tasks[0].mark_complete()

            if failed:
                # mark mark subsequent tasks complete until we reach the first exceptionally continuation
                # this is where we will continue from
                for task in tasks:
                    if task.is_exceptionally:
                        break  

                    elif not task.is_finally:
                        task.mark_complete()
            else:
                # mark subsequent exceptionally tasks complete until we reach the first non-exceptionally continuation
                # this is where we will continue from
                for task in tasks[1:]:
                    if not task.is_exceptionally:
                        break

                    task.mark_complete()

        return True
   
    def try_get_finally_task(self, task_id=None):
        finally_task = next((task for task in self._pipeline if isinstance(task, Task) and task.is_finally), None)
        return None if finally_task is None or finally_task.uid == task_id else finally_task

    def is_finally_task(self, task_id):
        finally_task = next((task for task in self._pipeline if isinstance(task, Task) and task.is_finally), None)
        return finally_task is not None and finally_task.uid == task_id

    def get_next_step_in_pipeline(self, task_id, task_result_or_exception):
        current_step, next_step, group, empty_group = self._get_next_step_in_pipeline(task_id)
        
        skipped_tasks = []

        if isinstance(task_result_or_exception, TaskException):
            # if we have a TaskException as input then we need to skip forward to the next exceptionally
            # task if we have one or otherwise to the finally task if we have one
            task = next_step

            while task is not None and isinstance(task, Task) and not task.is_exceptionally:
                skipped_tasks.append(task)
                _, task, _, _ = self._get_next_step_in_pipeline(task.uid)

            next_step = task or next_step

            if not (isinstance(next_step, Task) and next_step.is_exceptionally):
                next_step = None

                # if the task that failed is part of a group then wait for the group to complete before triggering the finally
                if group is not None:
                    if group.is_complete():
                        next_step = self.try_get_finally_task(task_id)

                else:
                    next_step = self.try_get_finally_task(task_id)

        else:
            # otherwise we have a TaskResult and so we need to skip over exceptionally tasks
            while next_step is not None and isinstance(next_step, Task) and next_step.is_exceptionally:
                next_step.mark_complete()
                skipped_tasks.append(next_step)

                _, next_step, _, _ = self._get_next_step_in_pipeline(next_step.uid)

        return current_step, next_step, group, empty_group, skipped_tasks

    def _get_next_step_in_pipeline(self, task_id):
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
                    if entry.uid == task_id:
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
