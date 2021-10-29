from statefun_tasks.types import Group, Task, TaskCancelledException
from statefun_tasks.utils import _try_next, _gen_id, _try_peek
from statefun_tasks.protobuf import pack_any
from statefun_tasks.messages_pb2 import TaskRequest, TaskResults, TaskResult, TaskException, DeferredTask, TupleOfAny, \
    PausedTask, TaskStatus, ChildPipeline, Address, TaskActionRequest, TaskAction
from statefun_tasks.type_helpers import _create_task_exception
from collections import ChainMap, deque


class _PipelineBehaviour(object):
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
                    yield from _PipelineBehaviour.yield_tasks(group_entry)
            else:
                yield task_or_group

    def get_task_chain(self, task_id):
        
        def extract_task_chain(state, pipeline, entry):
            tasks = state

            if entry.task_id == task_id:
                matched = False

                for task in _PipelineBehaviour.yield_tasks(pipeline):
                    if matched or task.task_id == task_id:
                        matched = True
                        tasks.append(task)

                return tasks, True # break

            return tasks, False # continue

        task_chain = []
        _PipelineBehaviour.walk_pipeline(self._pipeline, extract_task_chain, task_chain)

        return task_chain

    def get_last_task_in_chain(self, task_id):
        task_chain = self.get_task_chain(task_id)
        return task_chain[-1]

    def get_initial_tasks(self, entry=None):
        max_parallelism = 0 # unlimited

        stack = deque([entry or self._pipeline[0]])  # FIFO
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

        return tasks, None if max_parallelism < 1 else max_parallelism

    def notify_pipeline_created(self, context):
        # if this pipeline is the already root do nothing as it's not a child
        if context.pipeline_state.root_id == context.pipeline_state.id:
            return

        child_pipeline = ChildPipeline(
            id = context.pipeline_state.id,
            address = context.pipeline_state.address,
            root_id = context.pipeline_state.root_id,
            root_address = context.pipeline_state.root_address,
            caller_id = context.pipeline_state.caller_id,
            caller_address = context.pipeline_state.caller_address
        )
        
        for task in self.yield_tasks(self._pipeline): 
            child_pipeline.tasks.append(Address(namespace=task.namespace, type=task.worker_name, id=task.task_id))

        # notify back to the root pipeline
        context.send_message(child_pipeline.root_address, child_pipeline.root_id, child_pipeline)

    def defer_tasks(self, context, tasks, task_result_or_exception=None, max_parallelism=None):     
        # unpack task_result into result + state so we can merge in extras later
        if task_result_or_exception is not None:
            task_result, task_state = self._serialiser.unpack_response(task_result_or_exception)
        else:
            task_result, task_state = None, None

        # split into tasks to call vs those to defer
        if max_parallelism is None or max_parallelism < 1:
            tasks_to_call, tasks_to_defer = tasks, []
        else:
            tasks_to_call, tasks_to_defer = tasks[:max_parallelism], tasks[max_parallelism:]

        if any(tasks_to_defer):

            # create task deferral to hold these deferred tasks
            deferral_id = _gen_id()
            task_deferral = context.pipeline_state.task_deferrals_by_id[deferral_id]

            for task in reversed(tasks_to_defer):
                # add in state, extra task args & kwargs if we have them
                request = self._create_task_request(context, task, task.request, task_state, task_result)

                # add each task to deferral
                task_deferral.deferred_tasks.append(DeferredTask(destination=task.get_destination(), task_request=request))

            # parent tasks for the deferrals are the final tasks in each chain beginning with the tasks we are going to call
            # when a parent task completes, a task from the deferral will be called keep the overall parallism at max_parallelism
            for task in tasks_to_call:
                last_task = self.get_last_task_in_chain(task.task_id)
                context.pipeline_state.task_deferral_ids_by_task_id[last_task.task_id] = deferral_id

        # return task requests for those to call
        return [(task, self._create_task_request(context, task, task.request, task_state, task_result)) for task in tasks_to_call]

    def release_deferred_tasks(self, context, caller_id, task_result_or_exception):
        if isinstance(task_result_or_exception, TaskException):
            parent_id = self.get_last_task_in_chain(caller_id).id
        else:
            parent_id = caller_id
         
        if parent_id in context.pipeline_state.task_deferral_ids_by_task_id:
            deferral_id = context.pipeline_state.task_deferral_ids_by_task_id[parent_id]
            task_deferral = context.pipeline_state.task_deferrals_by_id[deferral_id]

            if any(task_deferral.deferred_tasks):
                deferred_task = task_deferral.deferred_tasks.pop()
                task_request = deferred_task.task_request

                # add task we are submitting to deferral task
                last_task = self.get_last_task_in_chain(task_request.id)
                context.pipeline_state.task_deferral_ids_by_task_id[last_task.id] = deferral_id

                # send the task
                self.send_task(context, deferred_task.destination, task_request)
            else:
                # clean up
                del context.pipeline_state.task_deferral_ids_by_task_id[caller_id]
                del context.pipeline_state.task_deferrals_by_id[deferral_id]

    def save_result_before_finally(self, context, task_result_or_exception):
        before_finally = context.pipeline_state.exception_before_finally \
            if isinstance(task_result_or_exception, TaskException) \
            else context.pipeline_state.result_before_finally

        before_finally.CopyFrom(task_result_or_exception)

    def get_result_before_finally(self, context):
        if context.pipeline_state.HasField('result_before_finally'):
            return context.pipeline_state.result_before_finally
        elif context.pipeline_state.HasField('exception_before_finally'):
           return context.pipeline_state.exception_before_finally

        return None

    def send_task(self, context, destination, task_request):
        # if the pipeline is paused we record the tasks to be sent into the pipeline state
        if  context.pipeline_state.status.value == TaskStatus.Status.PAUSED:
            paused_task = PausedTask(destination=destination, task_request=task_request)
            context.pipeline_state.paused_tasks.append(paused_task)

        # else send the tasks to their destinations
        else:
            context.send_message(destination, task_request.id, task_request)

    def pause_tasks(self, context):
        if context.pipeline_state.status.value not in [TaskStatus.PENDING, TaskStatus.RUNNING, TaskStatus.PAUSED]:
            raise ValueError(f'Pipeline is not in a state that can be paused')

        context.pipeline_state.status.value = TaskStatus.Status.PAUSED

        # tell any child pipelines to pause
        for child_pipeline in context.pipeline_state.child_pipelines:
            pause_action = TaskActionRequest(id=child_pipeline.id, action=TaskAction.PAUSE_PIPELINE)
            context.send_message(child_pipeline.address, pause_action.id, pause_action)

    def resume_paused_tasks(self, context):
        if context.pipeline_state.status.value not in [TaskStatus.PENDING, TaskStatus.RUNNING, TaskStatus.PAUSED]:
            raise ValueError(f'Pipeline is not in a state that can be paused')
        
        context.pipeline_state.status.value = TaskStatus.Status.RUNNING

        for paused_task in context.pipeline_state.paused_tasks:
            context.send_message(paused_task.destination, paused_task.task_request.id, paused_task.task_request)

        context.pipeline_state.ClearField('paused_tasks')

        # tell any child pipelines to resume
        for child_pipeline in context.pipeline_state.child_pipelines:
            pause_action = TaskActionRequest(id=child_pipeline.id, action=TaskAction.UNPAUSE_PIPELINE)
            context.send_message(child_pipeline.address, pause_action.id, pause_action)

    def cancel_pipeline(self, context):
        if context.pipeline_state.status.value not in [TaskStatus.PENDING, TaskStatus.RUNNING, TaskStatus.PAUSED]:
            raise ValueError(f'Pipeline is not in a state that can be cancelled')

        context.pipeline_state.status.value = TaskStatus.Status.CANCELLING

        # tell any child pipelines to cancel
        for child_pipeline in context.pipeline_state.child_pipelines:
            cancel_action = TaskActionRequest(id=child_pipeline.id, action=TaskAction.CANCEL_PIPELINE)
            context.send_message(child_pipeline.address, cancel_action.id, cancel_action)

        cancellation_ex = _create_task_exception(
            context.storage.task_request, 
            TaskCancelledException('Pipeline was cancelled'), 
            context.pipeline_state.last_task_state)

        finally_task = self.try_get_finally_task(context.get_caller_id())

        # we move from cancelling to cancelled either by sending and/or waiting on the finally task
        if finally_task is not None:
            if self.get_result_before_finally(context) is None:
                tasks = self.defer_tasks(context, [finally_task], cancellation_ex)

                for task, task_request in tasks:
                    context.send_message(task.get_destination(), task_request.id, task_request)

            # make sure result before finally is set to the task cancellation exception
            self.save_result_before_finally(context, cancellation_ex)

        else:
            # or by sending cancellation to ourself if there is no finally task
            context.send_message(context.pipeline_state.address, context.pipeline_state.id, cancellation_ex)

    def mark_task_complete(self, task_id, task_result_or_exception):
        failed = isinstance(task_result_or_exception, TaskException)

        tasks = self.get_task_chain(task_id)

        if failed:
            # mark this task complete and its children
            for task in tasks:
                task.mark_complete()
        elif any(tasks):
            tasks[0].mark_complete()
                        
    def add_to_task_results(self, task_id, task_result_or_exception, task_results):
        failed = isinstance(task_result_or_exception, TaskException)

        last_task = self.get_last_task_in_chain(task_id)
        
        if task_id == last_task.task_id:
            # if we are the last task in this chain in the group then record this result so we can aggregate laster
            task_results.by_id[last_task.task_id].CopyFrom(pack_any(task_result_or_exception))

        elif failed:
            # additionally propogate the error onto the last stage of this chain
            task_results.by_id[last_task.task_id].CopyFrom(pack_any(task_result_or_exception))

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
                            return entry, entry_after_parent_group, parent_group
                        else:
                            # otherwise if there is no next task and the group containing this task is complete 
                            # bubble up to the task after the group
                            if next_entry is None and group_entry is not None and group_entry.is_complete() and next_entry is None:
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

    def _create_task_request(self, context, task, task_request, task_state=None, task_result=None):
        if task_result is not None:
            task_args_and_kwargs = self._serialiser.to_args_and_kwargs(task.request)
            task_request = self._serialiser.merge_args_and_kwargs(task_result if not task.is_finally else TupleOfAny(), task_args_and_kwargs)

        request = TaskRequest(id=task.task_id, type=task.task_type)

        # allow callers to drop the results of fruitful tasks in their pipelines if they wish
        request.is_fruitful = task.is_fruitful

        # set extra pipeline related parameters
        self._add_pipeline_meta(context, request)

        self._serialiser.serialise_request(request, task_request, state=task_state, retry_policy=task.retry_policy)
        return request

    def _add_pipeline_meta(self, context, task_request):
        task_request.meta['pipeline_address'] = context.pipeline_state.address
        task_request.meta['pipeline_id'] = context.pipeline_state.id
        task_request.meta['root_pipeline_id'] = context.pipeline_state.root_id
        task_request.meta['root_pipeline_address'] = context.pipeline_state.root_address

        caller_id = context.get_caller_id()

        if caller_id is not None:
            task_request.meta['parent_task_address'] = context.get_caller_address()
            task_request.meta['parent_task_id'] = caller_id
