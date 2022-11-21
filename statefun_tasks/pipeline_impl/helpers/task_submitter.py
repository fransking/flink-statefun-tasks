from statefun_tasks.context import TaskContext
from statefun_tasks.messages_pb2 import TaskRequest, TaskResult, TupleOfAny, TaskException, PausedTask, TaskStatus, DeferredTask
from statefun_tasks.types import Task
from statefun_tasks.utils import _gen_id
from statefun_tasks.serialisation import unpack_any
from typing import List
import logging


_log = logging.getLogger('FlinkTasks')


class DeferredTaskSubmitter(object):
    __slots__ = ('_graph', '_serialiser')

    def __init__(self, graph, serialiser):
        self._graph = graph
        self._serialiser = serialiser

    def submit_tasks(
        self, 
        context: TaskContext, 
        tasks: List[Task], 
        task_result_or_exception=None, 
        initial_args=None, 
        initial_kwargs=None,
        initial_state=None,
        max_parallelism=None):     
        
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
            has_initial_args = initial_args is not None or initial_kwargs is not None or initial_state is not None
            
            # create task deferral to hold these deferred tasks
            deferral_id = self._create_task_deferral(context, tasks_to_defer, task_result_or_exception, has_initial_args)

            # parent tasks for the deferrals are the final tasks in each chain beginning with the tasks we are going to call
            # when a parent task completes, a task from the deferral will be called keep the overall parallism at max_parallelism
            for task in tasks_to_call:
                self._add_deferral_trigger(context, task, deferral_id)
        
        for task in tasks_to_call:      
            task_request = self._create_task_request(context, task, task.request, task_state, initial_args, initial_kwargs, initial_state, task_result)
            self._send_task(context, task.get_destination(), task_request)


    def release_tasks(self, context, caller_uid, task_result_or_exception):
        deferral_triggers = self._get_deferral_triggers(caller_uid, task_result_or_exception)
        
        if not any(deferral_triggers):
            return

        # process trigger (we only use one and delete the rest below)
        deferral_trigger = deferral_triggers[-1]

        if deferral_trigger in context.pipeline_state.task_deferral_ids_by_task_uid:
            deferral_id = context.pipeline_state.task_deferral_ids_by_task_uid[deferral_trigger]
            task_deferral = context.pipeline_state.task_deferrals_by_id[deferral_id]

            if any(task_deferral.tasks):
                
                deferred_task = task_deferral.tasks.pop()
                task = self._graph.get_task(deferred_task.task_uid) 

                initial_args, initial_kwargs, initial_state = None, None, None

                if deferred_task.has_initial_args:         
                    # send initial state to each initial task if we have some
                    if context.pipeline_state.pipeline.HasField('initial_state'):
                        initial_state = unpack_any(context.pipeline_state.pipeline.initial_state, known_proto_types=[])

                    # or the current task state if this pipeline is inline
                    elif context.pipeline_state.pipeline.inline:
                        initial_state = unpack_any(context.pipeline_state.task_state, known_proto_types=[])

                    # send initial arguments to each initial task if we have them
                    if context.pipeline_state.pipeline.HasField('initial_args'):
                        initial_args = unpack_any(context.pipeline_state.pipeline.initial_args, known_proto_types=[])

                    # send initial kwargs to each initial task if we have them
                    if context.pipeline_state.pipeline.HasField('initial_kwargs'):
                        initial_kwargs = context.pipeline_state.pipeline.initial_kwargs

                task_request = self._create_task_request_from_deferral(context, task, task_deferral, initial_args, initial_kwargs, initial_state)
                
                # add task we are submitting to deferral trigger so that when it completes we can release a further deferred task
                self._add_deferral_trigger(context, task, deferral_id)
            
                # send the task
                self._send_task(context, task.get_destination(), task_request)
            else:
                # delete deferral now that it is empty
                del context.pipeline_state.task_deferrals_by_id[deferral_id]

            # clean up used deferral triggers
            for task_id in deferral_triggers:
                if task_id in context.pipeline_state.task_deferral_ids_by_task_uid:
                    del context.pipeline_state.task_deferral_ids_by_task_uid[task_id]

    def unpause_tasks(self, context):
        for paused_task in context.pipeline_state.paused_tasks:
            context.send_message(paused_task.destination, paused_task.task_request.id, paused_task.task_request)

        context.pipeline_state.ClearField('paused_tasks')

    def _add_deferral_trigger(self, context, task, deferral_id):
        for task in self._graph.get_last_tasks_in_chain(task.uid):
            context.pipeline_state.task_deferral_ids_by_task_uid[task.uid] = deferral_id

    def _get_deferral_triggers(self, task_id, task_result_or_exception):
        if isinstance(task_result_or_exception, TaskException):
            # only return the triggers once we get to the last exception in the chain
            task_ids = [task.uid for task in self._graph.get_last_tasks_in_chain(task_id) if task is not None]

            if any(task_ids) and task_id == task_ids[-1]:
                return task_ids
            else:
                return []
        else:
            # else return this task as the trigger
            return [task_id]

    def _create_task_deferral(self, context, tasks_to_defer, task_result_or_exception, has_initial_args):
        # create task deferral to hold these deferred tasks
        deferral_id = _gen_id()
        task_deferral = context.pipeline_state.task_deferrals_by_id[deferral_id]

        if context.get_caller_id() is not None:
            task_deferral.parent_task_address = context.get_caller_address()
            task_deferral.parent_task_id = context.get_caller_id()

        if task_result_or_exception is not None:
            if isinstance(task_result_or_exception, TaskResult):
                task_deferral.task_result_or_exception.task_result.CopyFrom(task_result_or_exception)
            
            elif isinstance(task_result_or_exception, TaskException):
                task_deferral.task_result_or_exception.task_exception.CopyFrom(task_result_or_exception)

        for task in reversed(tasks_to_defer):
            task_deferral.tasks.append(DeferredTask(task_uid=task.uid, has_initial_args=has_initial_args))

        return deferral_id

    def _create_task_request_from_deferral(self, context, task, task_deferral, initial_args, initial_kwargs, initial_state):
        task_state, task_result = None, None

        if task_deferral.HasField('task_result_or_exception'):
            
            if task_deferral.task_result_or_exception.HasField('task_result'):
                task_result_or_exception = task_deferral.task_result_or_exception.task_result
            else:
                task_result_or_exception = task_deferral.task_result_or_exception.task_exception

            task_result, task_state = self._serialiser.unpack_response(task_result_or_exception)

        return self._create_task_request(context, task, task.request, task_state, initial_args, initial_kwargs, initial_state, task_result, task_deferral)

    def _create_task_request(self, context, task, task_request, task_state=None, initial_args=None, initial_kwargs=None, initial_state=None, task_result=None, deferral=None):
        if initial_args is not None or initial_kwargs is not None:
            task_args_and_kwargs = self._serialiser.to_args_and_kwargs(task.request)
            task.request = self._serialiser.merge_args_and_kwargs(task_args_and_kwargs, initial_args, initial_kwargs)

        elif task_result is not None:
            task_args_and_kwargs = self._serialiser.to_args_and_kwargs(task.request)
            task_request = self._serialiser.merge_args_and_kwargs(task_args_and_kwargs, task_result if not task.is_finally else TupleOfAny(), None)

        # use initial state if provided
        task_state = initial_state if initial_state is not None else task_state

        request = TaskRequest(id=task.task_id, type=task.task_type, uid=task.uid)

        # allow callers to drop the results of fruitful tasks in their pipelines if they wish
        request.is_fruitful = task.is_fruitful

        # add extra task meta
        self._set_task_meta(context, task, request, deferral)

        self._serialiser.serialise_request(request, task_request, state=task_state, retry_policy=task.retry_policy)
        return request

    def _send_task(self, context, destination, task_request):
        # if the pipeline is paused we record the tasks to be sent into the pipeline state
        if  context.pipeline_state.status.value == TaskStatus.PAUSED:
            
            paused_task = PausedTask(destination=destination, task_request=task_request)
            context.pipeline_state.paused_tasks.append(paused_task)

        # else send the tasks to their destinations
        else:
            context.send_message(destination, task_request.id, task_request)

    def _set_task_meta(self, context, task, request, deferral=None):
        # allow callers to drop the results of fruitful tasks in their pipelines if they wish
        request.is_fruitful = task.is_fruitful

        # set invocation id to pipeline invocation id
        request.invocation_id = context.pipeline_state.invocation_id

        # set extra pipeline related parameters
        request.meta['pipeline_address'] = context.pipeline_state.address
        request.meta['pipeline_id'] = context.pipeline_state.id
        request.meta['root_pipeline_id'] = context.pipeline_state.root_id
        request.meta['root_pipeline_address'] = context.pipeline_state.root_address

        # retain the correct parent pipeline for nested inline child pipelines
        if context.pipeline_state.pipeline.inline:
            request.meta['inline_parent_pipeline_id'] = context.get_parent_pipeline_id()
            request.meta['inline_parent_pipeline_address'] = context.get_parent_pipeline_address()

        if task.display_name is not None:
            request.meta['display_name'] = task.display_name
        
        if deferral is not None:  # copy parent task details from the deferral
            if deferral.HasField('parent_task_address'):
                request.meta['parent_task_address'] = deferral.parent_task_address

            if deferral.HasField('parent_task_id'):
                request.meta['parent_task_id'] = deferral.parent_task_id
        else: 
            # otherwise grab from the context
            if context.get_caller_id() is not None:
                request.meta['parent_task_address'] = context.get_caller_address()
                request.meta['parent_task_id'] = context.get_caller_id()
