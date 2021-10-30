from statefun_tasks.context import TaskContext
from statefun_tasks.messages_pb2 import DeferredTask, TaskRequest, TupleOfAny, TaskException, PausedTask, TaskStatus
from statefun_tasks.types import Task
from statefun_tasks.utils import _gen_id
from typing import List

class DeferredTaskSubmitter(object):
    def __init__(self, graph, serialiser):
        self._graph = graph
        self._serialiser = serialiser

    def submit_tasks(self, context: TaskContext, tasks: List[Task], task_result_or_exception=None, max_parallelism=None):     
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
                last_task = self._graph.get_last_task_in_chain(task.task_id)
                context.pipeline_state.task_deferral_ids_by_task_id[last_task.task_id] = deferral_id

        for task in tasks_to_call:
            task_request = self._create_task_request(context, task, task.request, task_state, task_result)
            self._send_task(context, task.get_destination(), task_request)

    def release_tasks(self, context, caller_id, task_result_or_exception):
        if isinstance(task_result_or_exception, TaskException):
            parent_id = self._graph.get_last_task_in_chain(caller_id).id
        else:
            parent_id = caller_id
         
        if parent_id in context.pipeline_state.task_deferral_ids_by_task_id:
            deferral_id = context.pipeline_state.task_deferral_ids_by_task_id[parent_id]
            task_deferral = context.pipeline_state.task_deferrals_by_id[deferral_id]

            if any(task_deferral.deferred_tasks):
                deferred_task = task_deferral.deferred_tasks.pop()
                task_request = deferred_task.task_request

                # add task we are submitting to deferral task
                last_task = self._graph.get_last_task_in_chain(task_request.id)
                context.pipeline_state.task_deferral_ids_by_task_id[last_task.id] = deferral_id

                # send the task
                self._send_task(context, deferred_task.destination, task_request)
            else:
                # clean up
                del context.pipeline_state.task_deferral_ids_by_task_id[caller_id]
                del context.pipeline_state.task_deferrals_by_id[deferral_id]

    def unpause_tasks(self, context):
        for paused_task in context.pipeline_state.paused_tasks:
            context.pack_and_send(paused_task.destination, paused_task.task_request.id, paused_task.task_request)

        context.pipeline_state.ClearField('paused_tasks')

    def _create_task_request(self, context, task, task_request, task_state=None, task_result=None):
        if task_result is not None:
            task_args_and_kwargs = self._serialiser.to_args_and_kwargs(task.request)
            task_request = self._serialiser.merge_args_and_kwargs(task_result if not task.is_finally else TupleOfAny(), task_args_and_kwargs)

        request = TaskRequest(id=task.task_id, type=task.task_type)

        # allow callers to drop the results of fruitful tasks in their pipelines if they wish
        request.is_fruitful = task.is_fruitful

        # set extra pipeline related parameters
        request.meta['pipeline_address'] = context.pipeline_state.address
        request.meta['pipeline_id'] = context.pipeline_state.id
        request.meta['root_pipeline_id'] = context.pipeline_state.root_id
        request.meta['root_pipeline_address'] = context.pipeline_state.root_address
        
        if context.get_caller_id() is not None:
            request.meta['parent_task_address'] = context.get_caller_address()
            request.meta['parent_task_id'] = context.get_caller_id()

        self._serialiser.serialise_request(request, task_request, state=task_state, retry_policy=task.retry_policy)
        return request

    def _send_task(self, context, destination, task_request):
        # if the pipeline is paused we record the tasks to be sent into the pipeline state
        if  context.pipeline_state.status.value == TaskStatus.PAUSED:
            paused_task = PausedTask(destination=destination, task_request=task_request)
            context.pipeline_state.paused_tasks.append(paused_task)

        # else send the tasks to their destinations
        else:
            context.pack_and_send(destination, task_request.id, task_request)
