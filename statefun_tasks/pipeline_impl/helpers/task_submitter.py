from statefun_tasks.context import TaskContext
from statefun_tasks.messages_pb2 import TaskRequest, TaskResult, TupleOfAny, TaskException, PausedTask, TaskStatus
from statefun_tasks.storage import StorageBackend
from statefun_tasks.types import Task
from statefun_tasks.utils import _gen_id
from typing import List
import asyncio
import logging


_log = logging.getLogger('FlinkTasks')


class DeferredTaskSubmitter(object):
    def __init__(self, graph, serialiser, storage: StorageBackend):
        self._graph = graph
        self._serialiser = serialiser
        self._storage = storage

    async def submit_tasks(self, context: TaskContext, tasks: List[Task], task_result_or_exception=None, max_parallelism=None):     
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
            deferral_id = self._create_task_deferral(context, tasks_to_defer, task_result_or_exception)

            # parent tasks for the deferrals are the final tasks in each chain beginning with the tasks we are going to call
            # when a parent task completes, a task from the deferral will be called keep the overall parallism at max_parallelism
            for task in tasks_to_call:
                last_task = self._graph.get_last_task_in_chain(task.task_id)
                context.pipeline_state.task_deferral_ids_by_task_id[last_task.task_id] = deferral_id
            
        sent_tasks = []
        for task in tasks_to_call:
            task_request = self._create_task_request(context, task, task.request, task_state, task_result, deferral=None)
            sent_tasks.append(asyncio.ensure_future(self._send_task(context, task.get_destination(), task_request)))

        if any(sent_tasks):
            await asyncio.gather(*sent_tasks)

    async def release_tasks(self, context, caller_id, task_result_or_exception):
        if isinstance(task_result_or_exception, TaskException):
            parent_id = self._graph.get_last_task_in_chain(caller_id).id
        else:
            parent_id = caller_id
         
        if parent_id in context.pipeline_state.task_deferral_ids_by_task_id:
            deferral_id = context.pipeline_state.task_deferral_ids_by_task_id[parent_id]
            task_deferral = context.pipeline_state.task_deferrals_by_id[deferral_id]

            if any(task_deferral.task_ids):
                task_id = task_deferral.task_ids.pop()
                task = self._graph.get_task(task_id) 

                task_request = self._create_deferred_task_request(context, task, task_deferral)
                
                # add task we are submitting to deferral task
                last_task = self._graph.get_last_task_in_chain(task_id)
                context.pipeline_state.task_deferral_ids_by_task_id[last_task.id] = deferral_id

                # send the task
                await self._send_task(context, task.get_destination(), task_request)
            else:
                # clean up
                del context.pipeline_state.task_deferral_ids_by_task_id[caller_id]
                del context.pipeline_state.task_deferrals_by_id[deferral_id]

    async def unpause_tasks(self, context):
        for paused_task in context.pipeline_state.paused_tasks:

            if paused_task.HasField('task_request'):
                context.send_message(paused_task.destination, paused_task.task_request.id, paused_task.task_request)
            else:
                paused_task = await self._load_from_store(context, paused_task)
                context.send_message(paused_task.destination, paused_task.task_request.id, paused_task.task_request)

        context.pipeline_state.ClearField('paused_tasks')
        await self._cleanup_storage(context)

    def _create_task_deferral(self, context, tasks_to_defer, task_result_or_exception):
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
            task_deferral.task_ids.append(task.id)

        return deferral_id

    def _create_deferred_task_request(self, context, task, task_deferral):
        task_state, task_result = None, None
        if task_deferral.HasField('task_result_or_exception'):
            
            if task_deferral.task_result_or_exception.HasField('task_result'):
                task_result_or_exception = task_deferral.task_result_or_exception.task_result
            else:
                task_result_or_exception = task_deferral.task_result_or_exception.task_exception

            task_result, task_state = self._serialiser.unpack_response(task_result_or_exception)

        return self._create_task_request(context, task, task.request, task_state, task_result, deferral=task_deferral)

    def _create_task_request(self, context, task, task_request, task_state=None, task_result=None, deferral=None):
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

        self._serialiser.serialise_request(request, task_request, state=task_state, retry_policy=task.retry_policy)
        return request

    async def _send_task(self, context, destination, task_request):
        # if the pipeline is paused we record the tasks to be sent into the pipeline state
        if  context.pipeline_state.status.value == TaskStatus.PAUSED:
            
            saved_to_store = False

            if self._storage is not None and context.pipeline_state_size >= self._storage.threshold:
                proto = PausedTask(destination=destination, task_request=task_request)
                saved_to_store = await self._try_save_to_store(context, proto, task_request.id)
                            
            if not saved_to_store:
                paused_task = PausedTask(destination=destination, task_request=task_request)
            else:
                paused_task = PausedTask(destination=destination, task_id=task_request.id)
            
            context.pipeline_state.paused_tasks.append(paused_task)

        # else send the tasks to their destinations
        else:
            context.send_message(destination, task_request.id, task_request)

    async def _try_save_to_store(self, context, paused_task, task_id):
        try:
            keys = [context.pipeline_state.id, task_id]
            await self._storage.store(keys, paused_task.SerializeToString())
            return True
        except Exception as ex:
            _log.warning(f'Error saving {keys} to backend storage - {ex}')
            return False

    async def _load_from_store(self, context, paused_task):
        keys = [context.pipeline_state.id, paused_task.task_id]

        proto_bytes = await self._storage.fetch(keys)
        paused_task = PausedTask()
        paused_task.ParseFromString(proto_bytes)
        return paused_task

    async def _cleanup_storage(self, context):
        if self._storage is None:
            return

        try:
            await self._storage.delete([context.pipeline_state.id])
        except Exception as ex:
            _log.warning(f'Error cleaning up paused tasks for {context.pipeline_state.id} from backend storage - {ex}')
