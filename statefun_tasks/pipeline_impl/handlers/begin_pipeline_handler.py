from statefun_tasks.context import TaskContext
from statefun_tasks.pipeline_impl.handlers import PipelineMessageHandler
from statefun_tasks.messages_pb2 import TaskRequest, TaskResult, TaskException, TaskStatus, PipelineState, ChildPipeline, TaskInfo
from statefun_tasks.protobuf import unpack_any
from statefun_tasks.serialisation import pack_any
from statefun_tasks.utils import _gen_id
from statefun_tasks.type_helpers import _create_task_result
from google.protobuf.any_pb2 import Any
from typing import Union


class BeginPipelineHandler(PipelineMessageHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
    
    def can_handle_message(self, context: TaskContext, message: Union[TaskRequest, TaskResult, TaskException]) -> bool:
        return (context.pipeline_state is None or context.pipeline_state.status.value == TaskStatus.PENDING) \
            and isinstance(message, TaskRequest)

    async def handle_message(self, context: TaskContext, message: Union[TaskRequest, TaskResult, TaskException], pipeline: '_Pipeline', state: Any):
        invoking_task = message  # type: TaskRequest

        # ensure we pick up the correct caller id when task producing this pipeline is a retry
        task_state = context.task_state.by_uid[invoking_task.uid]
        if task_state.original_caller_id == '':
            caller_id = context.get_caller_id()
        else:
            caller_id = task_state.original_caller_id

        # 1. record all the continuations into a pipeline and save into state with caller id and address
        context.pipeline_state = PipelineState(id = context.get_task_id(), address = context.get_address())
        context.pipeline_state.status.value = TaskStatus.RUNNING
        context.pipeline_state.pipeline.CopyFrom(pipeline.to_proto())
        context.pipeline_state.is_fruitful = pipeline.is_fruitful
        context.pipeline_state.task_state.CopyFrom(pack_any(self.serialiser.to_proto(state)))
        context.pipeline_state.invocation_id = _gen_id()

        if caller_id is not None:
            context.pipeline_state.caller_id = caller_id
            context.pipeline_state.caller_address = context.get_caller_address()

        # and record the root pipeline details from the calling task into our pipeline state to aid in tracking nested pipelines
        context.pipeline_state.root_id = invoking_task.meta['root_pipeline_id'] or context.pipeline_state.id
        context.pipeline_state.root_address = invoking_task.meta['root_pipeline_address'] or context.pipeline_state.address

        await pipeline.events.notify_pipeline_created(context, context.pipeline_state.pipeline)

        # and notify root pipeline of a new child pipeline
        self._notify_pipeline_created(context)

        # if we got an empty pipeline then complete immediately
        if self._graph.is_empty():
            task_result = _create_task_result(message)
            task_result.invocation_id = context.pipeline_state.invocation_id
            self._serialiser.serialise_result(task_result, ([]), state)

            context.pipeline_state.status.value = TaskStatus.COMPLETED
            await pipeline.events.notify_pipeline_status_changed(context, context.pipeline_state.pipeline, context.pipeline_state.status.value)
            
            # continue
            return True, task_result

        else:
            # else get initial tasks(s) to call - might be single start of chain task or a group of tasks to call in parallel
            tasks, max_parallelism, slice = self.graph.get_initial_tasks()

            # send initial state to each initial task if we have some
            if context.pipeline_state.pipeline.HasField('initial_state'):                
                task_state = unpack_any(context.pipeline_state.pipeline.initial_state, known_proto_types=[])
            else:
                task_state = None

            # send initial arguments to each initial task if we have them
            if context.pipeline_state.pipeline.HasField('initial_args'):
                initial_args = unpack_any(context.pipeline_state.pipeline.initial_args, known_proto_types=[])
            else:
                initial_args = None

            # if we skipped over empty group(s) then make sure we pass empty array to next task (result of in_parallel([]) is intuitively [])
            if slice > 0:
                for task in tasks:
                    _, kwargs = self._serialiser.deserialise_args_and_kwargs(task.request)
                    task.request = self._serialiser.serialise_args_and_kwargs(([]), kwargs)

            # split into tasks to call now and those to defer if max parallelism is exceeded
            self.submitter.submit_tasks(context, tasks, task_state=task_state, max_parallelism=max_parallelism, initial_args=initial_args)

            # break
            return False, message

    def _notify_pipeline_created(self, context):
        # if this pipeline is the already root do nothing as it's not a child
        if context.pipeline_state.root_id == context.pipeline_state.id:
            return

        child_pipeline = ChildPipeline(
            id = context.pipeline_state.id,
            invocation_id = context.pipeline_state.invocation_id,
            address = context.pipeline_state.address,
            root_id = context.pipeline_state.root_id,
            root_address = context.pipeline_state.root_address,
            caller_id = context.pipeline_state.caller_id,
            caller_address = context.pipeline_state.caller_address
        )
        
        for task in self.graph.yield_tasks(): 
            child_pipeline.tasks.append(TaskInfo(task_id=task.task_id, task_uid=task.uid, task_type=task.task_type, namespace=task.namespace, worker_name=task.worker_name))

        # notify back to the root pipeline
        context.send_message(child_pipeline.root_address, child_pipeline.root_id, child_pipeline)
