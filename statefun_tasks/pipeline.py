from statefun_tasks.context import TaskContext
from statefun_tasks.messages_pb2 import ChildPipeline, TaskRequest, TaskResult, TaskException, Pipeline, TaskActionRequest, TaskAction, TaskStatus
from statefun_tasks.serialisation import DefaultSerialiser
from statefun_tasks.storage import StorageBackend
from statefun_tasks.types import Task, Group, TaskCancelledException
from statefun_tasks.type_helpers import _create_task_exception
from statefun_tasks.pipeline_impl.handlers import BeginPipelineHandler, ContinuePipelineHandler, EndPipelineHandler, CancelPipelineHandler
from statefun_tasks.pipeline_impl.helpers import PipelineGraph, DeferredTaskSubmitter
from statefun_tasks.events import EventHandlers
from google.protobuf.any_pb2 import Any
from typing import Union


class _Pipeline(object):
    def __init__(self, pipeline: list, serialiser=None, is_fruitful=True, events: EventHandlers=None, storage: StorageBackend=None):
        self._pipeline = pipeline
        self._serialiser = serialiser or DefaultSerialiser()
        self._is_fruitful = is_fruitful
        self._events = events or EventHandlers()
        self._storage = storage

        self._handlers = [
            BeginPipelineHandler(self._pipeline, self._serialiser, self._storage),
            ContinuePipelineHandler(self._pipeline, self._serialiser, self._storage),
            CancelPipelineHandler(self._pipeline, self._serialiser, self._storage),
            EndPipelineHandler(self._pipeline, self._serialiser, self._storage)
        ]

        self._graph = PipelineGraph(self._pipeline)
        self._submitter = DeferredTaskSubmitter(self._graph, self._serialiser, self._storage)

    @property
    def events(self) -> EventHandlers:
        """
        EventHandler for this _Pipeline instance
        """
        return self._events

    @property
    def is_fruitful(self):
        return self._is_fruitful

    def to_proto(self) -> Pipeline:
        pipeline = Pipeline(entries=[p.to_proto(self._serialiser) for p in self._pipeline])
        return pipeline

    @staticmethod
    def from_proto(pipeline_proto: Pipeline, serialiser, events, storage):
        pipeline = []

        for proto in pipeline_proto.entries:
            if proto.HasField('task_entry'):
                pipeline.append(Task.from_proto(proto))
            elif proto.HasField('group_entry'):
                pipeline.append(Group.from_proto(proto))

        return _Pipeline(pipeline, serialiser=serialiser, events=events, storage=storage)

    async def handle_message(self, context: TaskContext, message: Union[TaskRequest, TaskResult, TaskException], task_state: Any=None) -> bool:
        handled = False

        for handler in self._handlers:
            if handler.can_handle_message(context, message):
                should_continue, message = await handler.handle_message(context, message, pipeline=self, task_state=task_state)
                handled = True

                if not should_continue:
                    break

        return handled

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

    def add_child(self, context: TaskContext, child_pipeline: ChildPipeline):
        context.pipeline_state.child_pipelines.append(child_pipeline)

    def status(self, context: TaskContext):
        return context.pipeline_state.status

    async def pause(self, context: TaskContext):
        if context.pipeline_state.status.value not in [TaskStatus.PENDING, TaskStatus.RUNNING, TaskStatus.PAUSED]:
            raise ValueError(f'Pipeline is not in a state that can be paused')

        context.pipeline_state.status.value = TaskStatus.Status.PAUSED
        self.events.notify_pipeline_status_changed(context, context.pipeline_state.pipeline, context.pipeline_state.status.value)

        # tell any child pipelines to pause
        for child_pipeline in context.pipeline_state.child_pipelines:
            pause_action = TaskActionRequest(id=child_pipeline.id, action=TaskAction.PAUSE_PIPELINE)
            context.send_message(child_pipeline.address, pause_action.id, pause_action)

    async def unpause(self, context: TaskContext):
        if context.pipeline_state.status.value not in [TaskStatus.PENDING, TaskStatus.RUNNING, TaskStatus.PAUSED]:
            raise ValueError(f'Pipeline is not in a state that can be unpaused')
        
        context.pipeline_state.status.value = TaskStatus.Status.RUNNING
        self.events.notify_pipeline_status_changed(context, context.pipeline_state.pipeline, context.pipeline_state.status.value)

        try:
            await self._submitter.unpause_tasks(context)

            # tell any child pipelines to resume
            for child_pipeline in context.pipeline_state.child_pipelines:
                pause_action = TaskActionRequest(id=child_pipeline.id, action=TaskAction.UNPAUSE_PIPELINE)
                context.send_message(child_pipeline.address, pause_action.id, pause_action)

        except Exception as ex:
           # abort the pipeline if we could not resume the tasks
            await self.cancel(context, ex)

    async def cancel(self, context: TaskContext, ex=None):
        if context.pipeline_state.status.value not in [TaskStatus.PENDING, TaskStatus.RUNNING, TaskStatus.PAUSED]:
            raise ValueError(f'Pipeline is not in a state that can be cancelled')

        context.pipeline_state.status.value = TaskStatus.Status.CANCELLING
        self.events.notify_pipeline_status_changed(context, context.pipeline_state.pipeline, context.pipeline_state.status.value)

        # tell any child pipelines to cancel
        for child_pipeline in context.pipeline_state.child_pipelines:
            cancel_action = TaskActionRequest(id=child_pipeline.id, action=TaskAction.CANCEL_PIPELINE)
            context.send_message(child_pipeline.address, cancel_action.id, cancel_action)

        # construct the cancellation exception to send to caller of this pipeline
        ex = ex or TaskCancelledException('Pipeline was cancelled')
        cancellation_ex = _create_task_exception(context.storage.task_request, ex, context.pipeline_state.last_task_state)

        # we move from cancelling to cancelled either by submitting and/or waiting on the finally task...
        finally_task = self._graph.try_get_finally_task(context.get_caller_id())

        if finally_task is not None:
            if self.get_result_before_finally(context) is None:

                # then we still need to submit the finally task
                self._submitter.submit_tasks(context, [finally_task], cancellation_ex)

            # set result before finally to our task cancellation exception
            self.save_result_before_finally(context, cancellation_ex)

        else:
            # ...or by sending cancellation to ourself if there is no finally task
            context.send_message(context.pipeline_state.address, context.pipeline_state.id, cancellation_ex)
