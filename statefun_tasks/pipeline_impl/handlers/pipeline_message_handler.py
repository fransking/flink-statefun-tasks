from abc import ABC, abstractmethod
from statefun_tasks.context import TaskContext
from statefun_tasks.serialisation import DefaultSerialiser
from statefun_tasks.messages_pb2 import TaskRequest, TaskResult, TaskException
from statefun_tasks.pipeline_impl.helpers import PipelineGraph, DeferredTaskSubmitter, ResultAggregator, ResultEmitter
from google.protobuf.any_pb2 import Any
from typing import Union, Tuple


class PipelineMessageHandler(ABC):
    def __init__(self, pipeline, serialiser: DefaultSerialiser):
        self._pipeline = pipeline
        self._serialiser = serialiser
        self._graph = PipelineGraph(pipeline)
        self._submitter = DeferredTaskSubmitter(self._graph, serialiser)
        self._result_aggregator = ResultAggregator(self._graph, serialiser)
        self._result_emitter = ResultEmitter()
        
    @abstractmethod
    def can_handle_message(self, context: TaskContext, message: Union[TaskRequest, TaskResult, TaskException]) -> bool:
        pass

    @abstractmethod
    async def handle_message(self, context: TaskContext, message: Union[TaskRequest, TaskResult, TaskException], pipeline: '_Pipeline', state: Any = None) -> Tuple[bool, Union[TaskRequest, TaskResult, TaskException]]:
        pass
    
    @property
    def graph(self):
        return self._graph

    @property
    def serialiser(self):
        return self._serialiser

    @property
    def submitter(self):
        return self._submitter

    @property
    def result_aggregator(self):
        return self._result_aggregator

    @property
    def result_emitter(self):
        return self._result_emitter
