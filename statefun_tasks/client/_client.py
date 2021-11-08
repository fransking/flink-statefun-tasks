from statefun_tasks import DefaultSerialiser, PipelineBuilder, TaskRequest, TaskResult, TaskException, TaskAction, \
    TaskActionRequest, TaskActionResult, TaskActionException, TaskStatus as TaskStatusProto, Task
from statefun_tasks.client import TaskError, TaskStatus

from google.protobuf.any_pb2 import Any
from kafka import KafkaProducer, KafkaConsumer

import logging
from uuid import uuid4
from threading import Thread
import asyncio
from concurrent.futures import Future
from typing import Union


_log = logging.getLogger('FlinkTasks')


class FlinkTasksClient(object):
    """
    Client for submitting TaskRequests / TaskActionRequests
    
    Replies are handled on a dedicated thread created per instance so FlinkTasksClientFactory.get_client() is preferred to 
    instantiating this class directly.

    :param kafka_broker_url: url of the kafka broker (or list of urls) used for ingress and egress
    :param request_topics: dictionary of worker to ingress topic mappings  (use None for default)
                            e.g. {'example/worker': 'example.requests', None: 'example.default.requests'}
    :param action_toptics: as per request_topics but used for action requests
    :param reply_topic: topic to listen on for responses (a unique consumer group id will be created)
    :param optional group_id: kafka group id to use when subscribing to reply_topic
    :param optional serialiser: serialiser to use (will use DefaultSerialiser if not set)
    :param optional kafka_properties: additional properties to be passed to the KafkaConsumer and KafkaProducer
    :param optional kafka_consumer_properties: additional properties to be passed to the KafkaConsumer
    :param optional kafka_producer_properties: additional properties to be passed to the KafkaProducer
    """

    def __init__(self, kafka_broker_url, request_topics, action_topics, reply_topic, group_id=None, serialiser=None, 
            kafka_properties=None, kafka_consumer_properties=None, kafka_producer_properties=None):

        self._kafka_broker_url = kafka_broker_url
        self._requests = {}

        self._request_topics = request_topics
        self._action_topics = action_topics
        self._reply_topic = reply_topic
        self._group_id = group_id
        self._serialiser = serialiser if serialiser is not None else DefaultSerialiser()

        kafka_properties = kafka_properties or {}
        kafka_consumer_properties = kafka_consumer_properties or {}
        kafka_producer_properties = kafka_producer_properties or {}

        bootstrap_servers = [kafka_broker_url] if isinstance(kafka_broker_url, str) else kafka_broker_url
        self._producer = KafkaProducer(bootstrap_servers=bootstrap_servers, **{**kafka_properties, **kafka_producer_properties})

        self._consumer = KafkaConsumer(
            self._reply_topic,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='earliest',
            group_id=self._group_id, 
            **{**kafka_properties, **kafka_consumer_properties})

        self._consumer_thread = Thread(target=self._consume, args=())
        self._consumer_thread.daemon = True
        self._consumer_thread.start()

    @staticmethod
    def _get_request_key(item):
        if isinstance(item, (TaskRequest, TaskResult, TaskException)):
            return f'request.{item.id}'
        elif isinstance(item, (TaskActionRequest, TaskActionResult, TaskActionException)):
            return f'action.{item.action}.{item.id}'
        else:
            raise ValueError(f'Unsupported request type {type(item)}')

    @staticmethod
    def _try_get_topic_for(pipeline_or_task: Union[PipelineBuilder, Task], topics, topic=None):
        if topic is not None:
            return topic

        destination = pipeline_or_task.get_destination()
        
        if destination in topics:
            return topics[destination]

        if None in topics:
            return topics[None]
        
        return None

    def _get_request_topic(self, pipeline: PipelineBuilder, topic=None):
        topic = self._try_get_topic_for(pipeline, self._request_topics, topic)
        if topic is None:
            raise ValueError(f'Could not find a topic to send this request to')
        else:
            return topic

    def _get_action_topic(self, pipeline_or_task: Union[PipelineBuilder, Task], topic=None):
        topic = self._try_get_topic_for(pipeline_or_task, self._action_topics, topic)
        if topic is None:
            raise ValueError(f'Could not find a topic to send this action to')
        else:
            return topic

    def _submit_request(self, request, topic):
        request_id = self._get_request_key(request)

        # if we have already subscribed then don't subscribe again
        future = self._requests.get(request_id, None)
        if future is not None:
            return future
    
        # else create new future for this request
        future = Future()
        self._requests[request_id] = future

        request.reply_topic = self._reply_topic

        key = request.id.encode('utf-8')
        val = request.SerializeToString()

        submit_response = self._producer.send(topic=topic, key=key, value=val)
        submit_response.add_errback(future.set_exception)
        self._producer.flush()

        return future

    def _submit_action(self, task_id, action, topic):
        task_action = TaskActionRequest(id=task_id, action=action, reply_topic=self._reply_topic)
        return self._submit_request(task_action, topic)

    @property
    def serialiser(self) -> DefaultSerialiser:
        """
        Returns the serialiser used by this client
        """
        return self._serialiser

    def submit(self, pipeline: PipelineBuilder, topic=None) -> Future:
        """
        Submit a pipeline to Flink

        :param pipeline: the pipeline
        :param optional topic: override the default ingress topic
        :return: a Future encapsulating the result of the pipeline
        """
        task_request = pipeline.to_task_request(self._serialiser)
        topic = self._get_request_topic(pipeline, topic)
        return self._submit_request(task_request, topic)

    async def submit_async(self, pipeline: PipelineBuilder, topic=None):
        """
        Submit a pipeline to Flink

        :param pipeline: the pipeline
        :param optional topic: override the default ingress topic
        :return: the result of the pipeline
        """
        return await asyncio.wrap_future(self.submit(pipeline, topic=topic))

    def get_status(self, pipeline_or_task: Union[PipelineBuilder, Task], topic=None) -> Future:
        """
        Get the status of a previously submitted pipeline or task

        :param pipeline_or_task: the pipeline or task
        :param optional topic: override the default ingress topic
        :return: a Future encapsulating the status of the pipeline
        """
        topic = self._get_action_topic(pipeline_or_task, topic)
        return self._submit_action(pipeline_or_task.id, TaskAction.GET_STATUS, topic)

    async def get_status_async(self, pipeline_or_task: Union[PipelineBuilder, Task], topic=None):
        """
        Get the status of a previously submitted pipeline or task

        :param pipeline_or_task: the pipeline or task
        :param optional topic: override the default ingress topic
        :return: the status of the pipeline
        """
        return await asyncio.wrap_future(self.get_status(pipeline_or_task, topic))
    
    def get_request(self, pipeline_or_task: Union[PipelineBuilder, Task], topic=None) -> Future:
        """
        Get the original TaskRequest for a previously submitted pipeline or task

        :param pipeline_or_task: the pipeline or task
        :param optional topic: override the default ingress topic
        :return: a Future encapsulating the original TaskRequest
        """
        topic = self._get_action_topic(pipeline_or_task, topic)
        return self._submit_action(pipeline_or_task.id, TaskAction.GET_REQUEST, topic)

    async def get_request_async(self, pipeline_or_task: Union[PipelineBuilder, Task], topic=None):
        """
        Get the original TaskRequest for a previously submitted pipeline or task

        :param pipeline_or_task: the pipeline or task
        :param optional topic: override the default ingress topic
        :return: the original TaskRequest
        """
        return await asyncio.wrap_future(self.get_request(pipeline_or_task, topic))

    def get_result(self, pipeline_or_task: Union[PipelineBuilder, Task], topic=None) -> Future:
        """
        Get the TaskResult for a previously submitted pipeline or task

        :param pipeline_or_task: the pipeline or task
        :param optional topic: override the default ingress topic
        :return: a Future encapsulating the original TaskResult
        """
        topic = self._get_action_topic(pipeline_or_task, topic)
        return self._submit_action(pipeline_or_task.id, TaskAction.GET_RESULT, topic)

    async def get_result_async(self, pipeline_or_task: Union[PipelineBuilder, Task], topic=None):
        """
        Get the TaskResult for a previously submitted pipeline or task

        :param pipeline_or_task: the pipeline or task
        :param optional topic: override the default ingress topic
        :return: the original TaskResult
        """
        return await asyncio.wrap_future(self.get_result(pipeline_or_task, topic))

    def pause_pipeline(self, pipeline_or_task: Union[PipelineBuilder, Task], topic=None) -> Future:
        """
        Pauses a pipeline

        :param pipeline_or_task: the pipeline or task
        :param optional topic: override the default ingress topic
        :return: a Future indicating whether the pipeline was successfully paused or not
        """
        topic = self._get_action_topic(pipeline_or_task, topic)
        return self._submit_action(pipeline_or_task.id, TaskAction.PAUSE_PIPELINE, topic)

    async def pause_pipeline_async(self, pipeline_or_task: Union[PipelineBuilder, Task], topic=None):
        """
        Pauses a pipeline

        :param pipeline_or_task: the pipeline or task
        :param optional topic: override the default ingress topic
        """
        await asyncio.wrap_future(self.pause_pipeline(pipeline_or_task, topic))

    def unpause_pipeline(self, pipeline_or_task: Union[PipelineBuilder, Task], topic=None) -> Future:
        """
        Unpauses a pipeline

        :param pipeline_or_task: the pipeline or task
        :param optional topic: override the default ingress topic
        :return: a Future indicating whether the pipeline was successfully paused or not
        """
        topic = self._get_action_topic(pipeline_or_task, topic)
        return self._submit_action(pipeline_or_task.id, TaskAction.UNPAUSE_PIPELINE, topic)

    async def unpause_pipeline_async(self, pipeline_or_task: Union[PipelineBuilder, Task], topic=None):
        """
        Unpauses a pipeline

        :param pipeline_or_task: the pipeline or task
        :param optional topic: override the default ingress topic
        """
        await asyncio.wrap_future(self.unpause_pipeline(pipeline_or_task, topic))

    def cancel_pipeline(self, pipeline_or_task: Union[PipelineBuilder, Task], topic=None) -> Future:
        """
        Cancels a pipeline

        :param pipeline_or_task: the pipeline or task
        :param optional topic: override the default ingress topic
        :return: a Future indicating whether the pipeline was successfully paused or not
        """
        topic = self._get_action_topic(pipeline_or_task, topic)
        return self._submit_action(pipeline_or_task.id, TaskAction.CANCEL_PIPELINE, topic)

    async def cancel_pipeline_async(self, pipeline_or_task: Union[PipelineBuilder, Task], topic=None):
        """
        Cancels a pipeline

        :param pipeline_or_task: the pipeline or task
        :param optional topic: override the default ingress topic
        """
        await asyncio.wrap_future(self.cancel_pipeline(pipeline_or_task, topic))

    def _consume(self):
        while True:
            try:
                for message in self._consumer:

                    _log.debug(f'Message received - {message}')

                    proto = Any()
                    proto.ParseFromString(message.value)

                    if proto.Is(TaskException.DESCRIPTOR):
                        self._raise_exception(proto, TaskException)
                        
                    elif proto.Is(TaskResult.DESCRIPTOR):
                        self._return_result(proto, TaskResult)

                    elif proto.Is(TaskActionException.DESCRIPTOR):
                        self._raise_exception(proto, TaskActionException)

                    elif proto.Is(TaskActionResult.DESCRIPTOR):
                        self._return_action_result(proto, TaskActionResult)

            except Exception as ex:
                _log.warning(f'Exception in consumer thread - {ex}', exc_info=ex)


    @staticmethod
    def _unpack(any_proto: Any, proto_type):
        proto = proto_type()
        any_proto.Unpack(proto)
        return proto

    def _unpack_with_future(self, any_proto: Any, proto_type):
        proto = self._unpack(any_proto, proto_type)

        request_id = self._get_request_key(proto)
        future = self._requests.get(request_id, None)

        if future is not None:
            del self._requests[request_id]
            return proto, future
        
        return None, None

    def _return_action_result(self, any_proto: Any, proto_type):
        proto, future = self._unpack_with_future(any_proto, proto_type)

        if future is not None:
            try:

                if proto.action == TaskAction.GET_STATUS:
                    future.set_result(TaskStatus(self._unpack(proto.result, TaskStatusProto).value))
                
                elif proto.action == TaskAction.GET_REQUEST:
                    future.set_result(self._unpack(proto.result, TaskRequest))

                elif proto.action == TaskAction.GET_RESULT:
                    
                    if proto.result.Is(TaskResult.DESCRIPTOR):
                        result, _ = self._serialiser.deserialise_result(self._unpack(proto.result, TaskResult))
                        future.set_result(result)

                    elif proto.result.Is(TaskException.DESCRIPTOR):
                        future.set_exception(TaskError(self._unpack(proto.result, TaskException)))

                else:
                    future.set_result(None)

            except Exception as ex:
                future.set_exception(ex)

    def _return_result(self, any: Any, proto_type):
        task_result, future = self._unpack_with_future(any, proto_type)

        if future is not None:
            try:
                result, _ = self._serialiser.deserialise_result(task_result)
                future.set_result(result)
            except Exception as ex:
                future.set_exception(ex)

    def _raise_exception(self, any: Any, proto_type):
        task_exception, future = self._unpack_with_future(any, proto_type)

        if future is not None:
            try:
                future.set_exception(TaskError(task_exception))
            except Exception as ex:
                future.set_exception(ex)


class FlinkTasksClientFactory():
    """
    Factory for creating memoized FlinkTasksClients
    """
    __clients = {}

    @staticmethod
    def get_client(kafka_broker_url, request_topics: dict, action_topics: dict, reply_topic, serialiser=None, 
            kafka_properties=None, kafka_consumer_properties=None, kafka_producer_properties=None) -> FlinkTasksClient:
        """
        Creates a FlinkTasksClient for submitting tasks to flink.  Clients are memoized by broker url and reply topic.

        :param kafka_broker_url: url of the kafka broker (or list of urls) used for ingress and egress
        :param request_topics: dictionary of worker to ingress topic mappings  (use None for default)
                              e.g. {'example/worker': 'example.requests', None: 'example.default.requests'}
        :param action_toptics: as per request_topics but used for action requests
        :param reply_topic: topic to listen on for responses (a unique consumer group id will be created)
        :param optional serialiser: serialiser to use (will use DefaultSerialiser if not set)
        :param optional kafka_properties: additional properties to be passed to the KafkaConsumer and KafkaProducer
        :param optional kafka_consumer_properties: additional properties to be passed to the KafkaConsumer
        :param optional kafka_producer_properties: additional properties to be passed to the KafkaProducer
        """

        key = f'{kafka_broker_url}.{reply_topic}'

        if key not in FlinkTasksClientFactory.__clients:
            client = FlinkTasksClient(kafka_broker_url, request_topics, action_topics, reply_topic, serialiser=serialiser, group_id=str(uuid4()), 
            kafka_properties=kafka_properties, kafka_consumer_properties=kafka_consumer_properties, kafka_producer_properties=kafka_producer_properties)
            FlinkTasksClientFactory.__clients[key] = client

        return FlinkTasksClientFactory.__clients[key]
