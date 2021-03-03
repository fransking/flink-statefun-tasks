from statefun_tasks import DefaultSerialiser, PipelineBuilder, TaskRequest, TaskResult, TaskException, TaskAction, \
    TaskActionRequest, TaskActionResult, TaskActionException, TaskStatus as TaskStatusProto
from statefun_tasks.client import TaskError, TaskStatus

from google.protobuf.any_pb2 import Any
from kafka import KafkaProducer, KafkaConsumer, TopicPartition

import logging
from uuid import uuid4
from threading import Thread
import asyncio
from concurrent.futures import Future


_log = logging.getLogger('FlinkTasks')


class FlinkTasksClient(object):
    """
    Client for submitting TaskRequests / TaskActionRequests
    
    Replies are handled on a dedicated thread created per instance so FlinkTasksClientFactory.get_client() is preferred to 
    instantiating this class directly.

    :param kafka_broker_url: url of the kafka broker used for ingress and egress
    :param request_topics: dictionary of worker to ingress topic mappings  (use None for default)
                            e.g. {'example/worker': 'example.requests', None: 'example.default.requests'}
    :param action_toptics: as per request_topics but used for action requests
    :param reply_topic: topic to listen on for responses (a unique consumer group id will be created)
    :param optional group_id: kafka group id to use when subscribing to reply_topic
    :param optional serialiser: serialiser to use (will use DefaultSerialiser if not set)
    """

    def __init__(self, kafka_broker_url, request_topics, action_topics, reply_topic, group_id=None, serialiser=None):
        self._kafka_broker_url = kafka_broker_url
        self._requests = {}

        self._request_topics = request_topics
        self._action_topics = action_topics
        self._reply_topic = reply_topic
        self._group_id = group_id
        self._serialiser = serialiser if serialiser is not None else DefaultSerialiser()

        self._producer = KafkaProducer(bootstrap_servers=[kafka_broker_url])

        self._consumer = KafkaConsumer(
            self._reply_topic,
            bootstrap_servers=[self._kafka_broker_url],
            auto_offset_reset='earliest',
            group_id=self._group_id)

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
    def _try_get_topic_for(pipeline: PipelineBuilder, topics, topic=None):
        if topic is not None:
            return topic

        destination = pipeline.get_inital_destination()
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

    def _get_action_topic(self, pipeline: PipelineBuilder, topic=None):
        topic = self._try_get_topic_for(pipeline, self._action_topics, topic)
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

        self._producer.send(topic=topic, key=key, value=val)
        self._producer.flush()

        return future

    def _submit_action(self, task_id, action, topic):
        task_action = TaskActionRequest(id=task_id, action=action, reply_topic=self._reply_topic)
        return self._submit_request(task_action, topic)

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

    def get_status(self, pipeline: PipelineBuilder, topic=None) -> Future:
        """
        Get the status of a previously submitted pipeline

        :param pipeline: the pipeline
        :param optional topic: override the default ingress topic
        :return: a Future encapsulating the status of the pipeline
        """
        topic = self._get_action_topic(pipeline, topic)
        return self._submit_action(pipeline.id, TaskAction.GET_STATUS, topic)

    async def get_status_async(self, pipeline: PipelineBuilder, topic=None):
        """
        Get the status of a previously submitted pipeline

        :param pipeline: the pipeline
        :param optional topic: override the default ingress topic
        :return: the status of the pipeline
        """
        return await asyncio.wrap_future(self.get_status(pipeline, topic))
    
    def get_request(self, pipeline: PipelineBuilder, topic=None) -> Future:
        """
        Get the original TaskRequest for a previously submitted pipeline

        :param pipeline: the pipeline
        :param optional topic: override the default ingress topic
        :return: a Future encapsulating the original TaskRequest
        """
        topic = self._get_action_topic(pipeline, topic)
        return self._submit_action(pipeline.id, TaskAction.GET_REQUEST, topic)

    async def get_request_async(self, pipeline: PipelineBuilder, topic=None):
        """
        Get the original TaskRequest for a previously submitted pipeline

        :param pipeline: the pipeline
        :param optional topic: override the default ingress topic
        :return: the original TaskRequest
        """
        return await asyncio.wrap_future(self.get_request(pipeline, topic))

    def get_result(self, pipeline: PipelineBuilder, topic=None) -> Future:
        """
        Get the TaskResult for a previously submitted pipeline

        :param pipeline: the pipeline
        :param optional topic: override the default ingress topic
        :return: a Future encapsulating the original TaskResult
        """
        topic = self._get_action_topic(pipeline, topic)
        return self._submit_action(pipeline.id, TaskAction.GET_RESULT, topic)

    async def get_result_async(self, pipeline: PipelineBuilder, topic=None):
        """
        Get the TaskResult for a previously submitted pipeline

        :param pipeline: the pipeline
        :param optional topic: override the default ingress topic
        :return: the original TaskResult
        """
        return await asyncio.wrap_future(self.get_result(pipeline, topic))

    def _consume(self):
        while True:
            try:
                for message in self._consumer:

                    _log.info(f'Message received - {message}')

                    any = Any()
                    any.ParseFromString(message.value)

                    if any.Is(TaskException.DESCRIPTOR):
                        self._raise_exception(any, TaskException)
                    elif any.Is(TaskResult.DESCRIPTOR):
                        self._return_result(any, TaskResult)
                    elif any.Is(TaskActionException.DESCRIPTOR):
                        self._raise_exception(any, TaskActionException)
                    elif any.Is(TaskActionResult.DESCRIPTOR):
                        self._return_action_result(any, TaskActionResult)

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
                    future.set_result(TaskStatus(self._unpack(proto.result, TaskStatusProto).status))
                
                elif proto.action == TaskAction.GET_REQUEST:
                    future.set_result(self._unpack(proto.result, TaskRequest))

                elif proto.action == TaskAction.GET_RESULT:
                    future.set_result(self._unpack(proto.result, TaskResult))

                else:
                    raise ValueError(f'Unsupported action {TaskAction.Name(proto.action)}')
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
    def get_client(kafka_broker_url, request_topics: dict, action_topics: dict, reply_topic, serialiser=None) -> FlinkTasksClient:
        """
        Creates a FlinkTasksClient for submitting tasks to flink.  Clients are memoized by broker url and reply topic.

        :param kafka_broker_url: url of the kafka broker used for ingress and egress
        :param request_topics: dictionary of worker to ingress topic mappings  (use None for default)
                              e.g. {'example/worker': 'example.requests', None: 'example.default.requests'}
        :param action_toptics: as per request_topics but used for action requests
        :param reply_topic: topic to listen on for responses (a unique consumer group id will be created)
        :param optional serialiser: serialiser to use (will use DefaultSerialiser if not set)
        """

        key = f'{kafka_broker_url}.{reply_topic}'

        if key not in FlinkTasksClientFactory.__clients:
            client = FlinkTasksClient(kafka_broker_url, request_topics, action_topics, reply_topic, serialiser=serialiser, group_id=str(uuid4()))
            FlinkTasksClientFactory.__clients[key] = client

        return FlinkTasksClientFactory.__clients[key]
