import unittest
from statefun_tasks.core.statefun.request_reply_pb2 import Address
from statefun_tasks.types import Task, Group
from statefun_tasks import DefaultSerialiser, RetryPolicy


class PipelineSerialisationTests(unittest.TestCase):
    def test_task_entry_serialisation(self):
        serialiser = DefaultSerialiser(known_proto_types=[Address])

        args = (1,'2', Address(namespace='test'))
        kwargs = {'arg': [1, 2, 3]}
        parameters = {'is_fruitful': True}

        entry = Task.from_fields('task_id', 'task_type', args, kwargs, **parameters, is_finally=True)

        entry_proto = entry.to_proto(serialiser)
        reconsituted_entry = Task.from_proto(entry_proto).unpack(serialiser)

        self.assertEqual(entry_proto.task_entry.request.type_url, 'type.googleapis.com/statefun_tasks.ArgsAndKwargs')
        self.assertEqual(reconsituted_entry.task_id, entry.task_id)
        self.assertEqual(reconsituted_entry.task_type, entry.task_type)
        self.assertEqual(reconsituted_entry.is_fruitful, True)
        self.assertEqual(reconsituted_entry.is_finally, True)
        self.assertEqual(reconsituted_entry.to_tuple(), entry.to_tuple())

    def test_task_entry_serialisation_with_single_protobuf_arg(self):
        serialiser = DefaultSerialiser(known_proto_types=[Address])

        args = Address(namespace='test')
        entry = Task.from_fields('task_id', 'task_type', args, {}, True)

        entry_proto = entry.to_proto(serialiser)
        reconsituted_entry = Task.from_proto(entry_proto).unpack(serialiser)

        self.assertEqual(entry_proto.task_entry.request.type_url, 'type.googleapis.com/io.statefun.sdk.reqreply.Address')
        self.assertEqual(reconsituted_entry.to_tuple(), entry.to_tuple())

    def test_group_entry_serialisation(self):
        serialiser = DefaultSerialiser(known_proto_types=[Address])

        args = (1,'2', Address(namespace='test'))
        kwargs = {'arg': [1, 2, 3]}
  
        group_entry = Group(group_id='inner_group_id', max_parallelism=10)

        group_entry.add_to_group([
            Task.from_fields('inner_task_id_1', 'task_type', args, kwargs),
            Task.from_fields('inner_task_id_2', 'task_type', args, kwargs)
        ])

        entry = Group(group_id='group_id')
        entry.add_to_group([
            group_entry,
            Task.from_fields('grouped_task_chain_1_1', 'task_type', args, kwargs),
            Task.from_fields('grouped_task_chain_1_2', 'task_type', args, kwargs)
        ])

        entry.add_to_group([
            Task.from_fields('grouped_task_chain_2_1', 'task_type', args, kwargs),
        ])

        proto = entry.to_proto(serialiser)
        reconsituted_entry = Group.from_proto(proto)
        self.assertEqual(str(reconsituted_entry), str(entry))

    def test_task_entry_serialisation_with_task_retry_policy(self):
        serialiser = DefaultSerialiser(known_proto_types=[Address])

        args = ()
        kwargs = {}
        retry_policy = RetryPolicy(retry_for=[Exception, ValueError]).to_proto()

        entry = Task.from_fields('task_id', 'task_type', args, kwargs, retry_policy=retry_policy)

        entry_proto = entry.to_proto(serialiser)
        reconsituted_entry = Task.from_proto(entry_proto).unpack(serialiser)
        retry_policy = reconsituted_entry.retry_policy
        self.assertEqual(['builtins.Exception', 'builtins.ValueError'], retry_policy.retry_for)
