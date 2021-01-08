import unittest

from statefun.request_reply_pb2 import Address

from statefun_tasks._types import _TaskEntry, _GroupEntry
from statefun_tasks import DefaultSerialiser, RetryPolicy

class PipelineSerialisationTests(unittest.TestCase):
    def test_task_entry_serialisation(self):
        serialiser = DefaultSerialiser(known_proto_types=[Address])

        args = (1,'2', Address(namespace='test'))
        kwargs = {'arg': [1, 2, 3]}
        parameters = {'a_parameter': 'some_value'}

        entry = _TaskEntry('task_id', 'task_type', args, kwargs, parameters, True)
        entry.mark_complete()

        entry_proto = entry.to_proto(serialiser)
        reconsituted_entry = _TaskEntry.from_proto(entry_proto, serialiser)

        self.assertEqual(reconsituted_entry.task_id, entry.task_id)
        self.assertEqual(reconsituted_entry.task_type, entry.task_type)
        self.assertEqual(reconsituted_entry.args, tuple(entry.args,))
        self.assertEqual(reconsituted_entry.kwargs, kwargs)
        self.assertEqual(reconsituted_entry.parameters, parameters)
        self.assertEqual(reconsituted_entry.is_finally, True)
        self.assertEqual(reconsituted_entry.is_complete(), True)

    def test_group_entry_serialisation(self):
        serialiser = DefaultSerialiser(known_proto_types=[Address])

        args = (1,'2', Address(namespace='test'))
        kwargs = {'arg': [1, 2, 3]}
  
        group_entry = _GroupEntry(group_id='inner_group_id')

        group_entry.add_to_group([
            _TaskEntry('inner_task_id_1', 'task_type', args, kwargs),
            _TaskEntry('inner_task_id_2', 'task_type', args, kwargs)
        ])

        entry = _GroupEntry(group_id='group_id')
        entry.add_to_group([
            group_entry,
            _TaskEntry('grouped_task_chain_1_1', 'task_type', args, kwargs),
            _TaskEntry('grouped_task_chain_1_2', 'task_type', args, kwargs)
        ])

        entry.add_to_group([
            _TaskEntry('grouped_task_chain_2_1', 'task_type', args, kwargs)
        ])

        proto = entry.to_proto(serialiser)
        reconsituted_entry = _GroupEntry.from_proto(proto, serialiser)
        self.assertEqual(str(reconsituted_entry), str(entry))

    def test_task_entry_serialisation_with_task_retry_policy(self):
        serialiser = DefaultSerialiser(known_proto_types=[Address])

        args = ()
        kwargs = {}
        parameters = {'retry_policy': RetryPolicy(retry_for=[Exception, ValueError]).to_proto()}

        entry = _TaskEntry('task_id', 'task_type', args, kwargs, parameters)

        entry_proto = entry.to_proto(serialiser)
        reconsituted_entry = _TaskEntry.from_proto(entry_proto, serialiser)
        retry_policy = reconsituted_entry.get_parameter('retry_policy')
        self.assertEqual(['builtins.Exception', 'builtins.ValueError'], retry_policy.retry_for)


if __name__ == '__main__':
    unittest.main()
