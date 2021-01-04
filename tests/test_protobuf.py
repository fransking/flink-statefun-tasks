import unittest

from statefun.request_reply_pb2 import Address

from statefun_tasks import TaskRequest, TaskEntry
from statefun_tasks._protobuf import _convert_from_proto, _convert_to_proto


class ProtobufTests(unittest.TestCase):
    def test_copy_statefun_address_into_task_request(self):
        address = Address(namespace="tests", type="test", id="id")
        task_request = TaskRequest()
        task_request.reply_address.ParseFromString(address.SerializeToString())
        self.assertEqual(task_request.reply_address.namespace, address.namespace)                
        self.assertEqual(task_request.reply_address.type, address.type)
        self.assertEqual(task_request.reply_address.id, address.id)


    def test_convert_dict_to_protofbuf(self):
        data = {
            'int': 123,
            'float': 1.23,
            'str': '123',
            'list': [1,2,3],
            'dict': {
                'a': 1,
                'b': 2
            },
            'dict_in_list': [1, {'a': 1}],
            'proto': Address(namespace="tests", type="test", id="id")
        }

        proto = _convert_to_proto(data)
        reconsituted_data = _convert_from_proto(proto, known_proto_types=[Address])
        
        self.assertEqual(reconsituted_data['int'], 123)
        self.assertEqual(reconsituted_data['float'], 1.23)
        self.assertEqual(reconsituted_data['str'], '123')
        self.assertEqual(reconsituted_data['list'], [1,2,3])
        self.assertEqual(reconsituted_data['dict']['a'], 1)
        self.assertEqual(reconsituted_data['dict']['b'], 2)
        self.assertEqual(reconsituted_data['dict_in_list'][0], 1)
        self.assertEqual(reconsituted_data['dict_in_list'][1]['a'], 1)
        self.assertTrue(isinstance(reconsituted_data['proto'], Address))
        self.assertEqual(reconsituted_data['proto'].namespace, 'tests')
        self.assertEqual(reconsituted_data['proto'].type, 'test')
        self.assertEqual(reconsituted_data['proto'].id, 'id')

    def test_convert_list_to_protofbuf(self):
        data = [
            Address(namespace="tests", type="test", id="id"),
            1,
            '123'
        ]

        proto = _convert_to_proto(data)
        reconsituted_data = _convert_from_proto(proto, known_proto_types=[Address])

        self.assertTrue(isinstance(reconsituted_data[0], Address))
        self.assertEqual(reconsituted_data[0].namespace, 'tests')
        self.assertEqual(reconsituted_data[0].type, 'test')
        self.assertEqual(reconsituted_data[0].id, 'id')
        self.assertEqual(reconsituted_data[1], 1)
        self.assertEqual(reconsituted_data[2], '123')


if __name__ == '__main__':
    unittest.main()
