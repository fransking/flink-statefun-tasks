import unittest

from statefun_tasks import TaskRequest
from statefun.request_reply_pb2 import Address


class ProtobufTests(unittest.TestCase):
    def test_copy_statefun_address_into_task_request(self):
        address = Address(namespace="tests", type="test", id="id")
        task_request = TaskRequest()
        task_request.reply_address.ParseFromString(address.SerializeToString())
        self.assertEqual(task_request.reply_address.namespace, address.namespace)                
        self.assertEqual(task_request.reply_address.type, address.type)
        self.assertEqual(task_request.reply_address.id, address.id)



if __name__ == '__main__':
    unittest.main()
