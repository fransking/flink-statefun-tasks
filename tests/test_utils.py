import unittest
from uuid import UUID

from statefun_tasks.utils import gen_id, id_to_uuid


_FIXED_CONVERSIONS = [
    ('AAAAAAAAAAAAAAAAAAAAAA', '00000000-0000-0000-0000-000000000000'),
    ('_____________________w', 'ffffffff-ffff-ffff-ffff-ffffffffffff'),
    ('VQ6EAOKbQdSnFkRmVUQAAA', '550e8400-e29b-41d4-a716-446655440000'),
    ('a6e4EJ2tEdGAtADAT9QwyA', '6ba7b810-9dad-11d1-80b4-00c04fd430c8'),
    ('a6e4EZ2tEdGAtADAT9QwyA', '6ba7b811-9dad-11d1-80b4-00c04fd430c8'),
    ('AAAAAAAAAAAAAAAAAAAAAQ', '00000000-0000-0000-0000-000000000001'),
    ('AAAAAQAAAAAAAAAAAAAAAA', '00000001-0000-0000-0000-000000000000'),
    ('QAAAAAAAAAAAAAAAAAAAAA', '40000000-0000-0000-0000-000000000000'),
    ('AAAAAAAAAAAAAAAAAAAAAg', '00000000-0000-0000-0000-000000000002')
]


class UtilsTests(unittest.TestCase):
    def test_gen_id_can_be_reconstructed_as_valid_uuid4(self):
        id_str = gen_id()
        uuid = id_to_uuid(id_str)
        self.assertEqual(uuid.version, 4)

    def test_id_to_uuid_fixed_conversions(self):
        for id_str, expected_uuid in _FIXED_CONVERSIONS:
            with self.subTest(id_str=id_str):
                self.assertEqual(id_to_uuid(id_str), UUID(expected_uuid))

    def test_uuid_to_id_fixed_conversions(self):
        from base64 import urlsafe_b64encode
        for expected_id, uuid_str in _FIXED_CONVERSIONS:
            with self.subTest(uuid_str=uuid_str):
                result = urlsafe_b64encode(UUID(uuid_str).bytes).rstrip(b'=').decode('ascii')
                self.assertEqual(result, expected_id)


if __name__ == '__main__':
    unittest.main()
