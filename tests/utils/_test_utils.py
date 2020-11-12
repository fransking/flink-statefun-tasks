from google.protobuf.any_pb2 import Any
from statefun.request_reply_pb2 import Address


def update_address(address: Address, namespace, address_type, address_id):
    address.namespace = namespace
    address.type = address_type
    address.id = address_id


def update_state(state, name, value):
    state_item = next((item for item in state if item.state_name == name), None)
    if state_item is None:
        state_item = state.add()
    state_item.state_name = name
    if value:
        if isinstance(value, bytes):
            state_item.state_value = value
        else:
            any = Any()
            any.Pack(value)
            state_item.state_value = any.SerializeToString()


def unpack_any(any_: Any, known_types):
    for cls in known_types:
        if any_.Is(cls.DESCRIPTOR):
            instance = cls()
            any_.Unpack(instance)
            return instance
    raise ValueError(f'Any {any_} is not an instance of {", ".join(known_types)}.')
