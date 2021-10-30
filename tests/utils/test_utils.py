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
        state_item.state_value.CopyFrom(value)
