import unittest
from dataclasses import dataclass

from google.protobuf.message import Message

from statefun_tasks.protobuf import ObjectProtobufConverter
from tests.test_messages_pb2 import HelloWorkflowInputMessage, HelloWorkflowResultMessage
from tests.utils import FlinkTestHarness, tasks, test_harness_serialiser


@dataclass
class HelloWorkflowInput:
    first_name: str
    last_name: str


@dataclass
class HelloWorkflowResult:
    message: str


class HelloWorkflowInputProtobufConverter(ObjectProtobufConverter[HelloWorkflowInputMessage]):
    def can_convert_to_proto(self, obj: object) -> bool:
        return type(obj) == HelloWorkflowInput

    def can_convert_from_proto(self, message: Message) -> bool:
        return type(message) == HelloWorkflowInput

    def convert_to_proto(self, obj: HelloWorkflowInput) -> HelloWorkflowInputMessage:
        message = HelloWorkflowInputMessage()
        message.first_name = obj.first_name
        message.last_name = obj.last_name
        return message

    def convert_from_proto(self, message: HelloWorkflowInputMessage) -> object:
        return HelloWorkflowInput(message.first_name, message.last_name)


class HelloWorkflowResultProtobufConverter(ObjectProtobufConverter[HelloWorkflowResultMessage]):
    def can_convert_to_proto(self, obj: object) -> bool:
        return type(obj) == HelloWorkflowResult

    def can_convert_from_proto(self, message: Message) -> bool:
        return type(message) == HelloWorkflowResultMessage

    def convert_to_proto(self, obj: HelloWorkflowResult) -> HelloWorkflowResultMessage:
        message = HelloWorkflowResultMessage()
        message.message = obj.message
        return message

    def convert_from_proto(self, message: HelloWorkflowResultMessage) -> object:
        return HelloWorkflowResult(message.message)


@tasks.bind()
def hello_workflow(workflow_input: HelloWorkflowInput):
    return HelloWorkflowResult(f'Hello {workflow_input.first_name} {workflow_input.last_name}')


@tasks.bind(with_state=True)
def hello_workflow_with_state(state, workflow_input: HelloWorkflowInput):
    state = state or {}
    pipeline = initialise_state.send(workflow_input).continue_with(extract_result_from_state)
    return state, pipeline


@tasks.bind(with_state=True)
def initialise_state(_, workflow_input: HelloWorkflowInput):
    return {'result': HelloWorkflowResult(f'Hello from state {workflow_input.first_name} {workflow_input.last_name}')}


@tasks.bind(with_state=True)
def extract_result_from_state(state):
    return state, state['result']


class PipelineWithProtobufConversionTests(unittest.TestCase):

    def setUp(self):
        test_harness_serialiser.register_converters(
            [HelloWorkflowInputProtobufConverter(), HelloWorkflowResultProtobufConverter()])
        test_harness_serialiser.register_proto_types([HelloWorkflowInputMessage, HelloWorkflowResultMessage])

    def test_hello_workflow(self):
        test_harness = FlinkTestHarness()
        result = test_harness.run_pipeline(hello_workflow.send(HelloWorkflowInput('John', 'Smith')))
        self.assertEqual(result, HelloWorkflowResult('Hello John Smith'))

    def test_hello_workflow_from_state(self):
        test_harness = FlinkTestHarness()

        result = test_harness.run_pipeline(hello_workflow_with_state.send(HelloWorkflowInput('John', 'Smith')))
        self.assertEqual(result, HelloWorkflowResult('Hello from state John Smith'))


if __name__ == '__main__':
    unittest.main()
