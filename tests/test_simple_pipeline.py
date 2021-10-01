import asyncio
import unittest

from statefun_tasks import DefaultSerialiser
from tests.test_messages_pb2 import TestPerson, TestGreetingRequest, TestGreetingResponse
from tests.utils import TestHarness, tasks, other_tasks_instance, TaskErrorException


@tasks.bind()
def hello_workflow(first_name, last_name):
    return tasks.send(_say_hello, first_name, last_name)


@tasks.bind()
def hello_and_goodbye_workflow(first_name, last_name):
    return tasks.send(_say_hello, first_name, last_name).continue_with(_say_goodbye, goodbye_message="see you later!")


@tasks.bind()
def simple_protobuf_workflow(person: TestPerson):
    return tasks.send(_create_greeting, person).continue_with(_greet_person)


@tasks.bind()
def _create_greeting(person: TestPerson) -> TestGreetingRequest:
    return TestGreetingRequest(person=person, message='Hello')


@tasks.bind()
def _greet_person(greeting: TestGreetingRequest) -> TestGreetingResponse:
    return TestGreetingResponse(greeting=f'{greeting.message} {greeting.person.first_name} {greeting.person.last_name}')


@tasks.bind()
def _say_hello(first_name, last_name):
    return f'Hello {first_name} {last_name}'


@tasks.bind()
async def _say_goodbye(greeting, goodbye_message):
    await asyncio.sleep(0)
    return f'{greeting}.  So now I will say {goodbye_message}'


@tasks.bind(is_fruitful=False)
async def _non_fruitful_task():
    return 1


class SimplePipelineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.test_harness = TestHarness()

    def test_pipeline(self):
        pipeline = tasks.send(hello_workflow, 'Jane', 'Doe')
        result = self.test_harness.run_pipeline(pipeline)
        self.assertEqual(result, 'Hello Jane Doe')

    def test_pipeline_using_kwargs(self):
        pipeline = tasks.send(hello_workflow, first_name='Jane', last_name='Doe')
        proto = pipeline.to_proto(serialiser=DefaultSerialiser())
        self.assertEqual(proto.entries[0].task_entry.request.type_url,
                         'type.googleapis.com/statefun_tasks.ArgsAndKwargs')

        result = self.test_harness.run_pipeline(pipeline)
        self.assertEqual(result, 'Hello Jane Doe')

    def test_pipeline_with_continuation(self):
        pipeline = tasks.send(hello_and_goodbye_workflow, 'Jane', last_name='Doe')
        result = self.test_harness.run_pipeline(pipeline)
        self.assertEqual(result, 'Hello Jane Doe.  So now I will say see you later!')

    def test_simple_protobuf_pipeline(self):
        pipeline = tasks.send(simple_protobuf_workflow, TestPerson(first_name='Jane', last_name='Doe'))
        proto = pipeline.to_proto(serialiser=DefaultSerialiser())

        self.assertEqual(proto.entries[0].task_entry.request.type_url, 'type.googleapis.com/tests.TestPerson')

        result = self.test_harness.run_pipeline(pipeline)
        self.assertEqual(result.greeting, 'Hello Jane Doe')

    def test_unregistered_function(self):
        @other_tasks_instance.bind()
        def new_function():
            return 'Hello'

        pipeline = tasks.send(new_function, 'Jane', 'Doe')
        try:
            self.test_harness.run_pipeline(pipeline)
        except TaskErrorException as e:
            self.assertEqual(str(e.task_error.message), f'{__name__}.new_function is not a registered FlinkTask')
        else:
            self.fail('Expected a TaskErrorException')

    def test_pipeline_with_unknown_namespace(self):
        pipeline = tasks.send(hello_workflow, 'Jane', 'Doe').set(namespace='unknown')

        try:
            self.test_harness.run_pipeline(pipeline)
        except KeyError as e:
            self.assertEqual(str(e.args[0]), 'unknown/worker')
        else:
            self.fail('Expected an exception')

    def test_pipeline_with_unknown_worker_name(self):
        pipeline = tasks.send(hello_workflow, 'Jane', 'Doe').set(worker_name='unknown')

        try:
            self.test_harness.run_pipeline(pipeline)
        except KeyError as e:
            self.assertEqual(str(e.args[0]), 'test/unknown')
        else:
            self.fail('Expected an exception')

    def test_non_fruitful_pipeline(self):
        pipeline = _non_fruitful_task.send()

        result = self.test_harness.run_pipeline(pipeline)
        self.assertIsNone(result)


if __name__ == '__main__':
    unittest.main()
