import asyncio
import unittest

from statefun_tasks import DefaultSerialiser
from tests.test_messages_pb2 import Person, GreetingRequest, GreetingResponse
from tests.utils import FlinkTestHarness, tasks, other_tasks_instance, TaskErrorException


@tasks.bind()
def hello_workflow(first_name, last_name):
    return tasks.send(_say_hello, first_name, last_name)


@tasks.bind()
def hello_and_goodbye_workflow(first_name, last_name):
    return tasks.send(_say_hello, first_name, last_name).continue_with(_say_goodbye, goodbye_message="see you later!")


@tasks.bind()
def simple_protobuf_workflow(person: Person):
    return tasks.send(_create_greeting, person).continue_with(_greet_person)


@tasks.bind()
def _create_greeting(person: Person) -> GreetingRequest:
    return GreetingRequest(person=person, message='Hello')


@tasks.bind()
def _greet_person(greeting: GreetingRequest) -> GreetingResponse:
    return GreetingResponse(greeting=f'{greeting.message} {greeting.person.first_name} {greeting.person.last_name}')


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


@tasks.bind(with_context=True)
async def _return_display_name(context):
    return context.get_display_name()


class SimplePipelineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.test_harness = FlinkTestHarness()

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
        pipeline = tasks.send(simple_protobuf_workflow, Person(first_name='Jane', last_name='Doe'))
        proto = pipeline.to_proto(serialiser=DefaultSerialiser())

        self.assertEqual(proto.entries[0].task_entry.request.type_url, 'type.googleapis.com/tests.Person')

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

    def test_pipeline_with_non_fruitful_override(self):
        pipeline = _say_hello.send('A', 'B').set(is_fruitful=False)

        result = self.test_harness.run_pipeline(pipeline)
        self.assertIsNone(result)

    def test_pipeline_without_display_name_set(self):
        pipeline = _return_display_name.send()

        result = self.test_harness.run_pipeline(pipeline)
        self.assertEqual(result, f'{__name__}._return_display_name')

    def test_pipeline_with_display_name_set(self):
        pipeline = _return_display_name.send().set(display_name='test pipeline')

        result = self.test_harness.run_pipeline(pipeline)
        self.assertEqual(result, 'test pipeline')

    def test_pipeline_with_conditional_continuation(self):
        pipeline = tasks.send(_say_hello, 'Jane', 'Doe').continue_if(False, _say_goodbye, goodbye_message="see you later!")
        result = self.test_harness.run_pipeline(pipeline)
        self.assertEqual(result, 'Hello Jane Doe')

    def test_pipeline_re_run(self):
        pipeline = tasks.send(hello_workflow, 'Jane', 'Doe')

        result = self.test_harness.run_pipeline(pipeline)
        self.assertEqual(result, 'Hello Jane Doe')

        result = self.test_harness.run_pipeline(pipeline)
        self.assertEqual(result, 'Hello Jane Doe')

    def test_pipeline_re_run_when_not_complete(self):
        pipeline = tasks.send(_say_hello, 'Jane', 'Doe').wait().continue_with(_say_goodbye, goodbye_message="see you later!")

        self.test_harness.run_pipeline(pipeline)
        
        try:
            self.test_harness.run_pipeline(pipeline)
        except TaskErrorException as e:
            self.assertEqual(str(e.task_error.message), 'Pipelines must have finished before they can be re-run')
        else:
            self.fail('Expected a TaskErrorException')

    def test_pipeline_composition(self):
        pipeline = tasks.send(_say_hello, 'Jane', 'Doe')
        continuation = tasks.send(_say_goodbye, goodbye_message="see you later!")
        continuation.append_to(pipeline)

        result = self.test_harness.run_pipeline(pipeline)
        self.assertEqual(result, 'Hello Jane Doe.  So now I will say see you later!')

    def test_pipeline_composition_fails_if_initial_parameters_are_set(self):
        pipeline = tasks.send(_say_hello, 'Jane', 'Doe')
        continuation = tasks.send(_say_goodbye, goodbye_message="see you later!").with_initial(state=1)

        try:
            continuation.append_to(pipeline)
        except ValueError as ex:
            self.assertEqual(str(ex), 'Ambiguous initial parameters')
        else:
            self.fail('Expected an exception')


if __name__ == '__main__':
    unittest.main()
