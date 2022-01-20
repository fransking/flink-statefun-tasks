import asyncio
import unittest

from statefun_tasks import in_parallel
from tests.utils import TestHarness, tasks


_started = []


@tasks.events.on_task_started
def on_task_started(context, task_request):
    _started.append(task_request.id)


@tasks.bind(with_context=True)
def hello_workflow(context, first_name, last_name):
    return _say_hello.send(first_name, last_name).set(task_id=f'{context.get_pipeline_id()}.stateful_hello')


@tasks.bind(with_context=True)
def _say_hello(context, first_name, last_name):
    called = context.get_state() or False

    if called:
        return f'Hello {first_name} {last_name} again'
    else:
        context.set_state(True)
        return f'Hello {first_name} {last_name}'


class StatefulPipelineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.test_harness = TestHarness()

    def test_simple_stateful_pipeline(self):

        pipeline = in_parallel([
            hello_workflow.send('Jane', 'Doe'),
            hello_workflow.send('Jane', 'Doe')
        ])

        pipeline.id = 'StatefulPipelineTests.test_simple_stateful_pipeline'

        result = self.test_harness.run_pipeline(pipeline)
        self.assertEqual(result, ['Hello Jane Doe', 'Hello Jane Doe again'])
        self.assertIn('StatefulPipelineTests.test_simple_stateful_pipeline', _started)
        self.assertIn('StatefulPipelineTests.test_simple_stateful_pipeline.stateful_hello', _started)

if __name__ == '__main__':
    unittest.main()
