import asyncio
import unittest

from tests.utils import TestHarness, tasks

_started = []
_finished = []
_pipelines = []


@tasks.events.on_task_started
def on_task_started(context, task_request):
    _started.append(task_request.id)


@tasks.events.on_task_finished
def on_task_finished(context, task_result=None, task_exception=None):
    if task_result is not None:
        _finished.append(task_result.id)
    else:
        _finished.append(task_result.id)


@tasks.events.on_pipeline_created
def on_pipeline_created(context, pipeline):
    _pipelines.append(context.get_task_id())


@tasks.bind()
def hello_workflow(first_name, last_name):
    return tasks.send(_say_hello, first_name, last_name)


@tasks.bind()
def hello_and_goodbye_workflow(first_name, last_name):
    return tasks.send(_say_hello, first_name, last_name).continue_with(_say_goodbye, goodbye_message="see you later!")


@tasks.bind()
def _say_hello(first_name, last_name):
    return f'Hello {first_name} {last_name}'


@tasks.bind()
async def _say_goodbye(greeting, goodbye_message):
    await asyncio.sleep(0)
    return f'{greeting}.  So now I will say {goodbye_message}'


class EventsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.test_harness = TestHarness()

    def test_pipeline_tasks_start_and_end(self):
        pipeline = tasks.send(hello_workflow, 'Jane', 'Doe')
        self.test_harness.run_pipeline(pipeline)

        self.assertIn(pipeline.id, _started)
        self.assertIn(pipeline.id, _finished)

        for _, _, task_id in pipeline.get_tasks():
            self.assertIn(task_id, _started)
            self.assertIn(task_id, _finished)

    def test_pipelines_are_created(self):
        pipeline = tasks.send(hello_workflow, 'Jane', 'Doe')
        self.test_harness.run_pipeline(pipeline)

        self.assertIn(pipeline.id, _pipelines)


if __name__ == '__main__':
    unittest.main()
