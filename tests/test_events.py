import unittest

from tests.utils import TestHarness, tasks

_received = []
_started = []
_finished = []
_pipelines = []
_pipeline_tasks_finished = []
_pipelines_finished = []
_emitted = []


@tasks.events.on_task_received
def on_task_received(context, task_request):
    _received.append(task_request.id)


@tasks.events.on_task_started
def on_task_started(context, task_request):
    _started.append(task_request.id)


@tasks.events.on_task_finished
def on_task_finished(context, task_result=None, task_exception=None, is_pipeline=False):
    if task_result is not None:
        _finished.append(task_result.id)
    else:
        _finished.append(task_exception.id)


@tasks.events.on_pipeline_created
def on_pipeline_created(context, pipeline):
    _pipelines.append(context.get_task_id())


@tasks.events.on_pipeline_task_finished
def on_pipeline_task_finished(context, task_result=None, task_exception=None):
    if task_result is not None:
        _pipeline_tasks_finished.append(task_result.id)
    else:
        _pipeline_tasks_finished.append(task_exception.id)


@tasks.events.on_pipeline_finished
def on_pipeline_finished(context, pipeline, task_result=None, task_exception=None):
    if task_result is not None:
        _pipelines_finished.append(context.get_task_id())
    else:
        _pipelines_finished.append(context.get_task_id())


@tasks.events.on_emit_result
def on_emit_result(context, task_result=None, task_exception=None):
    if task_result is not None:
        _emitted.append(task_result.id)
    else:
        _emitted.append(task_exception.id)

@tasks.bind()
def hello_workflow(first_name, last_name):
    return tasks.send(_say_hello, first_name, last_name)


@tasks.bind()
def _say_hello(first_name, last_name):
    return f'Hello {first_name} {last_name}'


@tasks.bind()
async def _say_goodbye(greeting, goodbye_message):
    return f'{greeting}.  So now I will say {goodbye_message}'


class EventsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.test_harness = TestHarness()

    def test_pipeline_tasks_start_and_end(self):
        pipeline = tasks.send(_say_hello, 'Jane', 'Doe').continue_with(_say_goodbye, goodbye_message="see you later!")
        self.test_harness.run_pipeline(pipeline)

        self.assertIn(pipeline.id, _started)
        self.assertIn(pipeline.id, _finished)
        self.assertIn(pipeline.id, _emitted)

        for _, _, task_id in pipeline.get_tasks():
            self.assertIn(task_id, _received)
            self.assertIn(task_id, _started)
            self.assertIn(task_id, _finished)
            self.assertIn(task_id, _pipeline_tasks_finished)
            self.assertIn(task_id, _emitted)
            
    def test_pipelines_are_created(self):
        pipeline = tasks.send(hello_workflow, 'Jane', 'Doe')
        self.test_harness.run_pipeline(pipeline)

        self.assertIn(pipeline.id, _pipelines)

    def test_pipelines_finish(self):
        pipeline = tasks.send(hello_workflow, 'Jane', 'Doe')
        self.test_harness.run_pipeline(pipeline)

        self.assertIn(pipeline.id, _pipelines_finished)


if __name__ == '__main__':
    unittest.main()
