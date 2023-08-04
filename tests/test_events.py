import unittest
from tests.utils import TaskRunner
from statefun_tasks import FlinkTasks


tasks = FlinkTasks()


received = []
started = []
completed = []
failed = []


@tasks.bind()
def _task():
    return True


@tasks.bind()
def _failing_task():
    raise ValueError()


@tasks.events.on_task_received
def on_task_received(context, task_request):
    received.append(task_request.id)


@tasks.events.on_task_started
def on_task_started(context, task_request):
    started.append(task_request.id)


@tasks.events.on_task_finished
def on_task_finished(context, task_result=None, task_exception=None, is_pipeline=False):
    if task_result is not None:
        completed.append(task_result.id)
    else:
        failed.append(task_exception.id)


class EventsTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.runner = TaskRunner(tasks)

    async def test_events_are_sent(self):
        await self.runner.run_task(_task)
        self.assertEqual(len(received), 1)
        self.assertEqual(len(started), 1)
        self.assertEqual(len(completed), 1)

        try:
            await self.runner.run_task(_failing_task)
        except:
            ()
            
        self.assertEqual(len(received), 2)
        self.assertEqual(len(started), 2)
        self.assertEqual(len(failed), 1)
