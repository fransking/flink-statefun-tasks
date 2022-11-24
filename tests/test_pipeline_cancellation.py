import unittest

from statefun_tasks import in_parallel, YieldTaskInvocation, TaskAction
from tests.utils import TestHarness, tasks
from uuid import uuid4


_tasks_started = []


@tasks.events.on_task_started
def on_task_started(context, task_request):
    _tasks_started.append(task_request.id)


@tasks.bind(with_context=True)
def _yield(context):
    task_request = tasks.clone_task_request(context)
    context.set_state(task_request)
    raise YieldTaskInvocation()


@tasks.bind(with_context=True)
async def _resume(context):
    task_request = context.get_state()
    _, _, state = tasks.unpack_task_request(task_request)
    await tasks.send_result(context, task_request, (), state)


@tasks.bind()
def _task():
    return True


class PipelineCancellationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.test_harness = TestHarness()

    def test_cancelling_an_in_parallel(self):
        task_id = str(uuid4())

        # first task will yield, second is not deferred
        pipeline = in_parallel([_yield.send().set(task_id=task_id), _task.send()])
        self.test_harness.run_pipeline(pipeline)

        # cancel the pipeline
        self.test_harness.run_action(pipeline, TaskAction.CANCEL_PIPELINE)

        # resume invocation of yield task
        self.test_harness.run_pipeline(_resume.send().set(task_id=task_id))

        # assert that the second task in the pipeline does not get called because the pipeline is cancelled
        tasks_id = [task_id for _, _, task_id in pipeline.get_tasks()]
        self.assertIn(tasks_id[0], _tasks_started)
        self.assertIn(tasks_id[-1], _tasks_started)

    def test_cancelling_an_in_parallel_with_deferred_tasks(self):
        task_id = str(uuid4())

        # first task will yield, second is deferred
        pipeline = in_parallel([_yield.send().set(task_id=task_id), _task.send()], max_parallelism=1)
        self.test_harness.run_pipeline(pipeline)

        # cancel the pipeline
        self.test_harness.run_action(pipeline, TaskAction.CANCEL_PIPELINE)

        # resume invocation of yield task
        self.test_harness.run_pipeline(_resume.send().set(task_id=task_id))

        # assert that the second task in the pipeline does not get called because the pipeline is cancelled
        tasks_id = [task_id for _, _, task_id in pipeline.get_tasks()]
        self.assertIn(tasks_id[0], _tasks_started)
        self.assertNotIn(tasks_id[-1], _tasks_started)


if __name__ == '__main__':
    unittest.main()
