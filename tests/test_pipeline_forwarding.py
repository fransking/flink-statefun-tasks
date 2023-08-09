import unittest
from unittest.mock import MagicMock
from statefun_tasks import FlinkTasks, Pipeline, Address, unpack_any, in_parallel
from tests.utils import TaskRunner


tasks = FlinkTasks(default_namespace='test', default_worker_name='worker')


@tasks.bind()
def _task():
    pass


@tasks.bind()
def _workflow():
    return _task.send()


@tasks.bind(is_fruitful=False)
def _non_fruitful_workflow():
    return _task.send()


@tasks.bind()
def _parallel_workflow_in_stages():
    return in_parallel([_task.send(), _task.send()], num_stages=2)


class PipelineForwardingTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.runner = TaskRunner(tasks)

    async def test_nested_pipeline(self):
        messages = []
        context = MagicMock()
        context.send_message = lambda *args: messages.append(args[0:3])

        result, _ = await self.runner.run_task(_workflow, context=context, state='initial', task_meta={'key': 'value'})

        self.assertIsInstance(result, Pipeline)
        self.assertEqual(len(result.entries), 1)
        self.assertEqual(result.entries[0].task_entry.task_type, "tests.test_pipeline_forwarding._task")

        message = [m for m in messages if "embedded" in m[0]]
        self.assertEqual(len(message), 1)
        destination, target_id, pipeline_request = message[0]

        self.assertEqual("test/embedded_pipeline", destination)
        self.assertEqual(target_id, context.storage.task_request.uid)
        self.assertEqual(pipeline_request.invocation_id, context.storage.task_request.invocation_id)
        self.assertTrue(pipeline_request.is_fruitful)
        self.assertEqual(unpack_any(pipeline_request.state, []).value, 'initial')
        self.assertEqual(pipeline_request.meta['key'], 'value')

    async def test_task_creating_a_nested_pipeline_does_not_egress(self):
        egresses = []
        context = MagicMock()
        context.safe_send_egress_message = lambda *args: egresses.append(args)
        await self.runner.run_task(_workflow, context=context, state='initial', reply_topic='test')

        self.assertEqual(len(egresses), 0)

    async def test_task_creating_a_nested_pipeline_fails_if_embedded_pipline_is_not_configured(self):
        with self.assertRaises(ValueError):
            await self.runner.run_task(_workflow, embedded_pipeline_namespace=None, embedded_pipeline_type=None)

    async def test_non_fruitful_nested_pipeline(self):
        messages = []
        context = MagicMock()
        context.send_message = lambda *args: messages.append(args[0:3])

        result, _ = await self.runner.run_task(_non_fruitful_workflow, context=context)

        self.assertIsInstance(result, Pipeline)
        self.assertEqual(len(result.entries), 1)
        self.assertEqual(result.entries[0].task_entry.task_type, "tests.test_pipeline_forwarding._task")

        message = [m for m in messages if "embedded" in m[0]]
        self.assertEqual(len(message), 1)
        destination, target_id, pipeline_request = message[0]

        self.assertEqual("test/embedded_pipeline", destination)
        self.assertEqual(target_id, context.storage.task_request.uid)
        self.assertEqual(pipeline_request.invocation_id, context.storage.task_request.invocation_id)
        self.assertFalse(pipeline_request.is_fruitful)

    async def test_nested_pipeline_has_default_worker_namespace_and_name_set(self):
        messages = []
        context = MagicMock()
        context.send_message = lambda *args: messages.append(args[0:3])

        _, _ = await self.runner.run_task(_workflow, context=context, caller_address="test/runner", caller_id="123")
        _, _, pipeline_request = [m for m in messages if "embedded" in m[0]][0]

        pipeline = unpack_any(pipeline_request.request, [Pipeline])
        self.assertEqual(pipeline.entries[0].task_entry.namespace, 'test')
        self.assertEqual(pipeline.entries[0].task_entry.worker_name, 'worker')

    async def test_nested_pipeline_has_reply_address_from_original_caller(self):
        messages = []
        context = MagicMock()
        context.send_message = lambda *args: messages.append(args[0:3])

        _, _ = await self.runner.run_task(_workflow, context=context, caller_address="test/runner", caller_id="123")
        _, _, pipeline_request = [m for m in messages if "embedded" in m[0]][0]

        self.assertEqual(pipeline_request.reply_address.namespace, "test")
        self.assertEqual(pipeline_request.reply_address.type, "runner")
        self.assertEqual(pipeline_request.reply_address.id, "123")

    async def test_nested_pipeline_has_reply_address_from_task_request(self):
        messages = []
        context = MagicMock()
        context.send_message = lambda *args: messages.append(args[0:3])

        reply_address = Address(namespace="test", type="address", id="1")
        _, _ = await self.runner.run_task(_workflow, context=context, reply_address=reply_address)
        _, _, pipeline_request = [m for m in messages if "embedded" in m[0]][0]

        self.assertEqual(pipeline_request.reply_address, context.storage.task_request.reply_address)

    async def test_nested_pipeline_has_reply_topic_from_task_request(self):
        messages = []
        context = MagicMock()
        context.send_message = lambda *args: messages.append(args[0:3])

        _, _ = await self.runner.run_task(_workflow, context=context, reply_topic="test")
        _, _, pipeline_request = [m for m in messages if "embedded" in m[0]][0]

        self.assertEqual(pipeline_request.reply_topic, "test")

    async def test_nested_parallel_pipeline_in_stages_has_empty_worker_namespace_and_name_for_run_pipeline_and_flatten_tasks(self):
        messages = []
        context = MagicMock()
        context.send_message = lambda *args: messages.append(args[0:3])

        _, _ = await self.runner.run_task(_parallel_workflow_in_stages, context=context)
        _, _, pipeline_request = [m for m in messages if "embedded" in m[0]][0]

        pipeline = unpack_any(pipeline_request.request, [Pipeline])

        self.assertEqual(pipeline.entries[0].group_entry.group[0].entries[0].task_entry.task_type, '__builtins.run_pipeline')
        self.assertEqual(pipeline.entries[0].group_entry.group[0].entries[0].task_entry.namespace, '')
        self.assertEqual(pipeline.entries[0].group_entry.group[0].entries[0].task_entry.worker_name, '')

        self.assertEqual(pipeline.entries[0].group_entry.group[1].entries[0].task_entry.task_type, '__builtins.run_pipeline')
        self.assertEqual(pipeline.entries[0].group_entry.group[1].entries[0].task_entry.namespace, '')
        self.assertEqual(pipeline.entries[0].group_entry.group[1].entries[0].task_entry.worker_name, '')

        self.assertEqual(pipeline.entries[1].task_entry.task_type, '__builtins.flatten_results')
        self.assertEqual(pipeline.entries[1].task_entry.namespace, '')
        self.assertEqual(pipeline.entries[1].task_entry.worker_name, '')

    async def test_nested_parallel_pipeline_in_stages_has_non_empty_namespace_and_worker_name_for_pipeline_stage_tasks(self):
        messages = []
        context = MagicMock()
        context.send_message = lambda *args: messages.append(args[0:3])

        _, _ = await self.runner.run_task(_parallel_workflow_in_stages, context=context)
        _, _, pipeline_request = [m for m in messages if "embedded" in m[0]][0]

        pipeline = unpack_any(pipeline_request.request, [Pipeline])

        stage_one_pipeline = unpack_any(pipeline.entries[0].group_entry.group[0].entries[0].task_entry.request, [Pipeline])

        self.assertEqual(stage_one_pipeline.entries[0].group_entry.group[0].entries[0].task_entry.task_type, 'tests.test_pipeline_forwarding._task')
        self.assertEqual(stage_one_pipeline.entries[0].group_entry.group[0].entries[0].task_entry.namespace, 'test')
        self.assertEqual(stage_one_pipeline.entries[0].group_entry.group[0].entries[0].task_entry.worker_name, 'worker')

        stage_two_pipeline = unpack_any(pipeline.entries[0].group_entry.group[1].entries[0].task_entry.request, [Pipeline])
        
        self.assertEqual(stage_two_pipeline.entries[0].group_entry.group[0].entries[0].task_entry.task_type, 'tests.test_pipeline_forwarding._task')
        self.assertEqual(stage_two_pipeline.entries[0].group_entry.group[0].entries[0].task_entry.namespace, 'test')
        self.assertEqual(stage_two_pipeline.entries[0].group_entry.group[0].entries[0].task_entry.worker_name, 'worker')
