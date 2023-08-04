import unittest
from tests.utils import TaskRunner
from statefun_tasks import FlinkTasks


tasks = FlinkTasks()


@tasks.bind()
def hello_workflow(first_name='Jane', last_name='Doe'):
    return f'Hello {first_name} {last_name}'


@tasks.bind()
def multi_arg_workflow(a, b, c, d):
    return ','.join([a, b, c, d])


@tasks.bind()
def multi_arg_workflow_with_some_defaults(a, b, c='c', d='d'):
    return ','.join([a, b, c, d])


class ArgsAndKwargsTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.runner = TaskRunner(tasks)

    async def test_passing_all_args(self):
        result, _ = await self.runner.run_task(multi_arg_workflow, 'a', 'b', 'c', 'd')
        self.assertEqual(result, 'a,b,c,d')

    async def test_passing_all_args_as_kwargs(self):
        result, _ = await self.runner.run_task(multi_arg_workflow, a='a', b='b', c='c', d='d')
        self.assertEqual(result, 'a,b,c,d')

    async def test_passing_mix_of_args_and_kwargs(self):
        result, _ = await self.runner.run_task(multi_arg_workflow, 'a', 'b', c='c', d='d')
        self.assertEqual(result, 'a,b,c,d')

    async def test_specifying_default_args_as_kwargs(self):
        result, _ = await self.runner.run_task(hello_workflow, first_name='John', last_name='Smith')
        self.assertEqual(result, 'Hello John Smith')

    async def test_specifying_default_args_as_args(self):
        result, _ = await self.runner.run_task(hello_workflow, 'John', 'Smith')
        self.assertEqual(result, 'Hello John Smith')

    async def test_specifying_single_default_arg(self):
        result, _ = await self.runner.run_task(hello_workflow, last_name='Smith')
        self.assertEqual(result, 'Hello Jane Smith')

    async def test_omitting_only_default_args(self):
        result, _ = await self.runner.run_task(multi_arg_workflow_with_some_defaults, 'a', 'b')
        self.assertEqual(result, 'a,b,c,d')

    async def test_providing_mandatory_plus_one_default_arg(self):
        result, _ = await self.runner.run_task(multi_arg_workflow_with_some_defaults, 'a', 'b', d='d')
        self.assertEqual(result, 'a,b,c,d')

    async def test_providing_mandatory_and_default_args(self):
        result, _ = await self.runner.run_task(multi_arg_workflow_with_some_defaults, 'a', 'b', c='c', d='d')
        self.assertEqual(result, 'a,b,c,d')

    async def test_not_providing_mandatory_args(self):
        try:
            await self.runner.run_task(multi_arg_workflow_with_some_defaults, 'a', c='c', d='d')
        except Exception as e:
            self.assertEqual(str(e), 'Not enough args supplied')
        else:
            self.fail('Expected an exception')
