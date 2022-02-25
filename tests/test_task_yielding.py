import unittest

from statefun_tasks import YieldTaskInvocation, TaskContext, TaskAction, TaskResult, TaskStatus
from google.protobuf.any_pb2 import Any
from tests.utils import TestHarness, tasks



@tasks.bind(with_context=True, task_id='123')
def yield_invocation(context: TaskContext):
    pipeline_id = context.get_pipeline_id()

    # record this task_request in the state
    context.set_state({pipeline_id: tasks.clone_task_request(context)})

    # but don't do anything with it by giving up execution
    raise YieldTaskInvocation()


@tasks.bind(with_context=True, task_id='123')
def resume_invocation(context: TaskContext, pipeline_id):
    state = context.get_state()

    task_request = state[pipeline_id]
    tasks.send_result(context, task_request, (), state={})

    return True


@tasks.bind()
def a_continuation():
    return 'A continuation was called'


class YieldingTasksTests(unittest.TestCase):
    def setUp(self) -> None:
        self.test_harness = TestHarness()

    @staticmethod
    def _unpack(any_proto: Any, proto_type):
        proto = proto_type()
        any_proto.Unpack(proto)
        return proto

    def test_task_that_yields_to_stop_the_pipeline(self):
        pipeline = yield_invocation.send().continue_with(a_continuation)  # a_continuation should not run 
        self.test_harness.run_pipeline(pipeline)

        # pipeline should have yielded and not run the continuation
        action_result = self.test_harness.run_action(pipeline, TaskAction.GET_STATUS)
        self.assertEqual(self._unpack(action_result.result, TaskStatus).value, TaskStatus.RUNNING)

        # call another pipeline that calls back to the first pipeline 
        pipeline2 = resume_invocation.send(pipeline.id)
        self.test_harness.run_pipeline(pipeline2)

        action_result = self.test_harness.run_action(pipeline, TaskAction.GET_STATUS)
        self.assertEqual(self._unpack(action_result.result, TaskStatus).value, TaskStatus.COMPLETED)

        action_result = self.test_harness.run_action(pipeline, TaskAction.GET_RESULT)
        task_result = self._unpack(action_result.result, TaskResult)
        self.assertTrue('A continuation was called' in str(task_result))


if __name__ == '__main__':
    unittest.main()
