import unittest

from statefun_tasks import YieldTaskInvocation, TaskContext, TaskAction, TaskResult, TaskStatus, RetryPolicy
from google.protobuf.any_pb2 import Any
from tests.utils import TestHarness, tasks


@tasks.bind(with_state=True, is_fruitful=False)
def setup(_):
    state = [1, 2, 3]
    return state


@tasks.bind(with_context=True, task_id='123', retry_policy=RetryPolicy([ValueError]))
def yield_invocation(context: TaskContext, fail_once=False):
    pipeline_id = context.get_pipeline_id()

    state = context.get_state() or {}
    if fail_once:
        if not 'failed' in state:
            state['failed'] = True
            context.set_state(state)
            raise ValueError()
        else:
            del state['failed']

    # record this task_request in the task state
    state[pipeline_id] = tasks.clone_task_request(context)
    context.set_state(state)

    # but don't do anything with it by giving up execution
    raise YieldTaskInvocation()


@tasks.bind(with_context=True, task_id='123')
async def resume_invocation(context: TaskContext, pipeline_id):
    task_state = context.get_state()

    # get saved task_request and unpack its state
    task_request = task_state[pipeline_id]
    _, _, state = tasks.unpack_task_request(task_request)

    # send the result back for this task_request updating the state
    await tasks.send_result(context, task_request, (), state + [4])

    return True


@tasks.bind(with_state=True)
def a_continuation(state):
    return state, f'A continuation was called with state {state}'


class YieldingTasksTests(unittest.TestCase):
    def setUp(self) -> None:
        self.test_harness = TestHarness()

    @staticmethod
    def _unpack(any_proto: Any, proto_type):
        proto = proto_type()
        any_proto.Unpack(proto)
        return proto

    def test_task_that_yields_to_stop_the_pipeline(self):
        pipeline = setup.send().continue_with(yield_invocation).continue_with(a_continuation)  # a_continuation should not run 
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
        self.assertTrue('A continuation was called with state [1, 2, 3, 4]' in str(task_result))

    def test_task_that_yields_with_retry_to_stop_the_pipeline(self):
        pipeline = setup.send().continue_with(yield_invocation, fail_once=True).continue_with(a_continuation)  # a_continuation should not run 
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
        self.assertTrue('A continuation was called with state [1, 2, 3, 4]' in str(task_result))

if __name__ == '__main__':
    unittest.main()
