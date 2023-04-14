import unittest

from statefun_tasks import in_parallel
from tests.utils import FlinkTestHarness, tasks
from os import urandom

_max_state_size = {}
_final_state_size = {}


@tasks.events.on_pipeline_task_finished
def on_pipeline_task_finished(context, *args, **kwargs):
    pipeline_id = context.get_task_id()    
    current_state_size = _max_state_size.get(pipeline_id, 0)
    new_state_size = context.pipeline_state.ByteSize() / 1024 / 1024
    _max_state_size[pipeline_id] = max(current_state_size, new_state_size)


@tasks.events.on_emit_result
def on_emit_result(context, *args, **kwargs):
    pipeline_id = context.get_task_id()    
    current_state_size = _max_state_size.get(pipeline_id, 0)
    new_state_size = context.pipeline_state.ByteSize() / 1024 / 1024
    _max_state_size[pipeline_id] = max(current_state_size, new_state_size)
    _final_state_size[pipeline_id] = new_state_size


@tasks.bind()
def gen_data(size_in_mb):
    data = urandom(size_in_mb * 1024 * 1024)
    return data


@tasks.bind()
def recv_data(data):
    return len(data)


class PipelineStateBalloonTests(unittest.TestCase):
    def setUp(self) -> None:
        self.test_harness = FlinkTestHarness()

    def test_pipeline_in_parallel_aggregation(self):
        pipeline = in_parallel([gen_data.send(1), gen_data.send(1), gen_data.send(1)])
        self.test_harness.run_pipeline(pipeline)
        self.assertLess(_max_state_size[pipeline.id], 3.1)
        self.assertLess(_final_state_size[pipeline.id], 0.1)

    def test_pipeline_in_parallel_aggregation_with_max_parallelism(self):
        pipeline = in_parallel([gen_data.send(1), gen_data.send(1), gen_data.send(1), gen_data.send(1), gen_data.send(1)], max_parallelism=1)
        self.test_harness.run_pipeline(pipeline)
        self.assertLess(_max_state_size[pipeline.id], 4.1)
        self.assertLess(_final_state_size[pipeline.id], 0.1)

    def test_pipeline_in_parallel_with_repeated_task_inputs(self):
        data = gen_data(1)
        pipeline = in_parallel([recv_data.send(data), recv_data.send(data), recv_data.send(data)])
        self.test_harness.run_pipeline(pipeline)
        self.assertLess(_max_state_size[pipeline.id], 4.1)
        self.assertLess(_final_state_size[pipeline.id], 3.1)

    def test_pipeline_in_parallel_with_initial_parameters(self):
        data = gen_data(1)
        pipeline = in_parallel([recv_data.send(), recv_data.send(), recv_data.send()]).with_initial(args=(data,))
        self.test_harness.run_pipeline(pipeline)
        self.assertLess(_max_state_size[pipeline.id], 1.1)
        self.assertLess(_final_state_size[pipeline.id], 1.1)

    def test_pipeline_in_parallel_with_initial_parameters_and_max_parallelism(self):
        data = gen_data(1)
        pipeline = in_parallel([recv_data.send(), recv_data.send(), recv_data.send()], max_parallelism=1).with_initial(args=(data,))
        self.test_harness.run_pipeline(pipeline)
        self.assertLess(_max_state_size[pipeline.id], 1.1)
        self.assertLess(_final_state_size[pipeline.id], 1.1)


if __name__ == '__main__':
    unittest.main()
