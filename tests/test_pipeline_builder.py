import unittest
from statefun_tasks import FlinkTasks, DefaultSerialiser
from statefun_tasks.messages_pb2 import Pipeline, TupleOfValue, TupleOfAny, MapOfStringToValue, MapOfStringToAny
from statefun_tasks.protobuf import unpack_any


tasks = FlinkTasks()


@tasks.bind()
def _task_a(x):
    return x


@tasks.bind()
def _task_b(x):
    return x


class PipelineBuilderToProtoTests(unittest.TestCase):
    def test_to_proto_produces_pipeline(self):
        serialiser = DefaultSerialiser()
        pipeline_proto = _task_a.send(1).continue_with(_task_b).to_proto(serialiser)
        self.assertIsInstance(pipeline_proto, Pipeline)
        self.assertEqual(len(pipeline_proto.entries), 2)

    def test_to_proto_uses_value_args_and_kwargs_by_default(self):
        serialiser = DefaultSerialiser()
        pipeline_proto = _task_a.send(1).to_proto(serialiser)
        self.assertEqual(
            pipeline_proto.entries[0].task_entry.request.type_url,
            'type.googleapis.com/statefun_tasks.ValueArgsAndKwargs'
        )

    def test_to_proto_round_trips_args(self):
        serialiser = DefaultSerialiser()
        pipeline_proto = _task_a.send(42).to_proto(serialiser)
        request = pipeline_proto.entries[0].task_entry.request
        args, kwargs = serialiser.deserialise_args_and_kwargs(request)
        self.assertEqual(args, (42,))
        self.assertEqual(kwargs, {})

    def test_to_proto_round_trips_kwargs(self):
        serialiser = DefaultSerialiser()
        pipeline_proto = _task_a.send(x=42).to_proto(serialiser)
        request = pipeline_proto.entries[0].task_entry.request
        args, kwargs = serialiser.deserialise_args_and_kwargs(request)
        self.assertEqual(args, ())
        self.assertEqual(kwargs, {'x': 42})


class LegacyPipelineBuilderToProtoTests(PipelineBuilderToProtoTests):
    def test_to_proto_uses_value_args_and_kwargs_by_default(self):
        serialiser = DefaultSerialiser(use_legacy_types=True)
        pipeline_proto = _task_a.send(1).to_proto(serialiser)
        self.assertEqual(
            pipeline_proto.entries[0].task_entry.request.type_url,
            'type.googleapis.com/statefun_tasks.ArgsAndKwargs'
        )

    def test_to_proto_round_trips_args(self):
        serialiser = DefaultSerialiser(use_legacy_types=True)
        pipeline_proto = _task_a.send(42).to_proto(serialiser)
        request = pipeline_proto.entries[0].task_entry.request
        args, kwargs = serialiser.deserialise_args_and_kwargs(request)
        self.assertEqual(args, (42,))
        self.assertEqual(kwargs, {})

    def test_to_proto_round_trips_kwargs(self):
        serialiser = DefaultSerialiser(use_legacy_types=True)
        pipeline_proto = _task_a.send(x=42).to_proto(serialiser)
        request = pipeline_proto.entries[0].task_entry.request
        args, kwargs = serialiser.deserialise_args_and_kwargs(request)
        self.assertEqual(args, ())
        self.assertEqual(kwargs, {'x': 42})

    def test_to_proto_produces_pipeline(self):
        serialiser = DefaultSerialiser(use_legacy_types=True)
        pipeline_proto = _task_a.send(1).continue_with(_task_b).to_proto(serialiser)
        self.assertIsInstance(pipeline_proto, Pipeline)
        self.assertEqual(len(pipeline_proto.entries), 2)


class PipelineBuilderToTaskRequestTests(unittest.TestCase):
    def test_to_task_request_has_run_pipeline_task_type(self):
        serialiser = DefaultSerialiser()
        task_request = _task_a.send(1).to_task_request(serialiser)
        self.assertEqual(task_request.type, '__builtins.run_pipeline')

    def test_to_task_request_uses_value_args_and_kwargs_by_default(self):
        serialiser = DefaultSerialiser()
        task_request = _task_a.send(1).to_task_request(serialiser)
        self.assertEqual(
            task_request.request.type_url,
            'type.googleapis.com/statefun_tasks.Pipeline'
        )

    def test_to_task_request_request_contains_pipeline(self):
        serialiser = DefaultSerialiser()
        task_request = _task_a.send(1).continue_with(_task_b).to_task_request(serialiser)
        args, _ = serialiser.deserialise_args_and_kwargs(task_request.request)
        self.assertIsInstance(args, Pipeline)
        self.assertEqual(len(args.entries), 2)


class LegacyPipelineBuilderToTaskRequestTests(PipelineBuilderToTaskRequestTests):
    def test_to_task_request_uses_value_args_and_kwargs_by_default(self):
        serialiser = DefaultSerialiser(use_legacy_types=True)
        task_request = _task_a.send(1).to_task_request(serialiser)
        self.assertEqual(
            task_request.request.type_url,
            'type.googleapis.com/statefun_tasks.Pipeline'
        )

    def test_to_task_request_request_contains_pipeline(self):
        serialiser = DefaultSerialiser(use_legacy_types=True)
        task_request = _task_a.send(1).continue_with(_task_b).to_task_request(serialiser)
        args, _ = serialiser.deserialise_args_and_kwargs(task_request.request)
        self.assertIsInstance(args, Pipeline)
        self.assertEqual(len(args.entries), 2)

    def test_to_task_request_has_run_pipeline_task_type(self):
        serialiser = DefaultSerialiser(use_legacy_types=True)
        task_request = _task_a.send(1).to_task_request(serialiser)
        self.assertEqual(task_request.type, '__builtins.run_pipeline')


class PipelineBuilderInitialParametersTests(unittest.TestCase):
    def test_initial_args_uses_tuple_of_value(self):
        serialiser = DefaultSerialiser()
        pipeline_proto = _task_a.send(1).with_initial(args=(42, 'hello')).to_proto(serialiser)
        self.assertTrue(pipeline_proto.HasField('initial_args'))
        unpacked = unpack_any(pipeline_proto.initial_args, [TupleOfValue])
        self.assertIsInstance(unpacked, TupleOfValue)

    def test_initial_kwargs_uses_map_of_string_to_value(self):
        serialiser = DefaultSerialiser()
        pipeline_proto = _task_a.send(1).with_initial(kwargs={'x': 1}).to_proto(serialiser)
        self.assertEqual(pipeline_proto.WhichOneof('kwargs_kind'), 'initial_value_kwargs')
        self.assertIsInstance(pipeline_proto.initial_value_kwargs, MapOfStringToValue)

    def test_initial_kwargs_values_are_correct(self):
        serialiser = DefaultSerialiser()
        pipeline_proto = _task_a.send(1).with_initial(kwargs={'x': 42, 'y': 'hello'}).to_proto(serialiser)
        self.assertEqual(pipeline_proto.initial_value_kwargs.items['x'].int_value, 42)
        self.assertEqual(pipeline_proto.initial_value_kwargs.items['y'].string_value, 'hello')

    def test_initial_state_uses_value(self):
        serialiser = DefaultSerialiser()
        pipeline_proto = _task_a.send(1).with_initial(state='my_state').to_proto(serialiser)
        self.assertTrue(pipeline_proto.HasField('initial_state'))
        unpacked = unpack_any(pipeline_proto.initial_state, [])
        self.assertEqual(unpacked.string_value, 'my_state')

    def test_initial_args_round_trips(self):
        serialiser = DefaultSerialiser()
        pipeline_proto = _task_a.send(1).with_initial(args=(42,)).to_proto(serialiser)
        unpacked = unpack_any(pipeline_proto.initial_args, [TupleOfValue])
        result = serialiser.from_proto(unpacked)
        self.assertEqual(result, (42,))

    def test_initial_state_round_trips(self):
        serialiser = DefaultSerialiser()
        pipeline_proto = _task_a.send(1).with_initial(state='my_state').to_proto(serialiser)
        result = serialiser.from_proto(unpack_any(pipeline_proto.initial_state, []))
        self.assertEqual(result, 'my_state')


class LegacyPipelineBuilderInitialParametersTests(unittest.TestCase):
    def test_initial_args_uses_tuple_of_any(self):
        serialiser = DefaultSerialiser(use_legacy_types=True)
        pipeline_proto = _task_a.send(1).with_initial(args=(42, 'hello')).to_proto(serialiser)
        self.assertTrue(pipeline_proto.HasField('initial_args'))
        unpacked = unpack_any(pipeline_proto.initial_args, [TupleOfAny])
        self.assertIsInstance(unpacked, TupleOfAny)

    def test_initial_kwargs_uses_map_of_string_to_any(self):
        serialiser = DefaultSerialiser(use_legacy_types=True)
        pipeline_proto = _task_a.send(1).with_initial(kwargs={'x': 1}).to_proto(serialiser)
        self.assertEqual(pipeline_proto.WhichOneof('kwargs_kind'), 'initial_kwargs')
        self.assertIsInstance(pipeline_proto.initial_kwargs, MapOfStringToAny)

    def test_initial_state_uses_legacy_wrapper(self):
        serialiser = DefaultSerialiser(use_legacy_types=True)
        pipeline_proto = _task_a.send(1).with_initial(state='my_state').to_proto(serialiser)
        self.assertTrue(pipeline_proto.HasField('initial_state'))
        unpacked = unpack_any(pipeline_proto.initial_state, [])
        self.assertEqual(unpacked.value, 'my_state')

    def test_initial_args_round_trips(self):
        serialiser = DefaultSerialiser(use_legacy_types=True)
        pipeline_proto = _task_a.send(1).with_initial(args=(42,)).to_proto(serialiser)
        unpacked = unpack_any(pipeline_proto.initial_args, [TupleOfAny])
        result = serialiser.from_proto(unpacked)
        self.assertEqual(result, (42,))

    def test_initial_state_round_trips(self):
        serialiser = DefaultSerialiser(use_legacy_types=True)
        pipeline_proto = _task_a.send(1).with_initial(state='my_state').to_proto(serialiser)
        result = serialiser.from_proto(unpack_any(pipeline_proto.initial_state, []))
        self.assertEqual(result, 'my_state')
