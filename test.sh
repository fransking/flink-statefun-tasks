#!/bin/sh

python3 -m tests.test_simple_pipeline && \
python3 -m tests.test_parallel_workflow && \
python3 -m tests.test_passthrough_args && \
python3 -m tests.test_request_serialisation && \
python3 -m tests.test_result_serialisation && \
python3 -m tests.test_retrying && \
python3 -m tests.test_protobuf && \
python3 -m tests.test_finally_do
