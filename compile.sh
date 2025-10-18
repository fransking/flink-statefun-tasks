#!/bin/bash

protoc -I=statefun_tasks/core/statefun --python_out=statefun_tasks/core/statefun kafka-egress.proto
protoc -I=statefun_tasks/core/statefun --python_out=statefun_tasks/core/statefun kinesis-egress.proto
protoc -I=statefun_tasks/core/statefun --python_out=statefun_tasks/core/statefun types.proto
protoc -I=statefun_tasks/core/statefun --python_out=statefun_tasks/core/statefun request-reply.proto
protoc -I=statefun_tasks --python_out=statefun_tasks messages.proto
protoc -I=tests --python_out=tests test_messages.proto
