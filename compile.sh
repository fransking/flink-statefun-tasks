#!/bin/bash

wget -q -O statefun_tasks/statefun.request_reply.proto https://github.com/apache/flink-statefun/raw/master/statefun-flink/statefun-flink-core/src/main/protobuf/http-function.proto
protoc -I=statefun_tasks --python_out=statefun_tasks messages.proto
rm statefun_tasks/statefun.request_reply.proto
