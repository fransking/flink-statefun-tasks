#!/bin/bash

protoc -I=statefun_tasks --python_out=statefun_tasks messages.proto
protoc -I=tests --python_out=tests test_messages.proto
