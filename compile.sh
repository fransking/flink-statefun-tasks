#!/bin/bash

protoc -I=statefun_tasks --python_out=statefun_tasks messages.proto
