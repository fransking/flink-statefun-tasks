#!/bin/bash

protoc -I=. --python_out=. messages.proto