#!/bin/sh

uvicorn examples.server:app --host 0.0.0.0 --port 8082
