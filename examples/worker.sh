#!/bin/sh

uvicorn examples.worker:app --host 0.0.0.0 --port 8085
