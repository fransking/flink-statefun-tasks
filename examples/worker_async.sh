#!/bin/sh

gunicorn -b "0.0.0.0:8081" -w 1 examples.worker_async:app --worker-class aiohttp.GunicornWebWorker
