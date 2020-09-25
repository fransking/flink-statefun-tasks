#!/bin/sh

gunicorn -b "0.0.0.0:8080" -w 1 examples.worker:app
