#!/bin/sh

rm -rf dist
rm -rf build
rm -rf statfun_tasks.egg-info
python3 setup.py sdist bdist_wheel
