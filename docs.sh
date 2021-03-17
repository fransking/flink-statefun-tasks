#!/bin/sh

cd docsrc && rm -rf _build/* && make html
cd ..
