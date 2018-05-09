#!/usr/bin/env bash

ROOT=$(dirname $0)

PY2NEO_QUICK_TEST=1 ${ROOT}/run_tests.sh $*
