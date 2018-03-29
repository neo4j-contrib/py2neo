#!/usr/bin/env bash

TEST_ARGS="$*"

coverage erase

coverage run -m pytest -v ${TEST_ARGS} test
STATUS=$?
if [ "${STATUS}" == "0" ]
then
    coverage report
else
    exit ${STATUS}
fi
