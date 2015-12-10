#!/usr/bin/env bash

if [ "$1" == "" ]
then
    TEST=test
else
    TEST=$1
fi

coverage run -m unittest discover -vfs "${TEST}"
STATUS=$?
if [ "${STATUS}" == "0" ]
then
    coverage report -m
else
    exit ${STATUS}
fi
