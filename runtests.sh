#!/usr/bin/env bash

TESTS="$*"
if [ "${TESTS}" == "" ]
then
    coverage run -m unittest discover -vf
    STATUS=$?
else
    coverage run -m unittest -vf ${TESTS}
    STATUS=$?
fi
if [ "${STATUS}" == "0" ]
then
    coverage report -m
else
    exit ${STATUS}
fi
