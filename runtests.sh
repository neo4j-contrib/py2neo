#!/usr/bin/env bash

coverage run -m unittest discover -vf
STATUS=$?
if [ "${STATUS}" == "0" ]
then
    coverage report -m
else
    exit ${STATUS}
fi
