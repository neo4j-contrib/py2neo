#!/usr/bin/env bash

ARGS=$*


function run_unit_tests
{
    echo "Running unit tests"
    coverage run --append --module pytest -v ${ARGS} test/unit
    STATUS="$?"
    if [[ ${STATUS} -eq 5 ]]
    then
        return
    fi
    if [[ ${STATUS} -ne 0 ]]
    then
        exit ${STATUS}
    fi
}


function run_integration_tests
{
    echo "Using Java ${JAVA_VERSION} installation at ${JAVA_HOME}"
    coverage run --append --module pytest -v ${ARGS} test/integration
    STATUS="$?"
    if [[ ${STATUS} -ne 0 ]]
    then
        exit ${STATUS}
    fi
    if [[ "${PY2NEO_QUICK_TEST}" != "" ]]
    then
        return
    fi
}


pip install --upgrade coverage pytest
pip install --upgrade -r requirements.txt -r test_requirements.txt
coverage erase

run_unit_tests

JAVA_VERSION=11
JAVA_HOME="$(./where-is-java.sh $JAVA_VERSION)"
export JAVA_VERSION JAVA_HOME
run_integration_tests

JAVA_VERSION=8
JAVA_HOME="$(./where-is-java.sh $JAVA_VERSION)"
export JAVA_VERSION JAVA_HOME
run_integration_tests

coverage report
