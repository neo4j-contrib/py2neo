#!/usr/bin/env bash

ARGS=$*
NEO4J_VERSIONS="3.5 3.4 3.3 3.2"


function run_unit_tests
{
    echo "Running unit tests"
    coverage run --append --module pytest -v ${ARGS} test/unit
    STATUS="$?"
    if [[ ${STATUS} -ne 0 ]]
    then
        exit ${STATUS}
    fi
}


function run_integration_1_tests
{
    for NEO4J_VERSION in ${NEO4J_VERSIONS}
    do
        echo "Running standalone integration tests against Neo4j CE ${NEO4J_VERSION}"
        NEO4J_VERSION=${NEO4J_VERSION} coverage run --append --module pytest -v ${ARGS} test/integration_1
        STATUS="$?"
        if [[ ${STATUS} -ne 0 ]]
        then
            exit ${STATUS}
        fi
    done
}


function run_all_tests
{
    run_unit_tests
    run_integration_1_tests
}


coverage erase
run_all_tests
coverage report
