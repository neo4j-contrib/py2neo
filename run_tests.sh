#!/usr/bin/env bash

ARGS=$*
VERSIONS="3.5.0 3.4.10 3.3.6 3.2.12"

coverage erase

echo "Running unit tests"
coverage run -a -m pytest -v ${ARGS} test/unit
STATUS="$?"
if [ ${STATUS} -ne 0 ]
then
    exit ${STATUS}
fi

if [ -z "${NEO4J_SERVER_PACKAGE}" ]
then
    for VERSION in ${VERSIONS}
    do
        echo "Running standalone integration tests against Neo4j CE ${VERSION}"
        NEO4J_SERVER_PACKAGE="http://dist.neo4j.org/neo4j-community-${VERSION}-unix.tar.gz" coverage run -a -m pytest -v ${ARGS} test/integration-1
        STATUS="$?"
        if [ ${STATUS} -ne 0 ]
        then
            exit ${STATUS}
        fi
    done
else
    echo "Running standalone integration tests against Neo4j at ${NEO4J_SERVER_PACKAGE}"
    coverage run -a -m pytest -v ${ARGS} test/integration-1
    STATUS="$?"
    if [ ${STATUS} -ne 0 ]
    then
        exit ${STATUS}
    fi
fi

coverage report
