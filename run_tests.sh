#!/usr/bin/env bash

ARGS=$*
NEO4J_URI=""

function run
{
    NEO4J_URI="$1"
    NEO4J_SECURE="$2"
    coverage run --append --module py2neo.testing -v ${ARGS}
    if [ "$?" != "0" ]
    then
        echo "Tests failed (NEO4J_URI=${NEO4J_URI} NEO4J_SECURE=${NEO4J_SECURE})"
        exit 1
    fi
}

rm -r *.egg-info 2> /dev/null
pip install --upgrade -r requirements.txt -r test_requirements.txt
coverage erase
run "" ""
run "bolt://localhost:7687" 0
run "bolt://localhost:7687" 1
run "http://localhost:7474" 0
run "https://localhost:7473" 1
coverage report
