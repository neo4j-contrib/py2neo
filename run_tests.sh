#!/usr/bin/env bash

pip install --upgrade coverage coveralls -r requirements.txt
coverage erase
coverage run --append --module py2neo.testing -v
if [ "$?" != "0" ]
then
    echo "Tests failed under Bolt"
    exit 1
fi
NEO4J_URI=http://localhost:7474 coverage run --append --module py2neo.testing -v
if [ "$?" != "0" ]
then
    echo "Tests failed under HTTP"
    exit 1
fi
coverage report
coveralls
