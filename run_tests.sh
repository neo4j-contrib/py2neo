#!/usr/bin/env bash

pip install --upgrade coverage coveralls -r requirements.txt
coverage erase
coverage run --append --module py2neo.testing -v
NEO4J_URI=http://localhost:7474 coverage run --append --module py2neo.testing -v
coverage report
coveralls
