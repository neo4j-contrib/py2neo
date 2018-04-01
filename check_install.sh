#!/usr/bin/env bash

ENV=venv

virtualenv ${ENV}
source ${ENV}/bin/activate
pip install .
deactivate
rm -r ${ENV}
