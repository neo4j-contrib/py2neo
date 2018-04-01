#!/usr/bin/env bash

ENV=venv

pip install --upgrade virtualenv
virtualenv ${ENV}
source ${ENV}/bin/activate
pip install .
deactivate
rm -r ${ENV}
