#!/usr/bin/env bash

ENV=venv

pip install --upgrade virtualenv
virtualenv ${ENV}
source ${ENV}/bin/activate
pip install .
if [ "$?" != "0" ]
then
    exit 1
fi
deactivate
rm -r ${ENV}
