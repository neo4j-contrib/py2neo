#!/usr/bin/env bash

ROOT=$(dirname $0)
ENV=venv

cd ${ROOT}
rm -r dist
python setup.py sdist

cd ${ROOT}/dist
pip install --upgrade virtualenv
virtualenv ${ENV}
source ${ENV}/bin/activate
pip install py2neo-*.tar.gz
if [ "$?" != "0" ]
then
    exit 1
fi
deactivate
rm -r ${ENV}
