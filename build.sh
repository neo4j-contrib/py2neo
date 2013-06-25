#!/usr/bin/env bash

# Build script for py2neo

# ascertain project directory
PY2NEO=`pwd`/`dirname $0`/..

# clear any existing builds
rm -rf $PY2NEO/dist

# check version number for this code
THIS_VERSION=`python $PY2NEO/version.py`

# check last release version number
LAST_VERSION=`curl http://pypi.python.org/pypi/py2neo/json 2> /dev/null | grep '"version"' | awk -F\" '{print $4}'`

# set python path
export PYTHONPATH=$PY2NEO/src

# package software
if [ "$THIS_VERSION" == "$LAST_VERSION" ]
then
    echo "*** Building version $THIS_VERSION ***"
    python $PY2NEO/setup.py sdist
else
    echo "*** Building and releasing version $THIS_VERSION ***"
    python $PY2NEO/setup.py sdist upload
fi
