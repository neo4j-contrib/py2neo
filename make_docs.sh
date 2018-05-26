#!/usr/bin/env bash

HOME=$(dirname $0)
OFFLINE=$(ping -n -c 1 -W 1 8.8.8.8 > /dev/null ; echo $?)

if [ "${OFFLINE}" == "0" ]
then
    pip install --upgrade sphinx
fi
make -C ${HOME}/docs html

echo ""
INDEX_FILE="${HOME}/docs/_build/html/index.html"
echo "Documentation index file can be found at file://$(cd "$(dirname "${INDEX_FILE}")"; pwd)/$(basename "${INDEX_FILE}")"
