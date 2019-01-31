#!/usr/bin/env bash


function check_license {
    find py2neo -name '*.py' | xargs grep -L "http://www.apache.org/licenses/LICENSE-2.0"
}


function check_copyright {
    YEAR=$(date +%Y)
    find py2neo -name '*.py' | xargs grep -L -e "Copyright.* 20[0-9][0-9]-${YEAR}"
}


UNLICENSED=$(check_license)
if [ "${UNLICENSED}" ]
then
    echo "The following files do not contain Apache license details:"
    echo "${UNLICENSED}"
    exit 1
fi

UNCOPYRIGHTED=$(check_copyright)
if [ "${UNCOPYRIGHTED}" ]
then
    echo "The following files do not contain an up-to-date copyright notice:"
    echo "${UNCOPYRIGHTED}"
    exit 1
fi
