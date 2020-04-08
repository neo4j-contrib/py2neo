#!/usr/bin/env bash


SCRIPT="$0"
VERSION="$1"


if [ "${VERSION}" == "" ]
then
    SCRIPT_NAME=$(basename "${SCRIPT}")
    echo "usage: ${SCRIPT_NAME} JDK_VERSION"
    exit 0
else
    JAVA_EXE=$(update-alternatives --list java | grep -E "(java-${VERSION}-openjdk|jdk-${VERSION})" | head -1)
    if [ "${JAVA_EXE}" == "" ]
    then
        JHOME="${HOME}/openjdk${VERSION}"
        if [ ! -d "${JHOME}" ]
        then
            if [ -z "${TRAVIS_BUILD_DIR}" ]
            then
                install-jdk.sh --silent --feature "${VERSION}" --target "${JHOME}" > /dev/null
            else
                "${TRAVIS_BUILD_DIR}/install-jdk.sh" --silent --feature "${VERSION}" --target "${JHOME}" > /dev/null
            fi
            STATUS="$?"
            if [[ ${STATUS} -ne 0 ]]
            then
                echo "Cannot find JDK ${VERSION} and failed to invoke install-jdk.sh"
                exit ${STATUS}
            fi
        fi
    else
        JHOME="$(dirname ${JAVA_EXE})/.."
    fi
    echo "${JHOME}"
    exit 0
fi
