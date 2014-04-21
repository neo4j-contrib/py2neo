#!/bin/bash

DIR="neo4j-community-$NEO4J_VERSION"
FILE="$DIR-unix.tar.gz"

# only download if the version directory does not exist
if [[ ! -d lib/$DIR ]]; then

    # download requested version, unpack, clearup
    wget http://dist.neo4j.org/$FILE
    tar xvfz $FILE &> /dev/null
    rm $FILE

    # if lib directory does not exist, create it
    [[ ! -d lib ]] && mkdir lib
    mv $DIR lib/

fi

# sym-link our version to the default path
[[ -h lib/neo4j ]] && unlink lib/neo4j
ln -fs $DIR lib/neo4j
