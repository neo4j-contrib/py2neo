#!/usr/bin/env bash

HOME=$(dirname $0)

pip install --upgrade sphinx
make -C ${HOME}/book html
xdg-open ${HOME}/book/_build/html/index.html 2> /dev/null
