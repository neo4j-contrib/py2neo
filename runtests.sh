#!/usr/bin/env bash


# TODO: start/stop database
# TODO: check coverage is installed

coverage run -m unittest -v
coverage report -m
