#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2020, Nigel Small
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from os import getenv, path
from warnings import warn

from setuptools import setup, find_packages

from py2neo.meta import VERSION_FILE, get_metadata, parse_version_string


README_FILE = path.join(path.dirname(__file__), "README.rst")


def get_readme():
    with open(README_FILE) as f:
        return f.read()


class Release(object):

    def __init__(self):
        self.__original = None

    def __enter__(self):
        self.__original = parse_version_string(self._read_version())
        if self.__original["dev"]:
            patched = parse_version_string(self._read_patched())
            self._check_compatible(self.__original, patched)
            self._patch_version(patched["string"])
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._patch_version(self.__original["string"])

    @classmethod
    def _read_version(cls):
        with open(VERSION_FILE, "r") as f:
            return f.read()

    @classmethod
    def _read_patched(cls):
        patched = getenv("RELEASE")
        if patched:
            return patched
        else:
            warn("RELEASE environment variable not set - assuming development release")
            return cls._read_version()

    @classmethod
    def _check_compatible(cls, original, patched):
        if original == patched:
            return
        compatible = True
        if patched["epoch"] != original["epoch"]:
            compatible = False
        if patched["release"][:2] != original["release"][:2]:
            compatible = False
        if patched["dev"] is not None:
            compatible = False
        if not compatible:
            raise SystemExit("Patched version string %r is not compatible with original version "
                             "string %r" % (patched["string"], original["string"]))

    @classmethod
    def _patch_version(cls, value):
        with open(VERSION_FILE, "w") as f:
            f.write(value)


with Release():
    setup(**dict(get_metadata(), **{
        "long_description": get_readme(),
        "long_description_content_type": "text/x-rst",
        "entry_points": {
            "console_scripts": [
                "py2neo = py2neo.__main__:main",
            ],
            "pygments.lexers": [
                "py2neo.cypher = py2neo.cypher.lexer:CypherLexer",
            ],
        },
        "packages": find_packages(exclude=("docs", "test", "test.*")),
        "package_data": {
            "py2neo": ["VERSION"],
        },
        "py_modules": [],
        "install_requires": [
            "certifi",
            "cryptography",
            "docker",
            "monotonic",
            "neotime~=1.7.4",
            "packaging",
            "pansi>=2020.7.3",
            "prompt_toolkit~=2.0.7",
            "pygments>=2.0.0",
            "pytz",
            "six>=1.15.0",
            "urllib3",
        ],
        "extras_require": {
        },
    }))
