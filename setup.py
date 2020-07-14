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

from os import path

from setuptools import setup, find_packages

from py2neo.meta import get_metadata, PatchedVersion


README_FILE = path.join(path.dirname(__file__), "README.rst")

with open(README_FILE) as _f:
    README = _f.read()


with PatchedVersion():
    setup(**dict(get_metadata(), **{
        "long_description": README,
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
            "colorama",
            "docker",
            "monotonic",
            "neotime~=1.7.4",
            "packaging",
            "pansi>=2020.7.3",
            "prompt_toolkit~=2.0.7",
            "pygments~=2.6.1",
            "pyopenssl",
            "pytz",
            "six>=1.15.0",
            "urllib3",
        ],
        "extras_require": {
        },
    }))
