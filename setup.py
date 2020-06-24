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


from os.path import dirname, join as path_join
try:
    from setuptools import setup, find_packages
except ImportError:
    from distutils.core import setup, find_packages

import py2neo


with open(path_join(dirname(__file__), "README.rst")) as f:
    README = f.read()

packages = find_packages(exclude=("docs", "test"))
package_metadata = {
    "name": py2neo.__package__,
    "version": py2neo.__version__,
    "description": "Python client library and toolkit for Neo4j",
    "long_description": README,
    "long_description_content_type": "text/markdown",
    "author": py2neo.__author__,
    "author_email": py2neo.__email__,
    "url": "https://py2neo.org/",
    "entry_points": {
        "console_scripts": [
            "py2neo = py2neo.__main__:main",
        ],
        "pygments.lexers": [
            "py2neo.cypher = py2neo.cypher.lexer:CypherLexer",
        ],
    },
    "packages": packages,
    "py_modules": [],
    "install_requires": [
        "certifi",
        "click==7.0",
        "colorama",
        "docker",
        "monotonic",
        "neotime~=1.7.4",
        "prompt_toolkit~=2.0.7",
        "pygments~=2.3.1",
        "pytz",
        "urllib3",
    ],
    "extras_require": {
    },
    "license": py2neo.__license__,
    "classifiers": [
        "Development Status :: 6 - Mature",
        "Environment :: Console",
        "Intended Audience :: Developers",
        "Intended Audience :: Information Technology",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: Apache Software License",
        "Natural Language :: English",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: Implementation :: CPython",
        "Topic :: Database",
        "Topic :: Database :: Database Engines/Servers",
        "Topic :: Scientific/Engineering",
        "Topic :: Software Development",
        "Topic :: Software Development :: Libraries",
    ],
    "zip_safe": False,
}

setup(**package_metadata)
