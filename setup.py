#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2018, Nigel Small
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

from py2neo.meta import __author__, __email__, __license__, __package__, __version__


with open(path_join(dirname(__file__), "README.md")) as f:
    README = f.read()

packages = find_packages(exclude=("book", "demo", "demo.*", "test"))
package_metadata = {
    "name": __package__,
    "version": __version__,
    "description": "Python client library and toolkit for Neo4j",
    "long_description": README,
    "long_description_content_type": "text/markdown",
    "author": __author__,
    "author_email": __email__,
    "url": "http://py2neo.org/",
    "entry_points": {
        "console_scripts": [
            "py2neo-admin = py2neo.admin:main",
            "py2neo-console = py2neo.console:repl",
        ],
    },
    "packages": packages,
    "py_modules": [],
    "install_requires": [
        "click>=2.0",
        "colorama",
        "neo4j-driver>=1.6.0a1",
        "prompt_toolkit",
        "pygments>=2.0",
        "pytest",
        "urllib3[secure]",
    ],
    "license": __license__,
    "classifiers": [
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3.3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Topic :: Database",
        "Topic :: Software Development",
    ],
    "zip_safe": False,
}

setup(**package_metadata)
