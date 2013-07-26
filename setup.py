#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright 2011-2013, Nigel Small
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


try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

from py2neo import __author__, __email__, __license__, __package__, __version__


setup(
    name=__package__,
    version=__version__,
    description="Python client library for the Neo4j REST server",
    long_description="Py2neo is a simple and pragmatic Python library that "
                     "provides access to the popular graph database Neo4j via "
                     "its RESTful web service interface. With no external "
                     "dependencies, installation is straightforward and "
                     "getting started with coding is easy. The library is "
                     "actively maintained on GitHub, regularly updated in the "
                     "Python Package Index and is built uniquely for Neo4j in "
                     "close association with its team and community.",
    author=__author__,
    author_email=__email__,
    url="http://py2neo.org/",
    scripts=[
        "scripts/neotool",
    ],
    packages=[
        "py2neo",
    ],
    install_requires=[
        "httpstream",
    ],
    license=__license__,
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 2.6",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3.3",
        "Topic :: Database",
        "Topic :: Software Development",
    ]
)
