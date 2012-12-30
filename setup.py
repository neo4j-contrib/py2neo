#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright 2011-2012 Nigel Small
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


import sys
sys.path.insert(0, "src")
from py2neo import __author__ as py2neo_author
from py2neo import __license__ as py2neo_license
from py2neo import __package__ as py2neo_package
from py2neo import __version__ as py2neo_version
sys.path.pop(0)

from distutils.core import setup

setup(
    name=py2neo_package,
    version=py2neo_version,
    description="Python client library for the Neo4j REST server",
    long_description="Py2neo is a simple and pragmatic Python library that "
                     "provides access to the popular graph database Neo4j via "
                     "its RESTful web service interface. With no external "
                     "dependencies, installation is straightforward and "
                     "getting started with coding is easy. The library is "
                     "actively maintained on GitHub, regularly updated in the "
                     "Python Package Index and is built uniquely for Neo4j in "
                     "close association with its team and community.",
    author=py2neo_author,
    author_email="nasmall@gmail.com",
    url="http://py2neo.org/",
    scripts=["scripts/cypher", "scripts/geoff"],
    package_dir={"": "src"},
    packages=["py2neo"],
    license=py2neo_license,
    classifiers=[]
)
