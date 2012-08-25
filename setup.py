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
    description="Python bindings to Neo4j",
    long_description="""The py2neo project provides bindings between Python and Neo4j via its RESTful web service interface. It attempts to be both Pythonic and consistent with the core Neo4j API and is compatible with Python 3.""",
    author=py2neo_author,
    author_email="py2neo@nigelsmall.net",
    url="http://py2neo.org/",
    scripts=["scripts/cypher", "scripts/geoff"],
    package_dir={"": "src"},
    packages=["py2neo"],
    license=py2neo_license,
    classifiers=[]
)
