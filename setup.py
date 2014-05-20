#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright 2011-2014, Nigel Small
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

try:
    from setuptools import setup
    from setuptools.extension import Extension
except ImportError:
    from distutils.core import setup
    from distutils.extension import Extension

from py2neo import __author__, __email__, __license__, __package__, __version__


def do_setup(extensions=None):
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
        url="http://nigelsmall.com/py2neo",
        scripts=[
            "scripts/neotool",
        ],
        packages=[
            "py2neo",
            "py2neo.packages",
            "py2neo.packages.httpstream",
            "py2neo.packages.jsonstream",
            "py2neo.packages.urimagic",
        ],
        install_requires=[
        ],
        license=__license__,
        classifiers=[
            "Development Status :: 5 - Production/Stable",
            "Intended Audience :: Developers",
            "License :: OSI Approved :: Apache Software License",
            "Operating System :: OS Independent",
            "Programming Language :: Python :: 2.7",
            "Programming Language :: Python :: 3.3",
            "Topic :: Database",
            "Topic :: Software Development",
        ],
        ext_modules=extensions,
        zip_safe=False,
    )

py3k = sys.version_info >= (3,)
sdist = "sdist" in sys.argv

extensions = []
if sdist or py3k:
    extensions.append(Extension("py2neo.packages.jsonstream.cjsonstream",
                                ["py2neo/packages/jsonstream/cjsonstream.c"]))
elif sdist or not py3k:
    extensions.append(Extension("py2neo.packages.jsonstream.cjsonstream_2x",
                                ["py2neo/packages/jsonstream/cjsonstream_2x.c"]))

try:
    do_setup(extensions)
except:
    do_setup()
