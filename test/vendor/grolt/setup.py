#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2021, Nigel Small
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


from setuptools import setup, find_packages


packages = find_packages()
package_metadata = {
    "name": "grolt",
    "version": "1.0.7",
    "description": "Docker-based development and testing framework for Neo4j",
    "long_description": "Please see https://github.com/technige/grolt "
                        "for details.",
    "author": "Nigel Small",
    "author_email": "technige@py2neo.org",
    "entry_points": {
        "console_scripts": [
            "grolt = grolt.__main__:grolt",
        ],
    },
    "packages": packages,
    "install_requires": [
        "certifi",
        "cryptography~=2.0; python_version<'3.6'",
        "cryptography~=3.0; python_version>='3.6'",
        "click<8.0; python_version<'3.6'",
        "click; python_version>='3.6'",
        "docker<5.0; python_version<'3.6'",
        "docker; python_version>='3.6'",
        "monotonic",
        "py2neo>=2021.1.4",
        "pyreadline>=2.1; platform_system=='Windows'",
        "six",
    ],
    "license": "Apache License, Version 2.0",
    "classifiers": [
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Topic :: Database",
        "Topic :: Software Development",
    ],
}

setup(**package_metadata)
