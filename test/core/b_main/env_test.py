#!/usr/bin/env python
# -*- encoding: utf-8 -*-

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


from py2neo.env import NEO4J_DIST, NEO4J_HOME, NEO4J_URI


def test_default_dist():
    assert NEO4J_DIST == "http://dist.neo4j.org/"


def test_neo4j_home():
    assert NEO4J_HOME


def test_neo4j_uri():
    assert NEO4J_URI
