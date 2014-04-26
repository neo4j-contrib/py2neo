#/usr/bin/env python
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


from py2neo import neo4j, node


def test_can_cast_node(graph_db):
    alice, = graph_db.create({"name": "Alice"})
    casted = node(alice)
    assert isinstance(casted, neo4j.Node)
    assert not casted.is_abstract
    assert casted["name"] == "Alice"


def test_can_cast_dict():
    casted = node({"name": "Alice"})
    assert isinstance(casted, neo4j.Node)
    assert casted.is_abstract
    assert casted["name"] == "Alice"


def test_can_cast_kwargs():
    casted = node(name="Alice")
    assert isinstance(casted, neo4j.Node)
    assert casted.is_abstract
    assert casted["name"] == "Alice"


def test_can_cast_nothing():
    casted = node()
    assert isinstance(casted, neo4j.Node)
    assert casted.is_abstract
