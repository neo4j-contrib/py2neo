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


from py2neo import neo4j


def test_can_cast_node(graph_db):
    alice, = graph_db.create({"name": "Alice"})
    casted = neo4j._cast(alice)
    assert isinstance(casted, neo4j.Node)
    assert not casted.is_abstract
    assert casted["name"] == "Alice"


def test_can_cast_dict():
    casted = neo4j._cast({"name": "Alice"})
    assert isinstance(casted, neo4j.Node)
    assert casted.is_abstract
    assert casted["name"] == "Alice"


def test_can_cast_rel(graph_db):
    a, b, ab = graph_db.create({}, {}, (0, "KNOWS", 1))
    casted = neo4j._cast(ab)
    assert isinstance(casted, neo4j.Relationship)
    assert not casted.is_abstract
    assert casted.start_node == a
    assert casted.type == "KNOWS"
    assert casted.end_node == b


def test_can_cast_3_tuple():
    casted = neo4j._cast(("Alice", "KNOWS", "Bob"))
    assert isinstance(casted, neo4j.Relationship)
    assert casted.is_abstract
    assert casted.start_node == "Alice"
    assert casted.type == "KNOWS"
    assert casted.end_node == "Bob"


def test_can_cast_4_tuple():
    casted = neo4j._cast(("Alice", "KNOWS", "Bob", {"since": 1999}))
    assert isinstance(casted, neo4j.Relationship)
    assert casted.is_abstract
    assert casted.start_node == "Alice"
    assert casted.type == "KNOWS"
    assert casted.end_node == "Bob"
    assert casted["since"] == 1999
