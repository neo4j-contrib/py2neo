#/usr/bin/env python
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

from py2neo import neo4j, rel


def test_can_cast_rel():
    graph_db = neo4j.GraphDatabaseService()
    a, b, ab = graph_db.create({}, {}, (0, "KNOWS", 1))
    casted = rel(ab)
    assert isinstance(casted, neo4j.Relationship)
    assert not casted.is_abstract
    assert casted.start_node == a
    assert casted.type == "KNOWS"
    assert casted.end_node == b


def test_cannot_cast_0_tuple():
    try:
        rel(())
        assert False
    except TypeError:
        assert True


def test_cannot_cast_1_tuple():
    try:
        rel(("Alice",))
        assert False
    except TypeError:
        assert True


def test_cannot_cast_2_tuple():
    try:
        rel(("Alice", "KNOWS"))
        assert False
    except TypeError:
        assert True


def test_can_cast_3_tuple():
    casted = rel(("Alice", "KNOWS", "Bob"))
    assert isinstance(casted, neo4j.Relationship)
    assert casted.is_abstract
    assert casted.start_node == "Alice"
    assert casted.type == "KNOWS"
    assert casted.end_node == "Bob"


def test_can_cast_3_tuple_with_unbound_rel():
    casted = rel(("Alice", ("KNOWS", {"since": 1999}), "Bob"))
    assert isinstance(casted, neo4j.Relationship)
    assert casted.is_abstract
    assert casted.start_node == "Alice"
    assert casted.type == "KNOWS"
    assert casted.end_node == "Bob"
    assert casted["since"] == 1999


def test_can_cast_4_tuple():
    casted = rel(("Alice", "KNOWS", "Bob", {"since": 1999}))
    assert isinstance(casted, neo4j.Relationship)
    assert casted.is_abstract
    assert casted.start_node == "Alice"
    assert casted.type == "KNOWS"
    assert casted.end_node == "Bob"
    assert casted["since"] == 1999


def test_cannot_cast_6_tuple():
    try:
        rel(("Alice", "KNOWS", "Bob", "foo", "bar", "baz"))
        assert False
    except TypeError:
        assert True


def test_cannot_cast_0_args():
    try:
        rel()
        assert False
    except TypeError:
        assert True


def test_cannot_cast_1_arg():
    try:
        rel("Alice")
        assert False
    except TypeError:
        assert True


def test_cannot_cast_2_args():
    try:
        rel("Alice", "KNOWS")
        assert False
    except TypeError:
        assert True


def test_can_cast_3_args():
    casted = rel("Alice", "KNOWS", "Bob")
    assert isinstance(casted, neo4j.Relationship)
    assert casted.is_abstract
    assert casted.start_node == "Alice"
    assert casted.type == "KNOWS"
    assert casted.end_node == "Bob"


def test_can_cast_kwargs():
    casted = rel("Alice", "KNOWS", "Bob", since=1999)
    assert isinstance(casted, neo4j.Relationship)
    assert casted.is_abstract
    assert casted.start_node == "Alice"
    assert casted.type == "KNOWS"
    assert casted.end_node == "Bob"
    assert casted["since"] == 1999
