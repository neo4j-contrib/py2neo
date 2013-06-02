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

from py2neo import neo4j, node, rel

import logging
import unittest

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    level=logging.DEBUG,
)


class NodeCastingTestCase(unittest.TestCase):

    def test_can_cast_node(self):
        graph_db = neo4j.GraphDatabaseService()
        alice, = graph_db.create({"name": "Alice"})
        casted = node(alice)
        assert isinstance(casted, neo4j.Node)
        assert not casted.is_abstract()
        assert casted["name"] == "Alice"

    def test_can_cast_dict(self):
        casted = node({"name": "Alice"})
        assert isinstance(casted, neo4j.Node)
        assert casted.is_abstract()
        assert casted["name"] == "Alice"

    def test_can_cast_args(self):
        casted = node("Person")
        assert isinstance(casted, neo4j.Node)
        assert casted.is_abstract()
        assert "Person" in casted._labels

    def test_can_cast_kwargs(self):
        casted = node(name="Alice")
        assert isinstance(casted, neo4j.Node)
        assert casted.is_abstract()
        assert casted["name"] == "Alice"

    def test_can_cast_args_and_kwargs(self):
        casted = node("Person", name="Alice")
        assert isinstance(casted, neo4j.Node)
        assert casted.is_abstract()
        assert "Person" in casted._labels
        assert casted["name"] == "Alice"

    def test_can_cast_nothing(self):
        casted = node()
        assert isinstance(casted, neo4j.Node)
        assert casted.is_abstract()


class RelCastingTestCase(unittest.TestCase):

    def test_can_cast_rel(self):
        graph_db = neo4j.GraphDatabaseService()
        a, b, ab = graph_db.create({}, {}, (0, "KNOWS", 1))
        casted = rel(ab)
        assert isinstance(casted, neo4j.Relationship)
        assert not casted.is_abstract()
        assert casted.start_node == a
        assert casted.type == "KNOWS"
        assert casted.end_node == b

    def test_cannot_cast_0_tuple(self):
        try:
            rel(())
            assert False
        except TypeError:
            assert True

    def test_cannot_cast_1_tuple(self):
        try:
            rel(("Alice",))
            assert False
        except TypeError:
            assert True

    def test_cannot_cast_2_tuple(self):
        try:
            rel(("Alice", "KNOWS"))
            assert False
        except TypeError:
            assert True

    def test_can_cast_3_tuple(self):
        casted = rel(("Alice", "KNOWS", "Bob"))
        assert isinstance(casted, neo4j.Relationship)
        assert casted.is_abstract()
        assert casted.start_node == "Alice"
        assert casted.type == "KNOWS"
        assert casted.end_node == "Bob"

    def test_can_cast_3_tuple_with_unbound_rel(self):
        casted = rel(("Alice", ("KNOWS", {"since": 1999}), "Bob"))
        assert isinstance(casted, neo4j.Relationship)
        assert casted.is_abstract()
        assert casted.start_node == "Alice"
        assert casted.type == "KNOWS"
        assert casted.end_node == "Bob"
        assert casted["since"] == 1999

    def test_can_cast_4_tuple(self):
        casted = rel(("Alice", "KNOWS", "Bob", {"since": 1999}))
        assert isinstance(casted, neo4j.Relationship)
        assert casted.is_abstract()
        assert casted.start_node == "Alice"
        assert casted.type == "KNOWS"
        assert casted.end_node == "Bob"
        assert casted["since"] == 1999

    def test_cannot_cast_6_tuple(self):
        try:
            rel(("Alice", "KNOWS", "Bob", "foo", "bar", "baz"))
            assert False
        except TypeError:
            assert True

    def test_cannot_cast_0_args(self):
        try:
            rel()
            assert False
        except TypeError:
            assert True

    def test_cannot_cast_1_arg(self):
        try:
            rel("Alice")
            assert False
        except TypeError:
            assert True

    def test_cannot_cast_2_args(self):
        try:
            rel("Alice", "KNOWS")
            assert False
        except TypeError:
            assert True

    def test_can_cast_3_args(self):
        casted = rel("Alice", "KNOWS", "Bob")
        assert isinstance(casted, neo4j.Relationship)
        assert casted.is_abstract()
        assert casted.start_node == "Alice"
        assert casted.type == "KNOWS"
        assert casted.end_node == "Bob"

    def test_can_cast_kwargs(self):
        casted = rel("Alice", "KNOWS", "Bob", since=1999)
        assert isinstance(casted, neo4j.Relationship)
        assert casted.is_abstract()
        assert casted.start_node == "Alice"
        assert casted.type == "KNOWS"
        assert casted.end_node == "Bob"
        assert casted["since"] == 1999


class EntityCastingTestCase(unittest.TestCase):

    def test_can_cast_node(self):
        graph_db = neo4j.GraphDatabaseService()
        alice, = graph_db.create({"name": "Alice"})
        casted = neo4j._cast(alice)
        assert isinstance(casted, neo4j.Node)
        assert not casted.is_abstract()
        assert casted["name"] == "Alice"

    def test_can_cast_dict(self):
        casted = neo4j._cast({"name": "Alice"})
        assert isinstance(casted, neo4j.Node)
        assert casted.is_abstract()
        assert casted["name"] == "Alice"

    def test_can_cast_rel(self):
        graph_db = neo4j.GraphDatabaseService()
        a, b, ab = graph_db.create({}, {}, (0, "KNOWS", 1))
        casted = neo4j._cast(ab)
        assert isinstance(casted, neo4j.Relationship)
        assert not casted.is_abstract()
        assert casted.start_node == a
        assert casted.type == "KNOWS"
        assert casted.end_node == b

    def test_can_cast_3_tuple(self):
        casted = neo4j._cast(("Alice", "KNOWS", "Bob"))
        assert isinstance(casted, neo4j.Relationship)
        assert casted.is_abstract()
        assert casted.start_node == "Alice"
        assert casted.type == "KNOWS"
        assert casted.end_node == "Bob"

    def test_can_cast_4_tuple(self):
        casted = neo4j._cast(("Alice", "KNOWS", "Bob", {"since": 1999}))
        assert isinstance(casted, neo4j.Relationship)
        assert casted.is_abstract()
        assert casted.start_node == "Alice"
        assert casted.type == "KNOWS"
        assert casted.end_node == "Bob"
        assert casted["since"] == 1999


if __name__ == '__main__':
    unittest.main()
