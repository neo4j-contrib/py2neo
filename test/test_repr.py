#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2015, Nigel Small
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


import re

from py2neo.types import Node, Relationship, Subgraph, Walkable, \
    entity_name, set_entity_name_property_key
from test.util import Py2neoTestCase


class EntityNameTestCase(Py2neoTestCase):

    def test_empty_node(self):
        a = Node()
        name = entity_name(a)
        assert name.startswith("_")

    def test_node_with_name_metadata(self):
        a = Node()
        a.__name__ = "alice"
        name = entity_name(a)
        assert name == "alice"

    def test_node_with_short_name_property(self):
        a = Node(name="Alice")
        name = entity_name(a)
        assert name == "alice"

    def test_node_with_long_name_property(self):
        a = Node(name="Alice Smith")
        name = entity_name(a)
        assert name == "alice_smith"

    def test_bound_node_with_no_name_property(self):
        a = Node()
        self.graph.create(a)
        name = entity_name(a)
        assert name.startswith("a")
        i = int(name[1:])
        assert i == a.resource._id

    def test_bound_node_with_name_property(self):
        a = Node(name="Alice")
        self.graph.create(a)
        name = entity_name(a)
        assert name.startswith("a")
        i = int(name[1:])
        assert i == a.resource._id

    def test_empty_relationship(self):
        a = Node()
        b = Node()
        r = Relationship(a, "TO", b)
        name = entity_name(r)
        assert name.startswith("_")

    def test_relationship_with_name_metadata(self):
        a = Node()
        b = Node()
        r = Relationship(a, "TO", b)
        r.__name__ = "foo"
        name = entity_name(r)
        assert name == "foo"

    def test_bound_relationship_with_no_name_property(self):
        a = Node()
        b = Node()
        r = Relationship(a, "TO", b)
        self.graph.create(r)
        name = entity_name(r)
        assert name.startswith("r")
        i = int(name[1:])
        assert i == r.resource._id

    def test_alternative_property_key(self):
        try:
            set_entity_name_property_key("email")
            a = Node(email="alice@example.com")
            name = entity_name(a)
            assert name == "alice@example.com"
        finally:
            set_entity_name_property_key("name")


class ReprTestCase(Py2neoTestCase):

    def test_node_repr(self):
        a = Node("Person", name="Alice")
        assert re.match(r'\(_?[0-9A-Za-z]+:Person \{name:"Alice"\}\)', repr(a))
        self.graph.create(a)
        assert re.match(r'\(a[0-9]+:Person \{name:"Alice"\}\)', repr(a))

    def test_relationship_repr(self):
        a = Node()
        b = Node()
        ab = Relationship(a, "KNOWS", b, since=1999)
        assert re.match(r'\(.*\)-\[:KNOWS \{since:1999\}\]->\(.*\)', repr(ab))

    def test_subgraph_repr(self):
        a = Node("Person", name="Alice")
        b = Node("Person", name="Bob")
        ab = Relationship(a, "TO", b)
        ba = Relationship(b, "FROM", a)
        s = ab | ba
        assert isinstance(s, Subgraph)
        r = repr(s)
        assert r.startswith("{")
        assert r.endswith("}")
        items = [item.strip() for item in r[1:-1].split(",")]
        assert len(items) == 4
        for i, item in enumerate(items):
            if 0 <= i < 2:
                assert re.match(r'\(_?[0-9A-Za-z]+:Person \{name:"(Alice|Bob)"\}\)', item)
            else:
                assert re.match(r'\(.*\)-\[:(TO|FROM)\]->\(.*\)', repr(ab))

    def test_walkable_repr(self):
        a = Node("Person", name="Alice")
        b = Node("Person", name="Bob")
        c = Node("Person", name="Carol")
        d = Node("Person", name="Dave")
        ab = Relationship(a, "LOVES", b)
        cb = Relationship(c, "HATES", b)
        cd = Relationship(c, "KNOWS", d)
        t = Walkable([a, ab, b, cb, c, cd, d])
        r = repr(t)
        expected = "(%s)-[:LOVES]->(%s)<-[:HATES]-(%s)-[:KNOWS]->(%s)" % (
            entity_name(a), entity_name(b), entity_name(c), entity_name(d))
        assert r == expected
