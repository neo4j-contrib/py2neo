#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2017, Nigel Small
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

from py2neo.types import Node, Relationship, Subgraph, Walkable
from test.util import GraphTestCase


class ReprTestCase(GraphTestCase):

    def test_node_repr(self):
        a = Node("Person", name="Alice")
        assert repr(a) == "(:Person {name: 'Alice'})"
        self.graph.create(a)
        assert repr(a) == "(:Person {name: 'Alice'})"

    def test_relationship_repr(self):
        a = Node("Person", name="Alice")
        b = Node("Person", name="Bob")
        ab = Relationship(a, "KNOWS", b, since=1999)
        assert repr(ab) == "(Alice)-[:KNOWS {since: 1999}]->(Bob)"

    def test_subgraph_repr(self):
        a = Node("Person", name="Alice")
        b = Node("Person", name="Bob")
        ab = Relationship(a, "TO", b)
        ba = Relationship(b, "FROM", a)
        s = ab | ba
        assert isinstance(s, Subgraph)
        r = repr(s)
        assert r.startswith("({")
        assert r.endswith("})")
        nodes, _, relationships = r[2:-2].partition("}, {")
        items = [item.strip() for item in nodes.split(",")]
        assert len(items) == 2
        for i, item in enumerate(items):
            assert re.match(r"\(:Person \{name: '(Alice|Bob)'\}\)", item)
        items = [item.strip() for item in relationships.split(",")]
        assert len(items) == 2
        for _ in items:
            assert re.match(r'\(.*\)-\[:(TO|FROM) \{\}\]->\(.*\)', repr(ab))

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
        expected = "(Alice)-[:LOVES {}]->(Bob)<-[:HATES {}]-(Carol)-[:KNOWS {}]->(Dave)"
        assert r == expected
