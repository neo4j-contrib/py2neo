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

from py2neo.cypher import base62
from py2neo.graph import Node, Relationship, Subgraph, Walkable
from test.util import Py2neoTestCase


class ReprTestCase(Py2neoTestCase):

    def test_node_repr(self):
        a = Node("Person", name="Alice")
        assert re.match(r'\(_[0-9A-Za-z]+:Person \{name:"Alice"\}\)', repr(a))
        self.graph.create(a)
        assert re.match(r'\(a[0-9]+:Person \{name:"Alice"\}\)', repr(a))

    def test_relationship_repr(self):
        a = Node()
        b = Node()
        ab = Relationship(a, "KNOWS", b, since=1999)
        assert re.match(r'\(.*\)-\[_[0-9A-Za-z]+:KNOWS \{since:1999\}\]->\(.*\)', repr(ab))
        self.graph.create(ab)
        assert re.match(r'\(.*\)-\[r[0-9]+:KNOWS \{since:1999\}\]->\(.*\)', repr(ab))

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
                assert re.match(r'\(_[0-9A-Za-z]+:Person \{name:"(Alice|Bob)"\}\)', item)
            else:
                assert re.match(r'\(.*\)-\[_[0-9A-Za-z]+:(TO|FROM)\]->\(.*\)', repr(ab))

    def test_walkable_repr(self):
        a = Node("Person", name="Alice")
        b = Node("Person", name="Bob")
        c = Node("Person", name="Carol")
        d = Node("Person", name="Dave")
        ab = Relationship(a, "LOVES", b)
        cb = Relationship(c, "HATES", b)
        cd = Relationship(c, "KNOWS", d)
        t = Walkable(a, ab, b, cb, c, cd, d)
        r = repr(t)
        expected = "(_%s)-[_%s:LOVES]->(_%s)<-[_%s:HATES]-(_%s)-[_%s:KNOWS]->" \
                   "(_%s)" % (base62(id(a)), base62(id(t[0])),
                              base62(id(b)), base62(id(t[1])),
                              base62(id(c)), base62(id(t[2])),
                              base62(id(d)))
        assert r == expected
