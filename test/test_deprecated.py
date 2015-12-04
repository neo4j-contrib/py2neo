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


from warnings import catch_warnings, simplefilter

from py2neo import Node, Relationship, Subgraph
from test.util import Py2neoTestCase


class DeprecatedTestCase(Py2neoTestCase):

    def setUp(self):
        simplefilter("always")
        self._catcher = catch_warnings(record=True)
        self.warnings = self._catcher.__enter__()

    def tearDown(self):
        assert issubclass(self.warnings[-1].category, DeprecationWarning)
        self._catcher.__exit__()


class PropertiesTestCase(DeprecatedTestCase):

    def test_node_properties(self):
        a = Node()
        _ = a.properties

    def test_relationship_properties(self):
        a = Node()
        b = Node()
        r = Relationship(a, "TO", b)
        _ = r.properties


class NodeMatchTestCase(DeprecatedTestCase):

    def setUp(self):
        super(NodeMatchTestCase, self).setUp()
        a = Node(name="Alice")
        b = Node(name="Bob")
        c = Node(name="Carol")
        d = Node(name="Dave")
        e = Node(name="Eve")
        self.graph.create(a | b | c | d | e)
        rels = (
            Relationship(a, "LOVES", b),
            Relationship(b, "LOVES", a),
            Relationship(b, "KNOWS", c),
            Relationship(b, "KNOWS", d),
            Relationship(d, "LOVES", e),
        )
        self.graph.create(Subgraph(*rels))
        self.sample_graph = a, b, c, d, e, rels

    def test_can_match_zero_outgoing(self):
        a, b, c, d, e, rels = self.sample_graph
        matches = list(e.match_outgoing())
        assert len(matches) == 0

    def test_can_match_one_outgoing(self):
        a, b, c, d, e, rels = self.sample_graph
        matches = list(a.match_outgoing())
        assert len(matches) == 1
        assert rels[0] in matches

    def test_can_match_many_outgoing(self):
        a, b, c, d, e, rels = self.sample_graph
        matches = list(b.match_outgoing())
        assert len(matches) == 3
        assert rels[1] in matches
        assert rels[2] in matches
        assert rels[3] in matches

    def test_can_match_many_outgoing_with_limit(self):
        a, b, c, d, e, rels = self.sample_graph
        matches = list(b.match_outgoing(limit=2))
        assert len(matches) == 2
        for match in matches:
            assert match in (rels[1], rels[2], rels[3])

    def test_can_match_many_outgoing_by_type(self):
        a, b, c, d, e, rels = self.sample_graph
        matches = list(b.match_outgoing("KNOWS"))
        assert len(matches) == 2
        assert rels[2] in matches
        assert rels[3] in matches

    def test_can_match_many_outgoing_by_multiple_types(self):
        a, b, c, d, e, rels = self.sample_graph
        matches = list(b.match_outgoing(("KNOWS", "LOVES")))
        assert len(matches) == 3
        assert rels[1] in matches
        assert rels[2] in matches
        assert rels[3] in matches

    def test_can_match_many_in_both_directions(self):
        a, b, c, d, e, rels = self.sample_graph
        matches = list(b.match())
        assert len(matches) == 4
        assert rels[0] in matches
        assert rels[1] in matches
        assert rels[2] in matches
        assert rels[3] in matches

    def test_can_match_many_in_both_directions_with_limit(self):
        a, b, c, d, e, rels = self.sample_graph
        matches = list(b.match(limit=2))
        assert len(matches) == 2
        for match in matches:
            assert match in (rels[0], rels[1], rels[2], rels[3])

    def test_can_match_many_by_type_in_both_directions(self):
        a, b, c, d, e, rels = self.sample_graph
        matches = list(b.match("LOVES"))
        assert len(matches) == 2
        assert rels[0] in matches
        assert rels[1] in matches
