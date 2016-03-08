#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2016, Nigel Small
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

from py2neo import Node, Relationship, Path
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

    def test_pull_node_properties_if_stale(self):
        a = Node(foo="bar")
        self.graph.create(a)
        a["foo"] = None
        a._Node__stale.add("properties")
        properties = a.properties
        assert properties == {"foo": "bar"}

    def test_pull_relationship_properties_if_stale(self):
        a = Node()
        b = Node()
        ab = Relationship(a, "TO", b, foo="bar")
        self.graph.create(ab)
        ab["foo"] = None
        ab._Relationship__stale.add("properties")
        properties = ab.properties
        assert properties == {"foo": "bar"}


class ExistsTestCase(DeprecatedTestCase):

    def test_node_exists(self):
        a = Node()
        self.graph.create(a)
        _ = a.exists()

    def test_relationship_exists(self):
        a = Node()
        b = Node()
        ab = Relationship(a, "TO", b)
        self.graph.create(ab)
        _ = ab.exists()


class DegreeTestCase(DeprecatedTestCase):

    def test_node_degree(self):
        alice = Node("Person", name="Alice")
        bob = Node("Person", name="Bob")
        carol = Node("Person", name="Carol")
        self.graph.create(alice)
        assert alice.degree() == 0
        self.graph.create(Path(alice, "KNOWS", bob))
        assert alice.degree() == 1
        self.graph.create(Path(alice, "KNOWS", carol))
        assert alice.degree() == 2
        self.graph.create(Path(carol, "KNOWS", alice))
        assert alice.degree() == 3


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
        self.graph.create(rels[0] | rels[1] | rels[2] | rels[3] | rels[4])
        self.sample_graph = a, b, c, d, e, rels

    def test_can_match_zero_outgoing(self):
        a, b, c, d, e, rels = self.sample_graph
        matches = list(e.match_outgoing())
        assert len(matches) == 0

    def test_can_match_one_incoming(self):
        a, b, c, d, e, rels = self.sample_graph
        matches = list(a.match_incoming())
        assert len(matches) == 1
        assert rels[1] in matches

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


class PullTestCase(DeprecatedTestCase):

    def test_can_pull_node(self):
        alpha = Node("Person", name="Alice")
        beta = Node()
        self.graph.create(alpha)
        assert set(beta.labels()) == set()
        assert dict(beta) == {}
        beta._set_remote(alpha.remote.uri)
        beta.pull()
        assert set(beta.labels()) == set(alpha.labels())
        assert dict(beta) == dict(alpha)

    def test_can_pull_relationship(self):
        a = Node()
        b = Node()
        alpha = Relationship(a, "TO", b, since=1999)
        beta = Relationship(a, "TO", b)
        self.graph.create(alpha)
        assert dict(beta) == {}
        beta._set_remote(alpha.remote.uri)
        beta.pull()
        assert dict(beta) == dict(alpha)


class PushTestCase(DeprecatedTestCase):

    def test_can_push_node(self):
        local = Node("Person", name="Alice")
        remote = Node()
        self.graph.create(remote)
        assert set(remote.labels()) == set()
        assert dict(remote) == {}
        local._set_remote(remote.remote.uri)
        local.push()
        remote.pull()
        assert set(local.labels()) == set(remote.labels())
        assert dict(local) == dict(remote)

    def test_can_push_relationship(self):
        a = Node()
        b = Node()
        ab = Relationship(a, "KNOWS", b)
        self.graph.create(ab)
        value = self.graph.evaluate("MATCH ()-[ab:KNOWS]->() WHERE id(ab)={i} "
                                    "RETURN ab.since", i=ab)
        assert value is None
        ab["since"] = 1999
        ab.push()
        value = self.graph.evaluate("MATCH ()-[ab:KNOWS]->() WHERE id(ab)={i} "
                                    "RETURN ab.since", i=ab)
        assert value == 1999


class GraphTestCase(DeprecatedTestCase):

    def test_neo4j_version_format(self):
        version = self.graph.neo4j_version
        assert isinstance(version, tuple)
        assert 3 <= len(version) <= 4
        assert isinstance(version[0], int)
        assert isinstance(version[1], int)
        assert isinstance(version[2], int)


class TransactionTestCase(DeprecatedTestCase):

    def test_append(self):
        with self.graph.begin() as tx:
            tx.append("RETURN 1")
