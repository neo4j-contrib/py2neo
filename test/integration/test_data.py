#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2018, Nigel Small
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


from py2neo.data import Node, Relationship, Path
from py2neo.internal.compat import long
from py2neo.testing import IntegrationTestCase


KNOWS = Relationship.type("KNOWS")


class RelationshipTestCase(IntegrationTestCase):

    def test_can_get_relationship_by_id_when_cached(self):
        a = Node()
        b = Node()
        r = Relationship(a, "TO", b)
        self.graph.create(r)
        got = self.graph.relationships.get(r.identity)
        assert got is r

    def test_can_get_relationship_by_id_when_not_cached(self):
        a = Node()
        b = Node()
        r = Relationship(a, "TO", b)
        self.graph.create(r)
        self.graph.relationship_cache.clear()
        got = self.graph.relationships.get(r.identity)
        assert got.identity == r.identity

    def test_relationship_cache_is_thread_local(self):
        import threading
        a = Node()
        b = Node()
        r = Relationship(a, "TO", b)
        self.graph.create(r)
        assert r.identity in self.graph.relationship_cache
        other_relationship_cache_keys = []

        def check_cache():
            other_relationship_cache_keys.extend(self.graph.relationship_cache.keys())

        thread = threading.Thread(target=check_cache)
        thread.start()
        thread.join()

        assert r.identity in self.graph.relationship_cache
        assert r.identity not in other_relationship_cache_keys

    def test_cannot_get_relationship_by_id_when_id_does_not_exist(self):
        a = Node()
        b = Node()
        r = Relationship(a, "TO", b)
        self.graph.create(r)
        rel_id = r.identity
        self.graph.delete(r)
        with self.assertRaises(KeyError):
            _ = self.graph.relationships[rel_id]

    def test_getting_no_relationships(self):
        alice = Node(name="Alice")
        self.graph.create(alice)
        rels = list(self.graph.match(nodes=[alice]))
        assert rels is not None
        assert isinstance(rels, list)
        assert len(rels) == 0

    def test_relationship_creation(self):
        a = Node()
        b = Node()
        r = Relationship(a, "TO", b)
        self.graph.create(r)
        self.assertEqual(r.graph, self.graph)
        self.assertIsNotNone(r.identity)

    def test_relationship_creation_on_existing_node(self):
        a = Node()
        self.graph.create(a)
        b = Node()
        r = Relationship(a, "TO", b)
        self.graph.create(r)
        self.assertEqual(r.graph, a.graph, self.graph)
        self.assertIsNotNone(r.identity)

    def test_only_one_relationship_in_a_relationship(self):
        rel = Relationship({}, "KNOWS", {})
        self.assertEqual(len(rel.relationships), 1)

    def test_relationship_equality_with_none(self):
        rel = Relationship({}, "KNOWS", {})
        none = None
        self.assertNotEqual(rel, none)

    def test_relationship_equality_for_concrete(self):
        a = Node()
        b = Node()
        r1 = Relationship(a, "KNOWS", b)
        r2 = Relationship(a, "KNOWS", b)
        self.graph.create(r1)
        self.graph.create(r2)
        self.assertEqual(r1, r2)

    def test_cannot_delete_uncreated_relationship(self):
        a = Node()
        b = Node()
        self.graph.create(a | b)
        r = Relationship(a, "TO", b)
        self.graph.delete(r)

    def test_relationship_exists(self):
        a = Node()
        b = Node()
        r = Relationship(a, "TO", b)
        self.graph.create(r)
        self.assertTrue(self.graph.exists(r))

    def test_relationship_does_not_exist(self):
        a = Node()
        b = Node()
        self.graph.create(a | b)
        r = Relationship(a, "TO", b)
        self.assertIsNot(r.graph, self.graph)
        self.assertFalse(self.graph.exists(r))

    def test_blank_type_automatically_updates(self):
        a = Node()
        b = Node()
        r = Relationship(a, "TO", b)
        self.graph.create(r)
        r._type = None
        self.assertIsNotNone(r.graph)
        self.assertIsNotNone(r.identity)
        self.assertIsNone(r._type)
        self.assertEqual(type(r).__name__, "TO")

    def test_cannot_cast_from_odd_object(self):
        with self.assertRaises(TypeError):
            _ = Relationship.cast(object())


class PathTestCase(IntegrationTestCase):

    def test_can_construct_simple_path(self):
        alice = Node(name="Alice")
        bob = Node(name="Bob")
        path = Path(alice, "KNOWS", bob)
        assert len(path.nodes) == 2
        assert len(path.relationships) == 1
        assert len(path) == 1

    def test_can_construct_path_with_none_node(self):
        alice = Node(name="Alice")
        path = Path(alice, "KNOWS", None)
        assert len(path.nodes) == 2
        assert len(path.relationships) == 1
        assert len(path) == 1

    def test_can_create_path(self):
        path = Path({"name": "Alice"}, "KNOWS", {"name": "Bob"})
        nodes = path.nodes
        assert len(path) == 1
        assert nodes[0]["name"] == "Alice"
        self.assertIs(type(path[0]), KNOWS)
        assert nodes[-1]["name"] == "Bob"
        path = Path(path, "KNOWS", {"name": "Carol"})
        nodes = path.nodes
        assert len(path) == 2
        assert nodes[0]["name"] == "Alice"
        self.assertIs(type(path[0]), KNOWS)
        assert nodes[1]["name"] == "Bob"
        path = Path({"name": "Zach"}, "KNOWS", path)
        nodes = path.nodes
        assert len(path) == 3
        assert nodes[0]["name"] == "Zach"
        self.assertIs(type(path[0]), KNOWS)
        assert nodes[1]["name"] == "Alice"
        self.assertIs(type(path[1]), KNOWS)
        assert nodes[2]["name"] == "Bob"

    def test_can_slice_path(self):
        a = Node(name="Alice")
        b = Node(name="Bob")
        c = Node(name="Carol")
        d = Node(name="Dave")
        e = Node(name="Eve")
        f = Node(name="Frank")
        path = Path(a, "KNOWS", b, "KNOWS", c, "KNOWS", d, "KNOWS", e, "KNOWS", f)
        assert len(path) == 5
        assert path[0] == Relationship(a, "KNOWS", b)
        assert path[1] == Relationship(b, "KNOWS", c)
        assert path[2] == Relationship(c, "KNOWS", d)
        assert path[-1] == Relationship(e, "KNOWS", f)
        assert path[0:2] == Path(a, "KNOWS", b, "KNOWS", c)
        assert path[3:5] == Path(d, "KNOWS", e, "KNOWS", f)
        assert path[:] == Path(a, "KNOWS", b, "KNOWS", c, "KNOWS", d, "KNOWS", e, "KNOWS", f)

    def test_can_iterate_path(self):
        a = Node(name="Alice")
        b = Node(name="Bob")
        c = Node(name="Carol")
        d = Node(name="Dave")
        e = Node(name="Eve")
        f = Node(name="Frank")
        path = Path(a, "KNOWS", b, "KNOWS", c, "KNOWS", d, "KNOWS", e, "KNOWS", f)
        assert list(path) == [
            Relationship(a, 'KNOWS', b),
            Relationship(b, 'KNOWS', c),
            Relationship(c, 'KNOWS', d),
            Relationship(d, 'KNOWS', e),
            Relationship(e, 'KNOWS', f),
        ]
        assert list(enumerate(path)) == [
            (0, Relationship(a, 'KNOWS', b)),
            (1, Relationship(b, 'KNOWS', c)),
            (2, Relationship(c, 'KNOWS', d)),
            (3, Relationship(d, 'KNOWS', e)),
            (4, Relationship(e, 'KNOWS', f))
        ]

    def test_can_join_paths(self):
        a = Node(name="Alice")
        b = Node(name="Bob")
        c = Node(name="Carol")
        d = Node(name="Dave")
        path1 = Path(a, "KNOWS", b)
        path2 = Path(c, "KNOWS", d)
        path = Path(path1, "KNOWS", path2)
        assert list(path) == [
            Relationship(a, 'KNOWS', b),
            Relationship(b, 'KNOWS', c),
            Relationship(c, 'KNOWS', d),
        ]

    def test_path_equality(self):
        alice = Node(name="Alice")
        bob = Node(name="Bob")
        carol = Node(name="Carol")
        dave = Node(name="Dave")
        path_1 = Path(alice, "LOVES", bob, Relationship(carol, "HATES", bob), carol, "KNOWS", dave)
        path_2 = Path(alice, "LOVES", bob, Relationship(carol, "HATES", bob), carol, "KNOWS", dave)
        assert path_1 == path_2

    def test_path_inequality(self):
        alice = Node(name="Alice")
        bob = Node(name="Bob")
        carol = Node(name="Carol")
        dave = Node(name="Dave")
        path_1 = Path(alice, "LOVES", bob, Relationship(carol, "HATES", bob), carol, "KNOWS", dave)
        path_2 = Path(alice, "KNOWS", carol, Relationship(dave, "KNOWS", carol), dave)
        assert path_1 != path_2
        assert path_1 != ""

    def test_path_in_several_ways(self):
        alice = Node(name="Alice")
        bob = Node(name="Bob")
        carol = Node(name="Carol")
        dave = Node(name="Dave")
        path = Path(alice, "LOVES", bob, Relationship(carol, "HATES", bob), carol, "KNOWS", dave)
        assert path.__bool__()
        assert path.__nonzero__()
        assert path[0] == Relationship(alice, "LOVES", bob)
        assert path[1] == Relationship(carol, "HATES", bob)
        assert path[2] == Relationship(carol, "KNOWS", dave)
        assert path[-1] == Relationship(carol, "KNOWS", dave)
        assert path[0:1] == Path(alice, "LOVES", bob)
        assert path[0:2] == Path(alice, "LOVES", bob, Relationship(carol, "HATES", bob), carol)
        try:
            _ = path[7]
        except IndexError:
            assert True
        else:
            assert False

    def test_path_direction(self):
        cypher = """\
        CREATE p=({name:'Alice'})-[:KNOWS]->({name:'Bob'})<-[:DISLIKES]-
                 ({name:'Carol'})-[:MARRIED_TO]->({name:'Dave'})
        RETURN p
        """
        path = self.graph.evaluate(cypher)
        assert path[0].start_node["name"] == "Alice"
        assert path[0].end_node["name"] == "Bob"
        assert path[1].start_node["name"] == "Carol"
        assert path[1].end_node["name"] == "Bob"
        assert path[2].start_node["name"] == "Carol"
        assert path[2].end_node["name"] == "Dave"


class CreatePathTestCase(IntegrationTestCase):

    def test_can_create_path(self):
        path = Path({"name": "Alice"}, "KNOWS", {"name": "Bob"})
        nodes = path.nodes
        assert dict(nodes[0]) == {"name": "Alice"}
        self.assertIs(type(path[0]), KNOWS)
        assert dict(nodes[1]) == {"name": "Bob"}
        self.graph.create(path)
        assert isinstance(nodes[0], Node)
        assert nodes[0]["name"] == "Alice"
        assert isinstance(path[0], Relationship)
        self.assertIs(type(path[0]), KNOWS)
        assert isinstance(nodes[1], Node)
        assert nodes[1]["name"] == "Bob"

    def test_can_create_path_with_rel_properties(self):
        path = Path({"name": "Alice"}, ("KNOWS", {"since": 1999}), {"name": "Bob"})
        nodes = path.nodes
        assert dict(nodes[0]) == {"name": "Alice"}
        self.assertIs(type(path[0]), KNOWS)
        assert dict(path[0]) == {"since": 1999}
        assert dict(nodes[1]) == {"name": "Bob"}
        self.graph.create(path)
        assert isinstance(nodes[0], Node)
        assert nodes[0]["name"] == "Alice"
        assert isinstance(path[0], Relationship)
        self.assertIs(type(path[0]), KNOWS)
        assert dict(path[0]) == {"since": 1999}
        assert isinstance(nodes[1], Node)
        assert nodes[1]["name"] == "Bob"


class PathIterationAndReversalTestCase(IntegrationTestCase):

    def setUp(self):
        self.alice = Node("Person", name="Alice", age=33)
        self.bob = Node("Person", name="Bob", age=44)
        self.carol = Node("Person", name="Carol", age=55)
        self.dave = Node("Person", name="Dave", age=66)

    def test_can_iterate_path_relationships(self):
        # given
        path = Path(self.alice, "LOVES", self.bob, Relationship(self.carol, "HATES", self.bob),
                    self.carol, "KNOWS", self.dave)
        # when
        rels = list(path)
        # then
        assert rels == [
            Relationship(self.alice, "LOVES", self.bob),
            Relationship(self.carol, "HATES", self.bob),
            Relationship(self.carol, "KNOWS", self.dave),
        ]

    def test_can_make_new_path_from_relationships(self):
        # given
        path = Path(self.alice, "LOVES", self.bob, Relationship(self.carol, "HATES", self.bob),
                    self.carol, "KNOWS", self.dave)
        rels = list(path)
        # when
        new_path = Path(*rels)
        # then
        new_rels = list(new_path)
        assert new_rels == [
            Relationship(self.alice, "LOVES", self.bob),
            Relationship(self.carol, "HATES", self.bob),
            Relationship(self.carol, "KNOWS", self.dave),
        ]

    def test_can_make_new_path_from_path(self):
        # given
        path = Path(self.alice, "LOVES", self.bob, Relationship(self.carol, "HATES", self.bob),
                    self.carol, "KNOWS", self.dave)
        # when
        new_path = Path(path)
        # then
        new_rels = list(new_path)
        assert new_rels == [
            Relationship(self.alice, "LOVES", self.bob),
            Relationship(self.carol, "HATES", self.bob),
            Relationship(self.carol, "KNOWS", self.dave),
        ]

    def test_can_reverse_iterate_path_relationships(self):
        # given
        path = Path(self.alice, "LOVES", self.bob, Relationship(self.carol, "HATES", self.bob),
                    self.carol, "KNOWS", self.dave)
        # when
        rels = list(reversed(path))
        # then
        assert rels == [
            Relationship(self.carol, "KNOWS", self.dave),
            Relationship(self.carol, "HATES", self.bob),
            Relationship(self.alice, "LOVES", self.bob),
        ]
