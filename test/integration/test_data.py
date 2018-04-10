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


from py2neo.data import order, size, Node, Relationship, Path
from py2neo.internal.compat import long
from py2neo.testing import IntegrationTestCase


KNOWS = Relationship.type("KNOWS")


class TableTestCase(IntegrationTestCase):

    def test_simple_usage(self):
        table = self.graph.run("UNWIND range(1, 3) AS n RETURN n, n * n AS n_sq").to_table()
        self.assertEqual(table.keys(), ["n", "n_sq"])
        name_field = table.field(0)
        self.assertEqual(name_field["type"], int)
        self.assertEqual(name_field["optional"], False)
        age_field = table.field(1)
        self.assertEqual(age_field["type"], int)
        self.assertEqual(age_field["optional"], False)


class NodeTestCase(IntegrationTestCase):

    def test_can_create_local_node(self):
        a = Node("Person", name="Alice", age=33)
        assert set(a.labels) == {"Person"}
        assert dict(a) == {"name": "Alice", "age": 33}

    def test_can_create_remote_node(self):
        a = Node("Person", name="Alice", age=33)
        self.graph.create(a)
        assert set(a.labels) == {"Person"}
        assert dict(a) == {"name": "Alice", "age": 33}

    def test_bound_node_equals_unbound_node_with_same_properties(self):
        alice_1 = Node(name="Alice")
        alice_1.graph = self.graph
        alice_1.identity = 999
        alice_2 = Node(name="Alice")
        assert set(alice_1.labels) == set(alice_2.labels)
        assert dict(alice_1) == dict(alice_2)

    def test_bound_node_equality(self):
        alice_1 = Node(name="Alice")
        alice_1.graph = self.graph
        alice_1.identity = 999
        self.graph.node_cache.clear()
        alice_2 = Node(name="Alice")
        alice_2.graph = alice_1.graph
        alice_2.identity = alice_1.identity
        assert alice_1 == alice_2

    def test_unbound_node_equality(self):
        alice_1 = Node("Person", name="Alice")
        alice_2 = Node("Person", name="Alice")
        assert set(alice_1.labels) == set(alice_2.labels)
        assert dict(alice_1) == dict(alice_2)

    def test_can_merge_unsaved_changes_when_querying_node(self):
        a = Node("Person", name="Alice")
        b = Node()
        self.graph.create(a | b | Relationship(a, "KNOWS", b))
        assert dict(a) == {"name": "Alice"}
        a["age"] = 33
        assert dict(a) == {"name": "Alice", "age": 33}
        _ = list(self.graph.match(a, "KNOWS"))
        assert dict(a) == {"name": "Alice", "age": 33}

    def test_pull_node_labels_if_stale(self):
        a = Node("Thing")
        self.graph.create(a)
        a.remove_label("Thing")
        a._stale.add("labels")
        labels = a.labels
        assert set(labels) == {"Thing"}

    def test_pull_node_property_if_stale(self):
        a = Node(foo="bar")
        self.graph.create(a)
        a["foo"] = None
        a._stale.add("properties")
        assert a["foo"] == "bar"


class ConcreteNodeTestCase(IntegrationTestCase):

    def test_can_create_concrete_node(self):
        alice = Node.cast({"name": "Alice", "age": 34})
        self.graph.create(alice)
        assert isinstance(alice, Node)
        assert alice["name"] == "Alice"
        assert alice["age"] == 34

    def test_all_property_types(self):
        data = {
            "nun": None,
            "yes": True,
            "no": False,
            "int": 42,
            "float": 3.141592653589,
            "long": long("9223372036854775807"),
            "str": "hello, world",
            "unicode": u"hello, world",
            "boolean_list": [True, False, True, True, False],
            "int_list": [1, 1, 2, 3, 5, 8, 13, 21, 35],
            "str_list": ["red", "orange", "yellow", "green", "blue", "indigo", "violet"]
        }
        foo = Node.cast(data)
        self.graph.create(foo)
        for key, value in data.items():
            self.assertEqual(foo[key], value)

    def test_node_hashes(self):
        node_1 = Node("Person", name="Alice")
        node_1.graph = self.graph
        node_1.identity = 999
        node_2 = Node("Person", name="Alice")
        node_2.graph = node_1.graph
        node_2.identity = node_1.identity
        assert node_1 is not node_2
        assert hash(node_1) == hash(node_2)

    def test_cannot_delete_uncreated_node(self):
        a = Node()
        self.graph.delete(a)

    def test_node_exists(self):
        a = Node()
        self.graph.create(a)
        self.assertTrue(self.graph.exists(a))

    def test_node_does_not_exist(self):
        a = Node()
        self.assertFalse(self.graph.exists(a))


class NodeAutoNamingTestCase(IntegrationTestCase):

    def test_can_name_using_name_property(self):
        a = Node(name="Alice")
        self.assertEqual(a.__name__, "Alice")

    def test_can_name_using_magic_name_property(self):
        a = Node(__name__="Alice")
        self.assertEqual(a.__name__, "Alice")


class RelationshipTestCase(IntegrationTestCase):

    def test_can_get_all_relationship_types(self):
        types = self.graph.relationship_types
        assert isinstance(types, frozenset)

    def test_can_get_relationship_by_id_when_cached(self):
        a = Node()
        b = Node()
        r = Relationship(a, "TO", b)
        self.graph.create(r)
        got = self.graph.relationship(r.identity)
        assert got is r

    def test_can_get_relationship_by_id_when_not_cached(self):
        a = Node()
        b = Node()
        r = Relationship(a, "TO", b)
        self.graph.create(r)
        self.graph.relationship_cache.clear()
        got = self.graph.relationship(r.identity)
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
        self.graph.relationship_cache.clear()
        with self.assertRaises(IndexError):
            _ = self.graph.relationship(rel_id)

    def test_getting_no_relationships(self):
        alice = Node(name="Alice")
        self.graph.create(alice)
        rels = list(self.graph.match(alice))
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

    def test_relationship_degree(self):
        a = Node()
        b = Node()
        r = Relationship(a, "TO", b)
        self.graph.create(r)
        self.assertEqual(self.graph.degree(r), 1)

    def test_only_one_relationship_in_a_relationship(self):
        rel = Relationship({}, "KNOWS", {})
        assert size(rel) == 1

    def test_relationship_equality_with_none(self):
        rel = Relationship({}, "KNOWS", {})
        none = None
        assert rel != none

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
        self.assertNotIn(r, self.graph)
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
        assert order(path) == 2
        assert size(path) == 1
        assert len(path) == 1

    def test_can_construct_path_with_none_node(self):
        alice = Node(name="Alice")
        path = Path(alice, "KNOWS", None)
        assert order(path) == 2
        assert size(path) == 1
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


class MergeNodeTestCase(IntegrationTestCase):

    def setUp(self):
        self.graph.delete_all()

    def test_can_merge_node_that_does_not_exist(self):
        alice = Node("Person", name="Alice")
        old_order = order(self.graph)
        self.graph.merge(alice)
        self.assertEqual(alice.graph, self.graph)
        self.assertIsNotNone(alice.identity)
        assert self.graph.exists(alice)
        new_order = order(self.graph)
        assert new_order == old_order + 1

    def test_can_merge_node_that_does_exist(self):
        self.graph.create(Node("Person", name="Alice"))
        alice = Node("Person", name="Alice")
        old_order = order(self.graph)
        self.graph.merge(alice)
        self.assertEqual(alice.graph, self.graph)
        self.assertIsNotNone(alice.identity)
        assert self.graph.exists(alice)
        new_order = order(self.graph)
        assert new_order == old_order

    def test_can_merge_bound_node(self):
        alice = Node("Person", name="Alice")
        self.graph.create(alice)
        old_order = order(self.graph)
        self.graph.merge(alice)
        self.assertEqual(alice.graph, self.graph)
        self.assertIsNotNone(alice.identity)
        assert self.graph.exists(alice)
        new_order = order(self.graph)
        assert new_order == old_order

    def test_can_merge_node_without_label(self):
        node = Node()
        old_order = order(self.graph)
        self.graph.merge(node)
        self.assertEqual(node.graph, self.graph)
        self.assertIsNotNone(node.identity)
        assert self.graph.exists(node)
        new_order = order(self.graph)
        assert new_order == old_order + 1

    def test_can_merge_with_label_node_without_label(self):
        node = Node()
        old_order = order(self.graph)
        self.graph.merge(node, "Person")
        self.assertEqual(node.graph, self.graph)
        self.assertIsNotNone(node.identity)
        assert self.graph.exists(node)
        new_order = order(self.graph)
        assert new_order == old_order + 1

    def test_can_merge_node_that_does_not_exist_on_specific_label_and_key(self):
        alice = Node("Person", "Employee", name="Alice", age=33)
        old_order = order(self.graph)
        self.graph.merge(alice, "Person", "name")
        self.assertEqual(alice.graph, self.graph)
        self.assertIsNotNone(alice.identity)
        assert self.graph.exists(alice)
        new_order = order(self.graph)
        assert new_order == old_order + 1

    def test_can_merge_node_that_does_exist_on_specific_label_and_key_with_extra_properties(self):
        self.graph.create(Node("Person", name="Alice"))
        alice = Node("Person", "Employee", name="Alice", age=33)
        old_order = order(self.graph)
        self.graph.merge(alice, "Person", "name")
        self.assertEqual(alice.graph, self.graph)
        self.assertIsNotNone(alice.identity)
        assert self.graph.exists(alice)
        new_order = order(self.graph)
        assert new_order == old_order

    def test_can_merge_node_that_does_exist_on_specific_label_and_key_with_other_properties(self):
        self.graph.create(Node("Person", name="Alice", age=44))
        alice = Node("Person", "Employee", name="Alice", age=33)
        old_order = order(self.graph)
        self.graph.merge(alice, "Person", "name")
        self.assertEqual(alice.graph, self.graph)
        self.assertIsNotNone(alice.identity)
        assert self.graph.exists(alice)
        new_order = order(self.graph)
        assert new_order == old_order


class MergeRelationshipTestCase(IntegrationTestCase):

    def setUp(self):
        self.graph.delete_all()

    def test_can_merge_relationship_that_does_not_exist(self):
        alice = Node("Person", name="Alice")
        bob = Node("Person", name="Bob")
        ab = Relationship(alice, "KNOWS", bob)
        old_order = order(self.graph)
        old_size = size(self.graph)
        self.graph.merge(ab)
        self.assertEqual(alice.graph, self.graph)
        self.assertIsNotNone(alice.identity)
        self.assertEqual(bob.graph, self.graph)
        self.assertIsNotNone(bob.identity)
        self.assertEqual(ab.graph, self.graph)
        self.assertIsNotNone(ab.identity)
        assert self.graph.exists(alice | bob | ab)
        new_order = order(self.graph)
        new_size = size(self.graph)
        assert new_order == old_order + 2
        assert new_size == old_size + 1

    def test_can_merge_relationship_where_one_node_exists(self):
        alice = Node("Person", name="Alice")
        self.graph.create(alice)
        bob = Node("Person", name="Bob")
        ab = Relationship(alice, "KNOWS", bob)
        old_order = order(self.graph)
        old_size = size(self.graph)
        self.graph.merge(ab)
        self.assertEqual(alice.graph, self.graph)
        self.assertIsNotNone(alice.identity)
        self.assertEqual(bob.graph, self.graph)
        self.assertIsNotNone(bob.identity)
        self.assertEqual(ab.graph, self.graph)
        self.assertIsNotNone(ab.identity)
        assert self.graph.exists(alice | bob | ab)
        new_order = order(self.graph)
        new_size = size(self.graph)
        assert new_order == old_order + 1
        assert new_size == old_size + 1

    def test_can_merge_relationship_where_all_exist(self):
        alice = Node("Person", name="Alice")
        self.graph.create(Relationship(alice, "KNOWS", Node("Person", name="Bob")))
        bob = Node("Person", name="Bob")
        ab = Relationship(alice, "KNOWS", bob)
        old_order = order(self.graph)
        old_size = size(self.graph)
        self.graph.merge(ab)
        self.assertEqual(alice.graph, self.graph)
        self.assertIsNotNone(alice.identity)
        self.assertEqual(bob.graph, self.graph)
        self.assertIsNotNone(bob.identity)
        self.assertEqual(ab.graph, self.graph)
        self.assertIsNotNone(ab.identity)
        assert self.graph.exists(alice | bob | ab)
        new_order = order(self.graph)
        new_size = size(self.graph)
        assert new_order == old_order
        assert new_size == old_size


class MergeSubgraphTestCase(IntegrationTestCase):

    def setUp(self):
        self.graph.delete_all()

    def test_cannot_merge_non_subgraph(self):
        with self.assertRaises(TypeError):
            self.graph.merge("this string is definitely not a subgraph")

    def test_can_merge_three_nodes_where_none_exist(self):
        alice = Node("Person", name="Alice")
        bob = Node("Person", name="Bob")
        carol = Node("Person", name="Carol")
        old_order = order(self.graph)
        subgraph = alice | bob | carol
        self.graph.merge(subgraph)
        for node in subgraph.nodes:
            self.assertEqual(node.graph, self.graph)
            self.assertIsNotNone(node.identity)
            assert self.graph.exists(node)
        new_order = order(self.graph)
        assert new_order == old_order + 3

    def test_can_merge_three_nodes_where_one_exists(self):
        alice = Node("Person", name="Alice")
        bob = Node("Person", name="Bob")
        carol = Node("Person", name="Carol")
        self.graph.create(alice)
        old_order = order(self.graph)
        subgraph = alice | bob | carol
        self.graph.merge(subgraph)
        for node in subgraph.nodes:
            self.assertEqual(node.graph, self.graph)
            self.assertIsNotNone(node.identity)
            assert self.graph.exists(node)
        new_order = order(self.graph)
        assert new_order == old_order + 2

    def test_can_merge_three_nodes_where_two_exist(self):
        alice = Node("Person", name="Alice")
        bob = Node("Person", name="Bob")
        carol = Node("Person", name="Carol")
        self.graph.create(alice | bob)
        old_order = order(self.graph)
        subgraph = alice | bob | carol
        self.graph.merge(subgraph)
        for node in subgraph.nodes:
            self.assertEqual(node.graph, self.graph)
            self.assertIsNotNone(node.identity)
            assert self.graph.exists(node)
        new_order = order(self.graph)
        assert new_order == old_order + 1

    def test_can_merge_three_nodes_where_three_exist(self):
        alice = Node("Person", name="Alice")
        bob = Node("Person", name="Bob")
        carol = Node("Person", name="Carol")
        self.graph.create(alice | bob | carol)
        old_order = order(self.graph)
        subgraph = alice | bob | carol
        self.graph.merge(subgraph)
        for node in subgraph.nodes:
            self.assertEqual(node.graph, self.graph)
            self.assertIsNotNone(node.identity)
            assert self.graph.exists(node)
        new_order = order(self.graph)
        assert new_order == old_order


class MergeWalkableTestCase(IntegrationTestCase):

    def setUp(self):
        self.graph.delete_all()

    def test_can_merge_long_straight_walkable(self):
        a = Node("Person", name="Alice")
        b = Node("Person", name="Bob")
        c = Node("Person", name="Carol")
        d = Node("Person", name="Dave")
        ab = Relationship(a, "KNOWS", b)
        cb = Relationship(c, "KNOWS", b)
        cd = Relationship(c, "KNOWS", d)
        self.graph.create(a)
        old_order = order(self.graph)
        old_size = size(self.graph)
        self.graph.merge(ab + cb + cd)
        new_order = order(self.graph)
        new_size = size(self.graph)
        assert new_order == old_order + 3
        assert new_size == old_size + 3

    def test_can_merge_long_walkable_with_repeats(self):
        a = Node("Person", name="Alice")
        b = Node("Person", name="Bob")
        c = Node("Person", name="Carol")
        d = Node("Person", name="Dave")
        ab = Relationship(a, "KNOWS", b)
        cb = Relationship(c, "KNOWS", b)
        cd = Relationship(c, "KNOWS", d)
        bd = Relationship(b, "KNOWS", d)
        self.graph.create(a)
        old_order = order(self.graph)
        old_size = size(self.graph)
        self.graph.merge(ab + cb + cb + bd + cd)
        new_order = order(self.graph)
        new_size = size(self.graph)
        assert new_order == old_order + 3
        assert new_size == old_size + 4

    def test_can_merge_without_arguments(self):
        a = Node("A", a=1)
        b = Node("B", b=2)
        self.graph.create(a | b)
        a_id = a.identity
        b_id = b.identity
        node = Node("A", "B", a=1, b=2)
        self.graph.merge(node)
        assert node.identity != a_id
        assert node.identity != b_id

    def test_can_merge_with_arguments(self):
        a = Node("A", a=1)
        b = Node("B", b=2)
        self.graph.create(a | b)
        a_id = a.identity
        b_id = b.identity
        node = Node("A", "B", a=1, b=2)
        self.graph.merge(node, "A", "a")
        assert node.identity == a_id
        assert node.identity != b_id

    def test_merge_with_magic_values_overrides_arguments(self):
        a = Node("A", a=1)
        b = Node("B", b=2)
        self.graph.create(a | b)
        a_id = a.identity
        b_id = b.identity
        node = Node("A", "B", a=1, b=2)
        node.__primarylabel__ = "B"
        node.__primarykey__ = "b"
        self.graph.merge(node, "A", "a")
        assert node.identity != a_id
        assert node.identity == b_id

    def test_merge_with_primary_key_list(self):
        a = Node("A", a=1)
        b = Node("B", b=2)
        self.graph.create(a | b)
        a_id = a.identity
        b_id = b.identity
        node = Node("A", "B", a=1, b=2)
        node.__primarylabel__ = "B"
        node.__primarykey__ = ["b"]
        self.graph.merge(node, "A", "a")
        assert node.identity != a_id
        assert node.identity == b_id


class PullTestCase(IntegrationTestCase):

    def test_cannot_pull_non_graphy_object(self):
        with self.assertRaises(TypeError):
            self.graph.pull("this is not a graphy object")

    def test_can_graph_pull_node(self):
        alice_1 = Node()
        alice_2 = Node("Person", name="Alice")
        self.graph.create(alice_2)
        assert set(alice_1.labels) == set()
        assert dict(alice_1) == {}
        alice_1.graph = alice_2.graph
        alice_1.identity = alice_2.identity
        self.graph.pull(alice_1)
        assert set(alice_1.labels) == set(alice_2.labels)
        assert dict(alice_1) == dict(alice_2)

    def test_can_pull_path(self):
        alice = Node(name="Alice")
        bob = Node(name="Bob")
        carol = Node(name="Carol")
        dave = Node(name="Dave")
        path = Path(alice, "LOVES", bob, Relationship(carol, "HATES", bob), carol, "KNOWS", dave)
        self.graph.create(path)
        assert path[0]["amount"] is None
        assert path[1]["amount"] is None
        assert path[2]["since"] is None
        statement = ("MATCH ()-[ab]->() WHERE id(ab)={ab} "
                     "MATCH ()-[bc]->() WHERE id(bc)={bc} "
                     "MATCH ()-[cd]->() WHERE id(cd)={cd} "
                     "SET ab.amount = 'lots', bc.amount = 'some', cd.since = 1999")
        id_0 = path[0].identity
        id_1 = path[1].identity
        id_2 = path[2].identity
        parameters = {"ab": id_0, "bc": id_1, "cd": id_2}
        self.graph.run(statement, parameters)
        self.graph.pull(path)
        assert path[0]["amount"] == "lots"
        assert path[1]["amount"] == "some"
        assert path[2]["since"] == 1999

    def test_node_label_pull_scenarios(self):
        label_sets = [set(), {"Foo"}, {"Foo", "Bar"}, {"Spam"}]
        for old_labels in label_sets:
            for new_labels in label_sets:
                node = Node(*old_labels)
                self.graph.create(node)
                node_id = node.identity
                assert set(node.labels) == old_labels
                if old_labels:
                    remove_clause = "REMOVE a:%s" % ":".join(old_labels)
                else:
                    remove_clause = ""
                if new_labels:
                    set_clause = "SET a:%s" % ":".join(new_labels)
                else:
                    set_clause = ""
                if remove_clause or set_clause:
                    self.graph.run("MATCH (a) WHERE id(a)={x} %s %s" %
                                   (remove_clause, set_clause), x=node_id)
                    self.graph.pull(node)
                    assert set(node.labels) == new_labels, \
                        "Failed to pull new labels %r over old labels %r" % \
                        (new_labels, old_labels)

    def test_node_property_pull_scenarios(self):
        property_sets = [{}, {"name": "Alice"}, {"name": "Alice", "age": 33}, {"name": "Bob"}]
        for old_props in property_sets:
            for new_props in property_sets:
                node = Node(**old_props)
                self.graph.create(node)
                node_id = node.identity
                assert dict(node) == old_props
                self.graph.run("MATCH (a) WHERE id(a)={x} SET a={y}", x=node_id, y=new_props)
                self.graph.pull(node)
                assert dict(node) == new_props,\
                    "Failed to pull new properties %r over old properties %r" % \
                    (new_props, old_props)

    def test_relationship_property_pull_scenarios(self):
        property_sets = [{}, {"name": "Alice"}, {"name": "Alice", "age": 33}, {"name": "Bob"}]
        for old_props in property_sets:
            for new_props in property_sets:
                a = Node()
                b = Node()
                relationship = Relationship(a, "TO", b, **old_props)
                self.graph.create(relationship)
                relationship_id = relationship.identity
                assert dict(relationship) == old_props
                self.graph.run("MATCH ()-[r]->() WHERE id(r)={x} SET r={y}",
                               x=relationship_id, y=new_props)
                self.graph.pull(relationship)
                assert dict(relationship) == new_props, \
                    "Failed to pull new properties %r over old properties %r" % \
                    (new_props, old_props)


class PushTestCase(IntegrationTestCase):

    def test_cannot_push_non_graphy_object(self):
        with self.assertRaises(TypeError):
            self.graph.push("this is not a graphy object")

    def test_can_graph_push_node(self):
        alice_1 = Node("Person", name="Alice")
        alice_2 = Node()
        self.graph.create(alice_2)
        assert set(alice_2.labels) == set()
        assert dict(alice_2) == {}
        alice_1.graph = alice_2.graph
        alice_1.identity = alice_2.identity
        self.graph.push(alice_1)
        self.graph.pull(alice_2)
        assert set(alice_1.labels) == set(alice_2.labels)
        assert dict(alice_1) == dict(alice_2)

    def test_can_push_relationship(self):
        a = Node()
        b = Node()
        ab = Relationship(a, "KNOWS", b)
        self.graph.create(ab)
        value = self.graph.evaluate("MATCH ()-[ab:KNOWS]->() WHERE id(ab)={i} "
                                    "RETURN ab.since", i=ab.identity)
        assert value is None
        ab["since"] = 1999
        self.graph.push(ab)
        value = self.graph.evaluate("MATCH ()-[ab:KNOWS]->() WHERE id(ab)={i} "
                                    "RETURN ab.since", i=ab.identity)
        assert value == 1999

    def test_can_push_path(self):
        alice = Node(name="Alice")
        bob = Node(name="Bob")
        carol = Node(name="Carol")
        dave = Node(name="Dave")
        path = Path(alice, "LOVES", bob, Relationship(carol, "HATES", bob), carol, "KNOWS", dave)
        self.graph.create(path)
        statement = ("MATCH ()-[ab]->() WHERE id(ab)={ab} "
                     "MATCH ()-[bc]->() WHERE id(bc)={bc} "
                     "MATCH ()-[cd]->() WHERE id(cd)={cd} "
                     "RETURN ab.amount, bc.amount, cd.since")
        parameters = {"ab": path[0].identity, "bc": path[1].identity, "cd": path[2].identity}
        path[0]["amount"] = "lots"
        path[1]["amount"] = "some"
        path[2]["since"] = 1999
        ab_amount, bc_amount, cd_since = self.graph.run(statement, parameters).next()
        assert ab_amount is None
        assert bc_amount is None
        assert cd_since is None
        self.graph.push(path)
        ab_amount, bc_amount, cd_since = self.graph.run(statement, parameters).next()
        assert ab_amount == "lots"
        assert bc_amount == "some"
        assert cd_since == 1999

    def assert_has_labels(self, node_id, expected):
        actual = self.graph.evaluate("MATCH (_) WHERE id(_) = {x} return labels(_)", x=node_id)
        assert set(actual) == set(expected)

    def test_should_push_no_labels_onto_no_labels(self):
        node = Node()
        self.graph.create(node)
        node_id = node.identity
        self.assert_has_labels(node_id, {})
        self.graph.push(node)
        self.assert_has_labels(node_id, {})

    def test_should_push_no_labels_onto_one_label(self):
        node = Node("A")
        self.graph.create(node)
        node_id = node.identity
        self.assert_has_labels(node_id, {"A"})
        node.clear_labels()
        self.graph.push(node)
        self.assert_has_labels(node_id, {})

    def test_should_push_one_label_onto_no_labels(self):
        node = Node()
        self.graph.create(node)
        node_id = node.identity
        self.assert_has_labels(node_id, {})
        node.add_label("A")
        self.graph.push(node)
        self.assert_has_labels(node_id, {"A"})

    def test_should_push_one_label_onto_same_label(self):
        node = Node("A")
        self.graph.create(node)
        node_id = node.identity
        self.assert_has_labels(node_id, {"A"})
        self.graph.push(node)
        self.assert_has_labels(node_id, {"A"})

    def test_should_push_one_additional_label(self):
        node = Node("A")
        self.graph.create(node)
        node_id = node.identity
        self.assert_has_labels(node_id, {"A"})
        node.add_label("B")
        self.graph.push(node)
        self.assert_has_labels(node_id, {"A", "B"})

    def test_should_push_one_label_onto_different_label(self):
        node = Node("A")
        self.graph.create(node)
        node_id = node.identity
        self.assert_has_labels(node_id, {"A"})
        node.clear_labels()
        node.add_label("B")
        self.graph.push(node)
        self.assert_has_labels(node_id, {"B"})

    def test_should_push_multiple_labels_with_overlap(self):
        node = Node("A", "B")
        self.graph.create(node)
        node_id = node.identity
        self.assert_has_labels(node_id, {"A", "B"})
        node.remove_label("A")
        node.add_label("C")
        self.graph.push(node)
        self.assert_has_labels(node_id, {"B", "C"})


class SeparateTestCase(IntegrationTestCase):

    def test_can_delete_relationship_by_separating(self):
        a = Node()
        b = Node()
        r = Relationship(a, "TO", b)
        self.graph.create(r)
        assert self.graph.exists(r)
        self.graph.separate(r)
        assert not self.graph.exists(r)
        assert self.graph.exists(a)
        assert self.graph.exists(b)

    def test_cannot_separate_non_graphy_thing(self):
        with self.assertRaises(TypeError):
            self.graph.separate("this string is definitely not graphy")
