#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2021, Nigel Small
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


from io import StringIO
from unittest import TestCase

from py2neo.collections import PropertyDict
from py2neo.data import Subgraph, Walkable, Node, Relationship, Path, walk
from py2neo.database import Record, Table


KNOWS = Relationship.type("KNOWS")
LIKES = Relationship.type("LIKES")
DISLIKES = Relationship.type("DISLIKES")
MARRIED_TO = Relationship.type("MARRIED_TO")
WORKS_FOR = Relationship.type("WORKS_FOR")
WORKS_WITH = Relationship.type("WORKS_WITH")

alice = Node("Person", "Employee", name="Alice", age=33)
bob = Node("Person", name="Bob")
carol = Node("Person", name="Carol")
dave = Node("Person", name="Dave")

alice_knows_bob = KNOWS(alice, bob, since=1999)
alice_likes_carol = LIKES(alice, carol)
carol_dislikes_bob = DISLIKES(carol, bob)
carol_married_to_dave = MARRIED_TO(carol, dave)
dave_works_for_dave = WORKS_FOR(dave, dave)

subgraph = (alice_knows_bob | alice_likes_carol | carol_dislikes_bob |
            carol_married_to_dave | dave_works_for_dave)


def test_subgraph_union_produces_subgraph():
    assert isinstance(subgraph, Subgraph)


class DataListTestCase(TestCase):

    def test_simple_usage(self):
        table = Table([
            ["Alice", 33],
            ["Bob", 44],
            ["Carol", 55],
            ["Dave", 66],
        ], keys=["name", "age"])
        self.assertEqual(table.keys(), ["name", "age"])
        name_field = table.field(0)
        self.assertEqual(name_field["type"], str)
        self.assertEqual(name_field["optional"], False)
        age_field = table.field(1)
        self.assertEqual(age_field["type"], int)
        self.assertEqual(age_field["optional"], False)

    def test_missing_keys(self):
        with self.assertRaises(ValueError):
            _ = Table([
                ["Alice", 33],
                ["Bob", 44],
                ["Carol", 55],
                ["Dave", 66],
            ])

    def test_optional_fields(self):
        table = Table([
            ["Alice", 33],
            ["Bob", 44],
            ["Carol", None],
            [None, 66],
        ], keys=["name", "age"])
        self.assertEqual(table.keys(), ["name", "age"])
        name_field = table.field(0)
        self.assertEqual(name_field["type"], str)
        self.assertEqual(name_field["optional"], True)
        age_field = table.field(1)
        self.assertEqual(age_field["type"], int)
        self.assertEqual(age_field["optional"], True)

    def test_mixed_types(self):
        table = Table([
            ["Alice", 33],
            ["Bob", 44],
            ["Carol", 55.5],
            ["Dave", 66.6],
        ], keys=["name", "age"])
        self.assertEqual(table.keys(), ["name", "age"])
        name_field = table.field(0)
        self.assertEqual(name_field["type"], str)
        self.assertEqual(name_field["optional"], False)
        age_field = table.field(1)
        self.assertEqual(set(age_field["type"]), {int, float})
        self.assertEqual(age_field["optional"], False)

    def test_fields_by_name_usage(self):
        table = Table([
            ["Alice", 33],
            ["Bob", 44],
            ["Carol", 55],
            ["Dave", 66],
        ], keys=["name", "age"])
        self.assertEqual(table.keys(), ["name", "age"])
        name_field = table.field("name")
        self.assertEqual(name_field["type"], str)
        self.assertEqual(name_field["optional"], False)
        age_field = table.field("age")
        self.assertEqual(age_field["type"], int)
        self.assertEqual(age_field["optional"], False)
        with self.assertRaises(KeyError):
            _ = table.field("gender")

    def test_bad_typed_field_selector(self):
        table = Table([
            ["Alice", 33],
            ["Bob", 44],
            ["Carol", 55],
            ["Dave", 66],
        ], keys=["name", "age"])
        with self.assertRaises(TypeError):
            _ = table.field(object)

    def test_repr(self):
        table = Table([
            ["Alice", 33],
            ["Bob", 44],
            ["Carol", 55],
            ["Dave", 66],
        ], keys=["name", "age"])
        out = repr(table)
        self.assertEqual(out, u' name  | age \r\n'
                              u'-------|-----\r\n'
                              u' Alice |  33 \r\n'
                              u' Bob   |  44 \r\n'
                              u' Carol |  55 \r\n'
                              u' Dave  |  66 \r\n')

    def test_write(self):
        table = Table([
            ["Alice", 33],
            ["Bob", 44],
            ["Carol", 55],
            ["Dave", 66],
        ], keys=["name", "age"])
        out = StringIO()
        table.write(out)
        self.assertEqual(out.getvalue(), u' Alice | 33 \r\n'
                                         u' Bob   | 44 \r\n'
                                         u' Carol | 55 \r\n'
                                         u' Dave  | 66 \r\n')

    def test_write_with_newline_in_value(self):
        table = Table([
            ["Alice\nSmith", 33],
            ["Bob", 44],
            ["Carol", 55],
            ["Dave", 66],
        ], keys=["name", "age"])
        out = StringIO()
        table.write(out)
        self.assertEqual(out.getvalue(), u' Alice | 33 \r\n'
                                         u' Smith |    \r\n'
                                         u' Bob   | 44 \r\n'
                                         u' Carol | 55 \r\n'
                                         u' Dave  | 66 \r\n')

    def test_write_with_style(self):
        table = Table([
            ["Alice", 33],
            ["Bob", 44],
            ["Carol", 55],
            ["Dave", 66],
        ], keys=["name", "age"])
        out = StringIO()
        table.write(out, header={"fg": "red"})
        self.assertEqual(out.getvalue(), u' name  | age \r\n'
                                         u'-------|-----\r\n'
                                         u' Alice |  33 \r\n'
                                         u' Bob   |  44 \r\n'
                                         u' Carol |  55 \r\n'
                                         u' Dave  |  66 \r\n')

    def test_write_with_skip(self):
        table = Table([
            ["Alice", 33],
            ["Bob", 44],
            ["Carol", 55],
            ["Dave", 66],
        ], keys=["name", "age"])
        out = StringIO()
        table.write(out, skip=2)
        self.assertEqual(out.getvalue(), u' Carol | 55 \r\n'
                                         u' Dave  | 66 \r\n')

    def test_write_with_limit(self):
        table = Table([
            ["Alice", 33],
            ["Bob", 44],
            ["Carol", 55],
            ["Dave", 66],
        ], keys=["name", "age"])
        out = StringIO()
        table.write(out, limit=2)
        self.assertEqual(out.getvalue(), u' Alice | 33 \r\n'
                                         u' Bob   | 44 \r\n')

    def test_write_with_skip_and_limit(self):
        table = Table([
            ["Alice", 33],
            ["Bob", 44],
            ["Carol", 55],
            ["Dave", 66],
        ], keys=["name", "age"])
        out = StringIO()
        table.write(out, skip=1, limit=2)
        self.assertEqual(out.getvalue(), u' Bob   | 44 \r\n'
                                         u' Carol | 55 \r\n')

    def test_write_with_skip_and_limit_overflow(self):
        table = Table([
            ["Alice", 33],
            ["Bob", 44],
            ["Carol", 55],
            ["Dave", 66],
        ], keys=["name", "age"])
        out = StringIO()
        table.write(out, skip=1, limit=10)
        self.assertEqual(out.getvalue(), u' Bob   | 44 \r\n'
                                         u' Carol | 55 \r\n'
                                         u' Dave  | 66 \r\n')

    def test_write_csv(self):
        table = Table([
            ["Alice", 33],
            ["Bob", 44],
            ["Carol", 55],
            ["Dave", 66],
        ], keys=["name", "age"])
        out = StringIO()
        table.write_csv(out)
        self.assertEqual(out.getvalue(), u'Alice,33\r\n'
                                         u'Bob,44\r\n'
                                         u'Carol,55\r\n'
                                         u'Dave,66\r\n')

    def test_write_csv_with_header(self):
        table = Table([
            ["Alice", 33],
            ["Bob", 44],
            ["Carol", 55],
            ["Dave", 66],
        ], keys=["name", "age"])
        out = StringIO()
        table.write_csv(out, header=True)
        self.assertEqual(out.getvalue(), u'name,age\r\n'
                                         u'Alice,33\r\n'
                                         u'Bob,44\r\n'
                                         u'Carol,55\r\n'
                                         u'Dave,66\r\n')

    def test_write_csv_with_header_style(self):
        table = Table([
            ["Alice", 33],
            ["Bob", 44],
            ["Carol", 55],
            ["Dave", 66],
        ], keys=["name", "age"])
        out = StringIO()
        table.write_csv(out, header={"fg": "cyan"})
        self.assertEqual(out.getvalue(), u'name,age\r\n'
                                         u'Alice,33\r\n'
                                         u'Bob,44\r\n'
                                         u'Carol,55\r\n'
                                         u'Dave,66\r\n')

    def test_write_csv_with_limit(self):
        table = Table([
            ["Alice", 33],
            ["Bob", 44],
            ["Carol", 55],
            ["Dave", 66],
        ], keys=["name", "age"])
        out = StringIO()
        table.write_csv(out, limit=2)
        self.assertEqual(out.getvalue(), u'Alice,33\r\n'
                                         u'Bob,44\r\n')

    def test_write_csv_with_comma_in_value(self):
        table = Table([
            ["Alice", 33],
            ["Bob", 44],
            ["Carol", 55],
            ["Smith, Dave", 66],
        ], keys=["name", "age"])
        out = StringIO()
        table.write_csv(out)
        self.assertEqual(out.getvalue(), u'Alice,33\r\n'
                                         u'Bob,44\r\n'
                                         u'Carol,55\r\n'
                                         u'"Smith, Dave",66\r\n')

    def test_write_csv_with_quotes_in_value(self):
        table = Table([
            ["Alice", 33],
            ["Bob", 44],
            ["Carol", 55],
            ["Dave \"Nordberg\" Smith", 66],
        ], keys=["name", "age"])
        out = StringIO()
        table.write_csv(out)
        self.assertEqual(out.getvalue(), u'Alice,33\r\n'
                                         u'Bob,44\r\n'
                                         u'Carol,55\r\n'
                                         u'"Dave ""Nordberg"" Smith",66\r\n')

    def test_write_csv_with_none_in_value(self):
        table = Table([
            ["Alice", 33],
            ["Bob", 44],
            ["Carol", 55],
            ["Dave", None],
        ], keys=["name", "age"])
        out = StringIO()
        table.write_csv(out)
        self.assertEqual(out.getvalue(), u'Alice,33\r\n'
                                         u'Bob,44\r\n'
                                         u'Carol,55\r\n'
                                         u'Dave,\r\n')

    def test_write_tsv(self):
        table = Table([
            ["Alice", 33],
            ["Bob", 44],
            ["Carol", 55],
            ["Dave", 66],
        ], keys=["name", "age"])
        out = StringIO()
        table.write_tsv(out)
        self.assertEqual(out.getvalue(), u'Alice\t33\r\n'
                                         u'Bob\t44\r\n'
                                         u'Carol\t55\r\n'
                                         u'Dave\t66\r\n')


class PropertySetTestCase(TestCase):

    def test_equality(self):
        first = PropertyDict({"name": "Alice", "age": 33, "colours": ["red", "purple"]})
        second = PropertyDict({"name": "Alice", "age": 33, "colours": ["red", "purple"]})
        assert first == second

    def test_inequality(self):
        first = PropertyDict({"name": "Alice", "age": 33, "colours": ["red", "purple"]})
        second = PropertyDict({"name": "Bob", "age": 44, "colours": ["blue", "purple"]})
        assert first != second

    def test_getter(self):
        properties = PropertyDict({"name": "Alice"})
        assert properties["name"] == "Alice"

    def test_getter_with_none(self):
        properties = PropertyDict({"name": "Alice"})
        assert properties["age"] is None

    def test_setter(self):
        properties = PropertyDict({"name": "Alice"})
        properties["age"] = 33
        assert properties == {"name": "Alice", "age": 33}

    def test_setter_with_none(self):
        properties = PropertyDict({"name": "Alice", "age": 33})
        properties["age"] = None
        assert properties == {"name": "Alice"}

    def test_setter_with_none_for_non_existent(self):
        properties = PropertyDict({"name": "Alice"})
        properties["age"] = None
        assert properties == {"name": "Alice"}

    def test_setdefault_without_default_with_existing(self):
        properties = PropertyDict({"name": "Alice", "age": 33})
        value = properties.setdefault("age")
        assert properties == {"name": "Alice", "age": 33}
        assert value == 33

    def test_setdefault_without_default_with_non_existent(self):
        properties = PropertyDict({"name": "Alice"})
        value = properties.setdefault("age")
        assert properties == {"name": "Alice"}
        assert value is None

    def test_setdefault_with_default_with_existing(self):
        properties = PropertyDict({"name": "Alice", "age": 33})
        value = properties.setdefault("age", 34)
        assert properties == {"name": "Alice", "age": 33}
        assert value == 33

    def test_setdefault_with_default_with_non_existent(self):
        properties = PropertyDict({"name": "Alice"})
        value = properties.setdefault("age", 33)
        assert properties == {"name": "Alice", "age": 33}
        assert value == 33

    def test_deleter(self):
        properties = PropertyDict({"name": "Alice", "age": 33})
        del properties["age"]
        assert properties == {"name": "Alice"}


class SubgraphTestCase(TestCase):

    subgraph = (alice_knows_bob | alice_likes_carol | carol_dislikes_bob |
                carol_married_to_dave | dave_works_for_dave)

    def test_nodes(self):
        self.assertSetEqual(self.subgraph.nodes, {alice, bob, carol, dave})

    def test_relationships(self):
        self.assertSetEqual(self.subgraph.relationships, {alice_knows_bob, alice_likes_carol, carol_dislikes_bob,
                                                          carol_married_to_dave, dave_works_for_dave})

    def test_can_infer_nodes_through_relationships(self):
        s = Subgraph([], [alice_knows_bob])
        self.assertSetEqual(s.nodes, {alice, bob})
        self.assertSetEqual(s.relationships, {alice_knows_bob})

    def test_equality(self):
        other_subgraph = (alice_knows_bob | alice_likes_carol | carol_dislikes_bob |
                          carol_married_to_dave | dave_works_for_dave)
        assert self.subgraph == other_subgraph
        assert hash(self.subgraph) == hash(other_subgraph)

    def test_inequality(self):
        other_subgraph = (alice_knows_bob | alice_likes_carol | carol_dislikes_bob |
                          carol_married_to_dave)
        assert self.subgraph != other_subgraph
        assert hash(self.subgraph) != hash(other_subgraph)

    def test_inequality_with_other_types(self):
        assert self.subgraph != "this is not a graph"

    def test_len(self):
        assert len(self.subgraph) == 5

    def test_bool(self):
        assert self.subgraph.__bool__() is True
        assert self.subgraph.__nonzero__() is True

    def test_labels(self):
        assert self.subgraph.labels() == {"Person", "Employee"}

    def test_types(self):
        assert self.subgraph.types() == {"KNOWS", "LIKES", "DISLIKES",
                                         "MARRIED_TO", "WORKS_FOR"}

    def test_property_keys(self):
        assert self.subgraph.keys() == {"name", "age", "since"}

    def test_empty_subgraph(self):
        with self.assertRaises(ValueError):
            Subgraph()


class WalkableTestCase(TestCase):

    sequence = (alice, alice_knows_bob, bob, carol_dislikes_bob, carol)
    walkable = Walkable(sequence)

    def test_nodes(self):
        nodes = self.walkable.nodes
        assert isinstance(nodes, tuple)
        assert nodes == (alice, bob, carol)

    def test_relationships(self):
        relationships = self.walkable.relationships
        assert isinstance(relationships, tuple)
        assert relationships == (alice_knows_bob, carol_dislikes_bob)

    def test_length(self):
        assert len(self.walkable) == 2

    def test_equality(self):
        other_subgraph = Walkable(self.sequence)
        assert self.walkable == other_subgraph
        assert hash(self.walkable) == hash(other_subgraph)

    def test_inequality(self):
        other_subgraph = Walkable([alice, alice_likes_carol, carol,
                                  carol_dislikes_bob, bob])
        assert self.walkable != other_subgraph
        assert hash(self.walkable) != hash(other_subgraph)

    def test_inequality_with_other_types(self):
        assert self.walkable != "this is not a graph"

    def test_iteration(self):
        assert tuple(iter(self.walkable)) == (alice_knows_bob, carol_dislikes_bob)

    def test_slicing(self):
        sequence = (alice, alice_knows_bob, bob, carol_dislikes_bob, carol,
                    carol_married_to_dave, dave, dave_works_for_dave, dave)
        subgraph = Walkable(sequence)
        assert subgraph[0] == alice_knows_bob
        assert subgraph[1] == carol_dislikes_bob
        assert subgraph[2] == carol_married_to_dave
        assert subgraph[3] == dave_works_for_dave
        assert subgraph[0:0] == Walkable([alice])
        assert subgraph[0:1] == Walkable([alice, alice_knows_bob, bob])
        assert subgraph[0:2] == Walkable([alice, alice_knows_bob, bob,
                                         carol_dislikes_bob, carol])
        assert subgraph[0:3] == Walkable([alice, alice_knows_bob, bob,
                                         carol_dislikes_bob, carol,
                                         carol_married_to_dave, dave])
        assert subgraph[0:4] == Walkable([alice, alice_knows_bob, bob,
                                         carol_dislikes_bob, carol,
                                         carol_married_to_dave, dave,
                                         dave_works_for_dave, dave])
        assert subgraph[0:5] == Walkable([alice, alice_knows_bob, bob,
                                         carol_dislikes_bob, carol,
                                         carol_married_to_dave, dave,
                                         dave_works_for_dave, dave])
        assert subgraph[0:] == Walkable([alice, alice_knows_bob, bob,
                                        carol_dislikes_bob, carol,
                                        carol_married_to_dave, dave,
                                        dave_works_for_dave, dave])
        assert subgraph[:1] == Walkable([alice, alice_knows_bob, bob])
        assert subgraph[1:1] == Walkable([bob])
        assert subgraph[1:2] == Walkable([bob, carol_dislikes_bob, carol])
        assert subgraph[1:3] == Walkable([bob, carol_dislikes_bob, carol,
                                         carol_married_to_dave, dave])
        assert subgraph[1:4] == Walkable([bob, carol_dislikes_bob, carol,
                                         carol_married_to_dave, dave,
                                         dave_works_for_dave, dave])
        assert subgraph[1:5] == Walkable([bob, carol_dislikes_bob, carol,
                                         carol_married_to_dave, dave,
                                         dave_works_for_dave, dave])
        assert subgraph[1:] == Walkable([bob, carol_dislikes_bob, carol,
                                        carol_married_to_dave, dave,
                                        dave_works_for_dave, dave])
        assert subgraph[:2] == Walkable([alice, alice_knows_bob, bob,
                                        carol_dislikes_bob, carol])
        assert subgraph[2:2] == Walkable([carol])
        assert subgraph[2:3] == Walkable([carol, carol_married_to_dave, dave])
        assert subgraph[2:4] == Walkable([carol, carol_married_to_dave, dave,
                                         dave_works_for_dave, dave])
        assert subgraph[2:5] == Walkable([carol, carol_married_to_dave, dave,
                                         dave_works_for_dave, dave])
        assert subgraph[2:] == Walkable([carol, carol_married_to_dave, dave,
                                        dave_works_for_dave, dave])
        assert subgraph[1:-1] == Walkable([bob, carol_dislikes_bob, carol,
                                          carol_married_to_dave, dave])
        assert subgraph[-3:-1] == Walkable([bob, carol_dislikes_bob, carol,
                                           carol_married_to_dave, dave])


class NodeTestCase(TestCase):

    def test_nodes(self):
        nodes = alice.nodes
        assert isinstance(nodes, tuple)
        assert nodes == (alice,)

    def test_relationships(self):
        relationships = alice.relationships
        assert isinstance(relationships, tuple)
        assert relationships == ()

    def test_empty_node(self):
        n = Node()
        assert not n.__bool__()
        assert not n.__nonzero__()
        assert len(n) == 0

    def test_node(self):
        assert alice.start_node == alice
        assert alice.end_node == alice
        assert alice.__bool__()
        assert alice.__nonzero__()
        assert len(alice) == 2
        assert list(walk(alice)) == [alice]
        assert set(alice.labels) == {"Person", "Employee"}
        assert dict(alice) == {"name": "Alice", "age": 33}
        assert dict(alice)["name"] == "Alice"
        assert alice["name"] == "Alice"
        assert len(alice.nodes) == 1
        assert len(alice.relationships) == 0
        assert set(alice.nodes) == {alice}
        assert set(alice.relationships) == set()

    def test_equality(self):
        other_node = alice
        assert alice == other_node

    def test_inequality(self):
        other_node = Node("Person", "Employee", name="Alice", age=33)
        assert alice != other_node

    def test_inequality_with_other_types(self):
        assert alice != "this is not a node"

    def test_can_add_label(self):
        node = Node("Person", name="Alice")
        node.add_label("Employee")
        assert set(node.labels) == {"Person", "Employee"}

    def test_add_label_is_idempotent(self):
        node = Node("Person", name="Alice")
        node.add_label("Employee")
        node.add_label("Employee")
        assert set(node.labels) == {"Person", "Employee"}

    def test_can_remove_label(self):
        node = Node("Person", "Employee", name="Alice")
        node.remove_label("Employee")
        assert set(node.labels) == {"Person"}

    def test_removing_non_existent_label_does_nothing(self):
        node = Node("Person", name="Alice")
        node.remove_label("Employee")
        assert set(node.labels) == {"Person"}

    def test_can_clear_labels(self):
        node = Node("Person", "Employee", name="Alice")
        node.clear_labels()
        assert set(node.labels) == set()

    def test_has_label(self):
        node = Node("Person", name="Alice")
        assert node.has_label("Person")
        assert not node.has_label("Employee")

    def test_update_labels(self):
        node = Node("Person", name="Alice")
        node.update_labels({"Person", "Employee"})
        assert set(node.labels) == {"Person", "Employee"}


class RelationshipTestCase(TestCase):

    def test_nodes(self):
        nodes = alice_knows_bob.nodes
        assert isinstance(nodes, tuple)
        assert nodes == (alice, bob)

    def test_relationships(self):
        relationships = alice_knows_bob.relationships
        assert isinstance(relationships, tuple)
        assert relationships == (alice_knows_bob,)

    def test_relationship(self):
        assert alice_knows_bob.start_node == alice
        assert alice_knows_bob.end_node == bob
        assert list(walk(alice_knows_bob)) == [alice, alice_knows_bob, bob]
        assert type(alice_knows_bob).__name__ == "KNOWS"
        assert dict(alice_knows_bob) == {"since": 1999}
        assert alice_knows_bob["since"] == 1999
        assert set(alice_knows_bob.nodes) == {alice, bob}
        assert set(alice_knows_bob.relationships) == {alice_knows_bob}

    def test_loop(self):
        assert dave_works_for_dave.start_node == dave
        assert dave_works_for_dave.end_node == dave
        assert list(walk(dave_works_for_dave)) == [dave, dave_works_for_dave, dave]
        assert set(dave_works_for_dave.nodes) == {dave}
        assert set(dave_works_for_dave.relationships) == {dave_works_for_dave}

    def test_construction_from_no_arguments(self):
        with self.assertRaises(TypeError):
            _ = Relationship()

    def test_construction_from_one_argument(self):
        rel = Relationship(alice)
        assert rel.start_node is alice
        assert rel.end_node is alice
        self.assertEqual(type(rel).__name__, "Relationship")

    def test_construction_from_two_node_arguments(self):
        rel = Relationship(alice, bob)
        assert rel.start_node is alice
        assert rel.end_node is bob
        self.assertEqual(type(rel).__name__, "Relationship")

    def test_construction_from_node_and_type_arguments(self):
        rel = Relationship(alice, "LIKES")
        assert rel.start_node is alice
        assert rel.end_node is alice
        self.assertEqual(type(rel).__name__, "LIKES")

    def test_construction_from_three_arguments(self):
        rel = Relationship(alice, "KNOWS", bob)
        assert rel.start_node is alice
        assert rel.end_node is bob
        self.assertIs(type(rel), KNOWS)

    def test_construction_from_subclass(self):
        rel = WORKS_WITH(alice, bob)
        assert rel.start_node is alice
        assert rel.end_node is bob
        self.assertIs(type(rel), WORKS_WITH)

    def test_construction_from_more_arguments(self):
        with self.assertRaises(TypeError):
            Relationship(alice, "KNOWS", bob, carol)

    def test_equality_with_self(self):
        self.assertEqual(alice_knows_bob, alice_knows_bob)

    def test_equality_with_similar(self):
        other_rel = Relationship(alice, "KNOWS", bob, since=1999)
        self.assertEqual(alice_knows_bob, other_rel)

    def test_inequality(self):
        other_rel = Relationship(alice, "KNOWS", bob, since=1998)
        assert alice_knows_bob != other_rel

    def test_inequality_with_other_types(self):
        assert alice_knows_bob != "there is no relationship"

    def test_inequality_with_sneaky_type(self):

        class Foo(object):
            graph = None
            identity = None

        assert alice_knows_bob != Foo()


class RelationshipLoopTestCase(TestCase):

    loop = Relationship(alice, "LIKES", alice)

    def test_nodes(self):
        nodes = self.loop.nodes
        assert isinstance(nodes, tuple)
        assert nodes == (alice, alice)

    def test_relationships(self):
        relationships = self.loop.relationships
        assert isinstance(relationships, tuple)
        assert relationships == (self.loop,)


class PathTestCase(TestCase):

    path = Path(alice, alice_knows_bob, bob, alice_knows_bob, alice, alice_likes_carol, carol)

    def test_nodes(self):
        nodes = self.path.nodes
        assert isinstance(nodes, tuple)
        assert nodes == (alice, bob, alice, carol)

    def test_relationships(self):
        relationships = self.path.relationships
        assert isinstance(relationships, tuple)
        assert relationships == (alice_knows_bob, alice_knows_bob, alice_likes_carol)

    def test_length(self):
        assert len(self.path) == 3

    def test_construction_of_path_length_0(self):
        sequence = [alice]
        path = Path(*sequence)
        assert len(path) == 0
        assert set(path.nodes) == {alice}
        assert set(path.relationships) == set()
        assert path.start_node == alice
        assert path.end_node == alice
        assert len(path) == 0
        assert list(walk(path)) == sequence

    def test_construction_of_path_length_1(self):
        sequence = [alice, alice_knows_bob, bob]
        path = Path(*sequence)
        assert len(path) == 1
        assert set(path.nodes) == {alice, bob}
        assert set(path.relationships) == {alice_knows_bob}
        assert path.start_node == alice
        assert path.end_node == bob
        assert len(path) == 1
        assert list(walk(path)) == sequence

    def test_construction_of_path_length_2(self):
        sequence = [alice, alice_knows_bob, bob, carol_dislikes_bob, carol]
        path = Path(*sequence)
        assert len(path) == 2
        assert set(path.nodes) == {alice, bob, carol}
        assert set(path.relationships) == {alice_knows_bob, carol_dislikes_bob}
        assert path.start_node == alice
        assert path.end_node == carol
        assert len(path) == 2
        assert list(walk(path)) == sequence

    def test_construction_of_path_with_revisits(self):
        sequence = [alice, alice_knows_bob, bob, carol_dislikes_bob, carol,
                    alice_likes_carol, alice, alice_knows_bob, bob]
        path = Path(*sequence)
        assert len(path) == 4
        assert set(path.nodes) == {alice, bob, carol}
        assert set(path.relationships) == {alice_knows_bob, alice_likes_carol, carol_dislikes_bob}
        assert path.start_node == alice
        assert path.end_node == bob
        assert len(path) == 4
        assert list(walk(path)) == sequence

    def test_construction_of_path_with_loop(self):
        sequence = [carol, carol_married_to_dave, dave, dave_works_for_dave, dave]
        path = Path(*sequence)
        assert len(path) == 2
        assert set(path.nodes) == {carol, dave}
        assert set(path.relationships) == {carol_married_to_dave, dave_works_for_dave}
        assert path.start_node == carol
        assert path.end_node == dave
        assert len(path) == 2
        assert list(walk(path)) == sequence

    def test_path_indexing(self):
        sequence = [alice_knows_bob, carol_dislikes_bob, carol_married_to_dave]
        path = Path(*sequence)
        assert path[0] == alice_knows_bob
        assert path[1] == carol_dislikes_bob
        assert path[2] == carol_married_to_dave
        assert path[-3] == alice_knows_bob
        assert path[-2] == carol_dislikes_bob
        assert path[-1] == carol_married_to_dave
        with self.assertRaises(IndexError):
            _ = path[3]


class WalkTestCase(TestCase):

    def test_can_walk_nothing(self):
        result = list(walk())
        assert result == []

    def test_can_walk_node(self):
        result = list(walk(alice))
        assert result == [alice]

    def test_can_walk_node_twice(self):
        result = list(walk(alice, alice))
        assert result == [alice]

    def test_can_walk_node_and_relationship(self):
        result = list(walk(alice, alice_knows_bob))
        assert result == [alice, alice_knows_bob, bob]

    def test_can_walk_node_relationship_and_node(self):
        result = list(walk(alice, alice_knows_bob, bob))
        assert result == [alice, alice_knows_bob, bob]

    def test_can_walk_node_relationship_and_node_in_reverse(self):
        result = list(walk(bob, alice_knows_bob, alice))
        assert result == [bob, alice_knows_bob, alice]

    def test_cannot_walk_non_walkable_as_first_argument(self):
        with self.assertRaises(TypeError):
            list(walk(object()))

    def test_cannot_walk_non_walkable_as_second_argument(self):
        with self.assertRaises(TypeError):
            list(walk(alice, object()))


class ConcatenationTestCase(TestCase):

    def test_can_concatenate_node_and_node(self):
        result = alice + alice
        assert result == Walkable([alice])

    def test_can_concatenate_node_and_relationship(self):
        result = alice + alice_knows_bob
        assert result == Walkable([alice, alice_knows_bob, bob])

    def test_can_concatenate_node_and_reversed_relationship(self):
        bob_knows_alice = Relationship(bob, "KNOWS", alice)
        result = alice + bob_knows_alice
        assert result == Walkable([alice, bob_knows_alice, bob])

    def test_can_concatenate_node_and_path(self):
        path = Walkable([alice, alice_knows_bob, bob])
        result = alice + path
        assert result == path

    def test_can_concatenate_node_and_reversed_path(self):
        result = alice + Walkable([bob, alice_knows_bob, alice])
        assert result == Walkable([alice, alice_knows_bob, bob])

    def test_can_concatenate_relationship_and_node(self):
        result = alice_knows_bob + bob
        assert result == Walkable([alice, alice_knows_bob, bob])

    def test_can_concatenate_relationship_and_relationship(self):
        result = alice_knows_bob + carol_dislikes_bob
        assert result == Walkable([alice, alice_knows_bob, bob, carol_dislikes_bob, carol])

    def test_can_concatenate_relationship_and_path(self):
        result = alice_knows_bob + Walkable([bob, carol_dislikes_bob, carol])
        assert result == Walkable([alice, alice_knows_bob, bob, carol_dislikes_bob, carol])

    def test_can_concatenate_path_and_node(self):
        result = Walkable([alice, alice_knows_bob, bob]) + bob
        assert result == Walkable([alice, alice_knows_bob, bob])

    def test_can_concatenate_path_and_relationship(self):
        result = Walkable([alice, alice_knows_bob, bob]) + carol_dislikes_bob
        assert result == Walkable([alice, alice_knows_bob, bob, carol_dislikes_bob, carol])

    def test_can_concatenate_path_and_path(self):
        result = (Walkable([alice, alice_knows_bob, bob]) +
                  Walkable([bob, carol_dislikes_bob, carol]))
        assert result == Walkable([alice, alice_knows_bob, bob, carol_dislikes_bob, carol])

    def test_cannot_concatenate_different_endpoints(self):
        with self.assertRaises(ValueError):
            _ = alice + bob

    def test_can_concatenate_node_and_none(self):
        result = alice + None
        assert result is alice


class UnionTestCase(TestCase):

    def test_graph_union(self):
        graph_1 = (alice_knows_bob | alice_likes_carol | carol_dislikes_bob)
        graph_2 = (carol_dislikes_bob | carol_married_to_dave | dave_works_for_dave)
        graph = graph_1 | graph_2
        assert len(graph.nodes) == 4
        assert len(graph.relationships) == 5
        assert graph.nodes == (alice | bob | carol | dave).nodes


class IntersectionTestCase(TestCase):

    def test_graph_intersection(self):
        graph_1 = (alice_knows_bob | alice_likes_carol | carol_dislikes_bob)
        graph_2 = (carol_dislikes_bob | carol_married_to_dave | dave_works_for_dave)
        graph = graph_1 & graph_2
        assert len(graph.nodes) == 2
        assert len(graph.relationships) == 1
        assert graph.nodes == (bob | carol).nodes


class DifferenceTestCase(TestCase):

    def test_graph_difference(self):
        graph_1 = (alice_knows_bob | alice_likes_carol | carol_dislikes_bob)
        graph_2 = (carol_dislikes_bob | carol_married_to_dave | dave_works_for_dave)
        graph = graph_1 - graph_2
        assert len(graph.nodes) == 3
        assert len(graph.relationships) == 2
        assert graph.nodes == (alice | bob | carol).nodes


class SymmetricDifferenceTestCase(TestCase):

    def test_graph_symmetric_difference(self):
        graph_1 = (alice_knows_bob | alice_likes_carol | carol_dislikes_bob)
        graph_2 = (carol_dislikes_bob | carol_married_to_dave | dave_works_for_dave)
        graph = graph_1 ^ graph_2
        assert len(graph.nodes) == 4
        assert len(graph.relationships) == 4
        assert graph.nodes == (alice | bob | carol | dave).nodes
        assert graph.relationships == frozenset(alice_knows_bob | alice_likes_carol |
                                                carol_married_to_dave | dave_works_for_dave)


def test_record_repr():
    person = Record(["name", "age"], ["Alice", 33])
    assert repr(person) == "Record({'name': 'Alice', 'age': 33})"


def test_record_str():
    person = Record(["name", "age"], ["Alice", 33])
    assert str(person) == "'Alice'\t33"


def test_node_repr():
    assert repr(alice) == "Node('Employee', 'Person', age=33, name='Alice')"


def test_node_str():
    assert str(alice) == "(:Employee:Person {age: 33, name: 'Alice'})"


def test_relationship_repr():
    assert (repr(alice_knows_bob) == "KNOWS(Node('Employee', 'Person', age=33, name='Alice'), "
                                     "Node('Person', name='Bob'), since=1999)")


def test_relationship_str():
    assert str(alice_knows_bob) == "(Alice)-[:KNOWS {since: 1999}]->(Bob)"
