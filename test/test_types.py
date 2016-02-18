#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# CyPy: Cypher Framework for Python
# Copyright 2015 Nigel Small
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.


from unittest import TestCase

from py2neo.types import PropertyDict, Subgraph, Walkable, Node, Relationship, Path, walk, order, size


alice = Node("Person", "Employee", name="Alice", age=33)
bob = Node("Person")
carol = Node("Person")
dave = Node("Person")

alice_knows_bob = Relationship(alice, "KNOWS", bob, since=1999)
alice_likes_carol = Relationship(alice, "LIKES", carol)
carol_dislikes_bob = Relationship(carol, "DISLIKES", bob)
carol_married_to_dave = Relationship(carol, "MARRIED_TO", dave)
dave_works_for_dave = Relationship(dave, "WORKS_FOR", dave)


class PropertyCoercionTestCase(TestCase):

    def test_boolean(self):
        props = PropertyDict({"value": True})
        assert props == {"value": True}

    def test_integer_in_range(self):
        props = PropertyDict({"value": 1})
        assert props == {"value": 1}

    def test_integer_too_high(self):
        with self.assertRaises(ValueError):
            PropertyDict({"value": 2 ** 64})

    def test_integer_too_low(self):
        with self.assertRaises(ValueError):
            PropertyDict({"value": -(2 ** 64)})

    def test_float(self):
        props = PropertyDict({"value": 3.141})
        assert props == {"value": 3.141}

    def test_byte_strings_are_supported(self):
        props = PropertyDict({"value": b"hello, world"})
        assert props == {"value": u"hello, world"}

    def test_unicode_strings_are_supported(self):
        props = PropertyDict({"value": u"hello, world"})
        assert props == {"value": u"hello, world"}

    def test_byte_arrays_are_not_supported(self):
        with self.assertRaises(TypeError):
            PropertyDict({"value": bytearray(b"hello, world")})

    def test_homogenous_list(self):
        props = PropertyDict({"value": [1, 2, 3]})
        assert props == {"value": [1, 2, 3]}

    def test_homogenous_list_of_strings(self):
        props = PropertyDict({"value": [u"hello", b"world"]})
        assert props == {"value": [u"hello", u"world"]}

    def test_heterogenous_list(self):
        with self.assertRaises(TypeError):
            PropertyDict({"value": [True, 2, u"three"]})


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
        nodes = self.subgraph.nodes()
        assert isinstance(nodes, frozenset)
        assert nodes == {alice, bob, carol, dave}

    def test_relationships(self):
        relationships = self.subgraph.relationships()
        assert isinstance(relationships, frozenset)
        assert relationships == {alice_knows_bob, alice_likes_carol, carol_dislikes_bob,
                                 carol_married_to_dave, dave_works_for_dave}

    def test_order(self):
        assert order(self.subgraph) == 4

    def test_size(self):
        assert size(self.subgraph) == 5

    def test_can_infer_nodes_through_relationships(self):
        subgraph = Subgraph([], [alice_knows_bob])
        assert order(subgraph) == 2
        assert size(subgraph) == 1
        assert subgraph.nodes() == {alice, bob}
        assert subgraph.relationships() == {alice_knows_bob}

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


class WalkableTestCase(TestCase):

    sequence = (alice, alice_knows_bob, bob, carol_dislikes_bob, carol)
    walkable = Walkable(sequence)

    def test_nodes(self):
        nodes = self.walkable.nodes()
        assert isinstance(nodes, tuple)
        assert nodes == (alice, bob, carol)

    def test_relationships(self):
        relationships = self.walkable.relationships()
        assert isinstance(relationships, tuple)
        assert relationships == (alice_knows_bob, carol_dislikes_bob)

    def test_length(self):
        assert len(self.walkable) == 2

    def test_order(self):
        assert order(self.walkable) == 3

    def test_size(self):
        assert size(self.walkable) == 2

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
        nodes = alice.nodes()
        assert isinstance(nodes, tuple)
        assert nodes == (alice,)

    def test_relationships(self):
        relationships = alice.relationships()
        assert isinstance(relationships, tuple)
        assert relationships == ()

    def test_order(self):
        assert order(alice) == 1

    def test_size(self):
        assert size(alice) == 0

    def test_empty_node(self):
        n = Node()
        assert not n.__bool__()
        assert not n.__nonzero__()
        assert len(n) == 0

    def test_node(self):
        assert alice.start_node() == alice
        assert alice.end_node() == alice
        assert alice.__bool__()
        assert alice.__nonzero__()
        assert len(alice) == 2
        assert list(walk(alice)) == [alice]
        assert set(alice.labels()) == {"Person", "Employee"}
        assert dict(alice) == {"name": "Alice", "age": 33}
        assert dict(alice)["name"] == "Alice"
        assert alice["name"] == "Alice"
        assert order(alice) == 1
        assert size(alice) == 0
        assert set(alice.nodes()) == {alice}
        assert set(alice.relationships()) == set()

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
        assert set(node.labels()) == {"Person", "Employee"}

    def test_add_label_is_idempotent(self):
        node = Node("Person", name="Alice")
        node.add_label("Employee")
        node.add_label("Employee")
        assert set(node.labels()) == {"Person", "Employee"}

    def test_can_remove_label(self):
        node = Node("Person", "Employee", name="Alice")
        node.remove_label("Employee")
        assert set(node.labels()) == {"Person"}

    def test_removing_non_existent_label_does_nothing(self):
        node = Node("Person", name="Alice")
        node.remove_label("Employee")
        assert set(node.labels()) == {"Person"}

    def test_can_clear_labels(self):
        node = Node("Person", "Employee", name="Alice")
        node.clear_labels()
        assert set(node.labels()) == set()

    def test_has_label(self):
        node = Node("Person", name="Alice")
        assert node.has_label("Person")
        assert not node.has_label("Employee")

    def test_update_labels(self):
        node = Node("Person", name="Alice")
        node.update_labels({"Person", "Employee"})
        assert set(node.labels()) == {"Person", "Employee"}


class RelationshipTestCase(TestCase):

    def test_nodes(self):
        nodes = alice_knows_bob.nodes()
        assert isinstance(nodes, tuple)
        assert nodes == (alice, bob)

    def test_relationships(self):
        relationships = alice_knows_bob.relationships()
        assert isinstance(relationships, tuple)
        assert relationships == (alice_knows_bob,)

    def test_order(self):
        assert order(alice_knows_bob) == 2

    def test_size(self):
        assert size(alice_knows_bob) == 1

    def test_relationship(self):
        assert alice_knows_bob.start_node() == alice
        assert alice_knows_bob.end_node() == bob
        assert list(walk(alice_knows_bob)) == [alice, alice_knows_bob, bob]
        assert alice_knows_bob.type() == "KNOWS"
        assert dict(alice_knows_bob) == {"since": 1999}
        assert alice_knows_bob["since"] == 1999
        assert order(alice_knows_bob) == 2
        assert size(alice_knows_bob) == 1
        assert set(alice_knows_bob.nodes()) == {alice, bob}
        assert set(alice_knows_bob.relationships()) == {alice_knows_bob}

    def test_loop(self):
        assert dave_works_for_dave.start_node() == dave
        assert dave_works_for_dave.end_node() == dave
        assert list(walk(dave_works_for_dave)) == [dave, dave_works_for_dave, dave]
        assert order(dave_works_for_dave) == 1
        assert size(dave_works_for_dave) == 1
        assert set(dave_works_for_dave.nodes()) == {dave}
        assert set(dave_works_for_dave.relationships()) == {dave_works_for_dave}

    def test_construction_from_no_arguments(self):
        with self.assertRaises(TypeError):
            _ = Relationship()

    def test_construction_from_one_argument(self):
        rel = Relationship(alice)
        assert rel.start_node() is alice
        assert rel.end_node() is alice
        assert rel.type() == "TO"

    def test_construction_from_two_node_arguments(self):
        rel = Relationship(alice, bob)
        assert rel.start_node() is alice
        assert rel.end_node() is bob
        assert rel.type() == "TO"

    def test_construction_from_node_and_type_arguments(self):
        rel = Relationship(alice, "LIKES")
        assert rel.start_node() is alice
        assert rel.end_node() is alice
        assert rel.type() == "LIKES"

    def test_construction_from_three_arguments(self):
        rel = Relationship(alice, "KNOWS", bob)
        assert rel.start_node() is alice
        assert rel.end_node() is bob
        assert rel.type() == "KNOWS"

    def test_construction_from_subclass(self):
        class WorksWith(Relationship):
            pass
        rel = WorksWith(alice, bob)
        assert rel.start_node() is alice
        assert rel.end_node() is bob
        assert rel.type() == "WORKS_WITH"

    def test_construction_from_more_arguments(self):
        with self.assertRaises(TypeError):
            Relationship(alice, "KNOWS", bob, carol)

    def test_equality(self):
        other_rel = alice_knows_bob
        assert alice_knows_bob == other_rel

    def test_inequality(self):
        other_rel = Relationship(alice, "KNOWS", bob, since=1999)
        assert alice != other_rel

    def test_inequality_with_other_types(self):
        assert alice_knows_bob != "there is no relationship"


class RelationshipLoopTestCase(TestCase):

    loop = Relationship(alice, "LIKES", alice)

    def test_nodes(self):
        nodes = self.loop.nodes()
        assert isinstance(nodes, tuple)
        assert nodes == (alice, alice)

    def test_relationships(self):
        relationships = self.loop.relationships()
        assert isinstance(relationships, tuple)
        assert relationships == (self.loop,)

    def test_order(self):
        assert order(self.loop) == 1

    def test_size(self):
        assert size(self.loop) == 1


class PathTestCase(TestCase):

    path = Path(alice, alice_knows_bob, bob, alice_knows_bob, alice, alice_likes_carol, carol)

    def test_nodes(self):
        nodes = self.path.nodes()
        assert isinstance(nodes, tuple)
        assert nodes == (alice, bob, alice, carol)

    def test_relationships(self):
        relationships = self.path.relationships()
        assert isinstance(relationships, tuple)
        assert relationships == (alice_knows_bob, alice_knows_bob, alice_likes_carol)

    def test_order(self):
        assert order(self.path) == 3

    def test_size(self):
        assert size(self.path) == 2

    def test_length(self):
        assert len(self.path) == 3

    def test_construction_of_path_length_0(self):
        sequence = [alice]
        path = Path(*sequence)
        assert order(path) == 1
        assert size(path) == 0
        assert len(path) == 0
        assert set(path.nodes()) == {alice}
        assert set(path.relationships()) == set()
        assert path.start_node() == alice
        assert path.end_node() == alice
        assert len(path) == 0
        assert list(walk(path)) == sequence

    def test_construction_of_path_length_1(self):
        sequence = [alice, alice_knows_bob, bob]
        path = Path(*sequence)
        assert order(path) == 2
        assert size(path) == 1
        assert len(path) == 1
        assert set(path.nodes()) == {alice, bob}
        assert set(path.relationships()) == {alice_knows_bob}
        assert path.start_node() == alice
        assert path.end_node() == bob
        assert len(path) == 1
        assert list(walk(path)) == sequence

    def test_construction_of_path_length_2(self):
        sequence = [alice, alice_knows_bob, bob, carol_dislikes_bob, carol]
        path = Path(*sequence)
        assert order(path) == 3
        assert size(path) == 2
        assert len(path) == 2
        assert set(path.nodes()) == {alice, bob, carol}
        assert set(path.relationships()) == {alice_knows_bob, carol_dislikes_bob}
        assert path.start_node() == alice
        assert path.end_node() == carol
        assert len(path) == 2
        assert list(walk(path)) == sequence

    def test_construction_of_path_with_revisits(self):
        sequence = [alice, alice_knows_bob, bob, carol_dislikes_bob, carol,
                    alice_likes_carol, alice, alice_knows_bob, bob]
        path = Path(*sequence)
        assert order(path) == 3
        assert size(path) == 3
        assert len(path) == 4
        assert set(path.nodes()) == {alice, bob, carol}
        assert set(path.relationships()) == {alice_knows_bob, alice_likes_carol, carol_dislikes_bob}
        assert path.start_node() == alice
        assert path.end_node() == bob
        assert len(path) == 4
        assert list(walk(path)) == sequence

    def test_construction_of_path_with_loop(self):
        sequence = [carol, carol_married_to_dave, dave, dave_works_for_dave, dave]
        path = Path(*sequence)
        assert order(path) == 2
        assert size(path) == 2
        assert len(path) == 2
        assert set(path.nodes()) == {carol, dave}
        assert set(path.relationships()) == {carol_married_to_dave, dave_works_for_dave}
        assert path.start_node() == carol
        assert path.end_node() == dave
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
        assert order(graph) == 4
        assert size(graph) == 5
        assert graph.nodes() == (alice | bob | carol | dave).nodes()


class IntersectionTestCase(TestCase):

    def test_graph_intersection(self):
        graph_1 = (alice_knows_bob | alice_likes_carol | carol_dislikes_bob)
        graph_2 = (carol_dislikes_bob | carol_married_to_dave | dave_works_for_dave)
        graph = graph_1 & graph_2
        assert order(graph) == 2
        assert size(graph) == 1
        assert graph.nodes() == (bob | carol).nodes()


class DifferenceTestCase(TestCase):

    def test_graph_difference(self):
        graph_1 = (alice_knows_bob | alice_likes_carol | carol_dislikes_bob)
        graph_2 = (carol_dislikes_bob | carol_married_to_dave | dave_works_for_dave)
        graph = graph_1 - graph_2
        assert order(graph) == 3
        assert size(graph) == 2
        assert graph.nodes() == (alice | bob | carol).nodes()


class SymmetricDifferenceTestCase(TestCase):

    def test_graph_symmetric_difference(self):
        graph_1 = (alice_knows_bob | alice_likes_carol | carol_dislikes_bob)
        graph_2 = (carol_dislikes_bob | carol_married_to_dave | dave_works_for_dave)
        graph = graph_1 ^ graph_2
        assert order(graph) == 4
        assert size(graph) == 4
        assert graph.nodes() == (alice | bob | carol | dave).nodes()
        assert graph.relationships() == frozenset(alice_knows_bob | alice_likes_carol |
                                                  carol_married_to_dave | dave_works_for_dave)
