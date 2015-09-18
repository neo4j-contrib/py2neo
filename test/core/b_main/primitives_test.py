#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2015, Nigel Small
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


from unittest import TestCase, main

from py2neo.compat import ustr
from py2neo.primitives import PropertySet, PropertyContainer, TraversableGraph, \
    Node, Relationship, Path


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
        props = PropertySet({"value": True})
        assert props == {"value": True}

    def test_integer_in_range(self):
        props = PropertySet({"value": 1})
        assert props == {"value": 1}

    def test_integer_too_high(self):
        try:
            PropertySet({"value": 2 ** 64})
        except ValueError:
            assert True
        else:
            assert False

    def test_integer_too_low(self):
        try:
            PropertySet({"value": -(2 ** 64)})
        except ValueError:
            assert True
        else:
            assert False

    def test_byte_strings_not_supported(self):
        try:
            PropertySet({"value": b"hello, world"})
        except ValueError:
            assert True
        else:
            assert False

    def test_unicode_strings_are_supported(self):
        props = PropertySet({"value": ustr("hello, world")})
        assert props == {"value": ustr("hello, world")}

    def test_homogenous_list(self):
        props = PropertySet({"value": [1, 2, 3]})
        assert props == {"value": [1, 2, 3]}

    def test_heterogenous_list(self):
        try:
            PropertySet({"value": [True, 2, ustr("three")]})
        except ValueError:
            assert True
        else:
            assert False


class PropertySetTestCase(TestCase):

    def test_equality(self):
        first = PropertySet({"name": "Alice", "age": 33, "colours": ["red", "purple"]})
        second = PropertySet({"name": "Alice", "age": 33, "colours": ["red", "purple"]})
        assert first == second

    def test_inequality(self):
        first = PropertySet({"name": "Alice", "age": 33, "colours": ["red", "purple"]})
        second = PropertySet({"name": "Bob", "age": 44, "colours": ["blue", "purple"]})
        assert first != second

    def test_hashable(self):
        first = PropertySet({"name": "Alice", "age": 33, "colours": ["red", "purple"]})
        second = PropertySet({"name": "Bob", "age": 44, "colours": ["blue", "purple"]})
        collected = {first, second}
        assert len(collected) == 2

    def test_getter(self):
        properties = PropertySet({"name": "Alice"})
        assert properties["name"] == "Alice"

    def test_getter_with_none(self):
        properties = PropertySet({"name": "Alice"})
        assert properties["age"] is None

    def test_setter(self):
        properties = PropertySet({"name": "Alice"})
        properties["age"] = 33
        assert properties == {"name": "Alice", "age": 33}

    def test_setter_with_none(self):
        properties = PropertySet({"name": "Alice", "age": 33})
        properties["age"] = None
        assert properties == {"name": "Alice"}

    def test_setter_with_none_for_non_existent(self):
        properties = PropertySet({"name": "Alice"})
        properties["age"] = None
        assert properties == {"name": "Alice"}

    def test_setdefault_without_default_with_existing(self):
        properties = PropertySet({"name": "Alice", "age": 33})
        value = properties.setdefault("age")
        assert properties == {"name": "Alice", "age": 33}
        assert value == 33

    def test_setdefault_without_default_with_non_existent(self):
        properties = PropertySet({"name": "Alice"})
        value = properties.setdefault("age")
        assert properties == {"name": "Alice", "age": None}
        assert value is None

    def test_setdefault_with_default_with_existing(self):
        properties = PropertySet({"name": "Alice", "age": 33})
        value = properties.setdefault("age", 34)
        assert properties == {"name": "Alice", "age": 33}
        assert value == 33

    def test_setdefault_with_default_with_non_existent(self):
        properties = PropertySet({"name": "Alice"})
        value = properties.setdefault("age", 33)
        assert properties == {"name": "Alice", "age": 33}
        assert value == 33

    def test_deleter(self):
        properties = PropertySet({"name": "Alice", "age": 33})
        del properties["age"]
        assert properties == {"name": "Alice"}


class PropertyContainerTestCase(TestCase):

    def test_length(self):
        container = PropertyContainer(name="Alice", age=33)
        assert len(container) == 2

    def test_contains(self):
        container = PropertyContainer(name="Alice", age=33)
        assert "name" in container

    def test_not_contains(self):
        container = PropertyContainer(name="Alice")
        assert "age" not in container

    def test_getter_where_exists(self):
        container = PropertyContainer(name="Alice", age=33)
        assert container["age"] == 33

    def test_getter_where_non_existent(self):
        container = PropertyContainer(name="Alice")
        assert container["age"] is None

    def test_setter_where_exists(self):
        container = PropertyContainer(name="Alice", age=33)
        container["age"] = 34
        assert container.properties == {"name": "Alice", "age": 34}

    def test_setter_where_non_existent(self):
        container = PropertyContainer(name="Alice")
        container["age"] = 34
        assert container.properties == {"name": "Alice", "age": 34}

    def test_deleter_where_exists(self):
        container = PropertyContainer(name="Alice", age=33)
        del container["age"]
        assert container.properties == {"name": "Alice"}

    def test_deleter_where_non_existent(self):
        container = PropertyContainer(name="Alice")
        try:
            del container["age"]
        except KeyError:
            assert True
        else:
            assert False

    def test_iteration(self):
        container = PropertyContainer(name="Alice", age=33)
        assert set(container) == {"name", "age"}

    def test_property_keys(self):
        container = PropertyContainer(name="Alice", age=33)
        assert container.property_keys == {"name", "age"}


class GraphTestCase(TestCase):

    graph = (alice_knows_bob | alice_likes_carol | carol_dislikes_bob |
             carol_married_to_dave | dave_works_for_dave)

    def test_equality(self):
        other_graph = (alice_knows_bob | alice_likes_carol | carol_dislikes_bob |
                       carol_married_to_dave | dave_works_for_dave)
        assert self.graph == other_graph
        assert hash(self.graph) == hash(other_graph)

    def test_inequality(self):
        other_graph = (alice_knows_bob | alice_likes_carol | carol_dislikes_bob |
                       carol_married_to_dave)
        assert self.graph != other_graph
        assert hash(self.graph) != hash(other_graph)

    def test_inequality_with_other_types(self):
        assert self.graph != "this is not a graph"

    def test_len(self):
        assert len(self.graph) == 5

    def test_bool(self):
        assert self.graph.__bool__() is True
        assert self.graph.__nonzero__() is True

    def test_labels(self):
        assert self.graph.labels == {"Person", "Employee"}

    def test_types(self):
        assert self.graph.types == {"KNOWS", "LIKES", "DISLIKES", "MARRIED_TO", "WORKS_FOR"}

    def test_property_keys(self):
        assert self.graph.property_keys == {"name", "age", "since"}


class TraversableGraphTestCase(TestCase):

    sequence = (alice, alice_knows_bob, bob, carol_dislikes_bob, carol)
    graph = TraversableGraph(*sequence)

    def test_equality(self):
        other_graph = TraversableGraph(*self.sequence)
        assert self.graph == other_graph
        assert hash(self.graph) == hash(other_graph)

    def test_inequality(self):
        other_graph = TraversableGraph(alice, alice_likes_carol, carol, carol_dislikes_bob, bob)
        assert self.graph != other_graph
        assert hash(self.graph) != hash(other_graph)

    def test_inequality_with_other_types(self):
        assert self.graph != "this is not a graph"

    def test_iteration(self):
        assert tuple(iter(self.graph)) == (alice_knows_bob, carol_dislikes_bob)

    def test_slicing(self):
        sequence = (alice, alice_knows_bob, bob, carol_dislikes_bob, carol,
                    carol_married_to_dave, dave, dave_works_for_dave, dave)
        graph = TraversableGraph(*sequence)
        assert graph[0] == alice_knows_bob
        assert graph[1] == carol_dislikes_bob
        assert graph[2] == carol_married_to_dave
        assert graph[3] == dave_works_for_dave
        assert graph[0:0] == TraversableGraph(alice)
        assert graph[0:1] == TraversableGraph(alice, alice_knows_bob, bob)
        assert graph[0:2] == TraversableGraph(alice, alice_knows_bob, bob,
                                              carol_dislikes_bob, carol)
        assert graph[0:3] == TraversableGraph(alice, alice_knows_bob, bob,
                                              carol_dislikes_bob, carol,
                                              carol_married_to_dave, dave)
        assert graph[0:4] == TraversableGraph(alice, alice_knows_bob, bob,
                                              carol_dislikes_bob, carol,
                                              carol_married_to_dave, dave,
                                              dave_works_for_dave, dave)
        assert graph[0:5] == TraversableGraph(alice, alice_knows_bob, bob,
                                              carol_dislikes_bob, carol,
                                              carol_married_to_dave, dave,
                                              dave_works_for_dave, dave)
        assert graph[0:] == TraversableGraph(alice, alice_knows_bob, bob,
                                             carol_dislikes_bob, carol,
                                             carol_married_to_dave, dave,
                                             dave_works_for_dave, dave)
        assert graph[:1] == TraversableGraph(alice, alice_knows_bob, bob)
        assert graph[1:1] == TraversableGraph(bob)
        assert graph[1:2] == TraversableGraph(bob, carol_dislikes_bob, carol)
        assert graph[1:3] == TraversableGraph(bob, carol_dislikes_bob, carol,
                                              carol_married_to_dave, dave)
        assert graph[1:4] == TraversableGraph(bob, carol_dislikes_bob, carol,
                                              carol_married_to_dave, dave,
                                              dave_works_for_dave, dave)
        assert graph[1:5] == TraversableGraph(bob, carol_dislikes_bob, carol,
                                              carol_married_to_dave, dave,
                                              dave_works_for_dave, dave)
        assert graph[1:] == TraversableGraph(bob, carol_dislikes_bob, carol,
                                             carol_married_to_dave, dave,
                                             dave_works_for_dave, dave)
        assert graph[:2] == TraversableGraph(alice, alice_knows_bob, bob,
                                             carol_dislikes_bob, carol)
        assert graph[2:2] == TraversableGraph(carol)
        assert graph[2:3] == TraversableGraph(carol, carol_married_to_dave, dave)
        assert graph[2:4] == TraversableGraph(carol, carol_married_to_dave, dave,
                                              dave_works_for_dave, dave)
        assert graph[2:5] == TraversableGraph(carol, carol_married_to_dave, dave,
                                              dave_works_for_dave, dave)
        assert graph[2:] == TraversableGraph(carol, carol_married_to_dave, dave,
                                             dave_works_for_dave, dave)
        assert graph[1:-1] == TraversableGraph(bob, carol_dislikes_bob, carol,
                                               carol_married_to_dave, dave)
        assert graph[-3:-1] == TraversableGraph(bob, carol_dislikes_bob, carol,
                                                carol_married_to_dave, dave)


class NodeTestCase(TestCase):

    def test_node(self):
        assert repr(alice)
        assert alice.start_node == alice
        assert alice.end_node == alice
        assert alice.length == 0
        assert list(alice.traverse()) == [alice]
        assert alice.labels == {"Person", "Employee"}
        assert alice.properties == {"name": "Alice", "age": 33}
        assert alice.properties["name"] == "Alice"
        assert alice["name"] == "Alice"
        assert alice.order == 1
        assert alice.size == 0
        assert set(alice.nodes) == {alice}
        assert set(alice.relationships) == set()

    def test_equality(self):
        other_node = Node("Person", "Employee", name="Alice", age=33)
        assert alice == other_node

    def test_inequality(self):
        other_node = Node("Person", "Employee", name="Bob", age=44)
        assert alice != other_node

    def test_inequality_with_other_types(self):
        assert alice != "this is not a node"


class RelationshipTestCase(TestCase):

    def test_relationship(self):
        assert repr(alice_knows_bob)
        assert alice_knows_bob.start_node == alice
        assert alice_knows_bob.end_node == bob
        assert alice_knows_bob.length == 1
        assert list(alice_knows_bob.traverse()) == [alice, alice_knows_bob, bob]
        assert alice_knows_bob.type == "KNOWS"
        assert alice_knows_bob.properties == {"since": 1999}
        assert alice_knows_bob.properties["since"] == 1999
        assert alice_knows_bob["since"] == 1999
        assert alice_knows_bob.order == 2
        assert alice_knows_bob.size == 1
        assert set(alice_knows_bob.nodes) == {alice, bob}
        assert set(alice_knows_bob.relationships) == {alice_knows_bob}

    def test_loop(self):
        assert dave_works_for_dave.start_node == dave
        assert dave_works_for_dave.end_node == dave
        assert dave_works_for_dave.length == 1
        assert list(dave_works_for_dave.traverse()) == [dave, dave_works_for_dave, dave]
        assert dave_works_for_dave.order == 1
        assert dave_works_for_dave.size == 1
        assert set(dave_works_for_dave.nodes) == {dave}
        assert set(dave_works_for_dave.relationships) == {dave_works_for_dave}

    def test_construction_from_zero_arguments(self):
        rel = Relationship()
        assert repr(rel)
        assert rel.start_node is None
        assert rel.end_node is None
        assert rel.type is None

    def test_construction_from_one_argument(self):
        rel = Relationship("KNOWS")
        assert repr(rel)
        assert rel.start_node is None
        assert rel.end_node is None
        assert rel.type == "KNOWS"

    def test_construction_from_two_arguments(self):
        rel = Relationship(alice, bob)
        assert repr(rel)
        assert rel.start_node is alice
        assert rel.end_node is bob
        assert rel.type is None

    def test_construction_from_three_arguments(self):
        rel = Relationship(alice, "KNOWS", bob)
        assert repr(rel)
        assert rel.start_node is alice
        assert rel.end_node is bob
        assert rel.type == "KNOWS"

    def test_construction_from_more_arguments(self):
        try:
            Relationship(alice, "KNOWS", bob, carol)
        except TypeError:
            assert True
        else:
            assert False

    def test_equality(self):
        other_rel = alice_knows_bob
        assert alice_knows_bob == other_rel

    def test_inequality(self):
        other_rel = Relationship(alice, "KNOWS", bob, since=1999)
        assert alice != other_rel

    def test_inequality_with_other_types(self):
        assert alice_knows_bob != "there is no relationship"


class PathTestCase(TestCase):

    def test_construction_of_path_length_0(self):
        sequence = [alice]
        path = Path(*sequence)
        assert repr(path)
        assert path.order == 1
        assert path.size == 0
        assert path.length == 0
        assert set(path.nodes) == {alice}
        assert set(path.relationships) == set()
        assert path.start_node == alice
        assert path.end_node == alice
        assert path.length == 0
        assert list(path.traverse()) == sequence

    def test_construction_of_path_length_1(self):
        sequence = [alice, alice_knows_bob, bob]
        path = Path(*sequence)
        assert repr(path)
        assert path.order == 2
        assert path.size == 1
        assert path.length == 1
        assert set(path.nodes) == {alice, bob}
        assert set(path.relationships) == {alice_knows_bob}
        assert path.start_node == alice
        assert path.end_node == bob
        assert path.length == 1
        assert list(path.traverse()) == sequence

    def test_construction_of_path_length_2(self):
        sequence = [alice, alice_knows_bob, bob, carol_dislikes_bob, carol]
        path = Path(*sequence)
        assert repr(path)
        assert path.order == 3
        assert path.size == 2
        assert path.length == 2
        assert set(path.nodes) == {alice, bob, carol}
        assert set(path.relationships) == {alice_knows_bob, carol_dislikes_bob}
        assert path.start_node == alice
        assert path.end_node == carol
        assert path.length == 2
        assert list(path.traverse()) == sequence

    def test_construction_of_path_with_revisits(self):
        sequence = [alice, alice_knows_bob, bob, carol_dislikes_bob, carol,
                    alice_likes_carol, alice, alice_knows_bob, bob]
        path = Path(*sequence)
        assert repr(path)
        assert path.order == 3
        assert path.size == 3
        assert path.length == 4
        assert set(path.nodes) == {alice, bob, carol}
        assert set(path.relationships) == {alice_knows_bob, alice_likes_carol, carol_dislikes_bob}
        assert path.start_node == alice
        assert path.end_node == bob
        assert path.length == 4
        assert list(path.traverse()) == sequence

    def test_construction_of_path_with_loop(self):
        sequence = [carol, carol_married_to_dave, dave, dave_works_for_dave, dave]
        path = Path(*sequence)
        assert repr(path)
        assert path.order == 2
        assert path.size == 2
        assert path.length == 2
        assert set(path.nodes) == {carol, dave}
        assert set(path.relationships) == {carol_married_to_dave, dave_works_for_dave}
        assert path.start_node == carol
        assert path.end_node == dave
        assert path.length == 2
        assert list(path.traverse()) == sequence


class ConcatenationTestCase(TestCase):
    # TODO: concatenations with items in reverse

    def test_can_concatenate_node_and_node(self):
        result = alice + alice
        assert result == TraversableGraph(alice)

    def test_can_concatenate_node_and_relationship(self):
        result = alice + alice_knows_bob
        assert result == TraversableGraph(alice, alice_knows_bob, bob)

    def test_can_concatenate_node_and_reversed_relationship(self):
        bob_knows_alice = Relationship(bob, "KNOWS", alice)
        result = alice + bob_knows_alice
        assert result == TraversableGraph(alice, bob_knows_alice, bob)

    def test_can_concatenate_node_and_path(self):
        path = TraversableGraph(alice, alice_knows_bob, bob)
        result = alice + path
        assert result == path

    def test_can_concatenate_node_and_reversed_path(self):
        result = alice + TraversableGraph(bob, alice_knows_bob, alice)
        assert result == TraversableGraph(alice, alice_knows_bob, bob)

    def test_can_concatenate_relationship_and_node(self):
        bob_knows_alice = Relationship(bob, "KNOWS", alice)
        result = bob_knows_alice + bob
        assert result == TraversableGraph(alice, bob_knows_alice, bob)

    def test_can_concatenate_reversed_relationship_and_node(self):
        result = alice_knows_bob + bob
        assert result == TraversableGraph(alice, alice_knows_bob, bob)

    def test_can_concatenate_relationship_and_relationship(self):
        result = alice_knows_bob + carol_dislikes_bob
        assert result == TraversableGraph(alice, alice_knows_bob, bob, carol_dislikes_bob, carol)

    def test_can_concatenate_reversed_relationship_and_reversed_relationship(self):
        bob_knows_alice = Relationship(bob, "KNOWS", alice)
        result = bob_knows_alice + carol_dislikes_bob
        assert result == TraversableGraph(alice, bob_knows_alice, bob, carol_dislikes_bob, carol)

    def test_can_concatenate_relationship_and_path(self):
        result = alice_knows_bob + TraversableGraph(bob, carol_dislikes_bob, carol)
        assert result == TraversableGraph(alice, alice_knows_bob, bob, carol_dislikes_bob, carol)

    def test_can_concatenate_path_and_node(self):
        result = TraversableGraph(alice, alice_knows_bob, bob) + bob
        assert result == TraversableGraph(alice, alice_knows_bob, bob)

    def test_can_concatenate_path_and_relationship(self):
        result = TraversableGraph(alice, alice_knows_bob, bob) + carol_dislikes_bob
        assert result == TraversableGraph(alice, alice_knows_bob, bob, carol_dislikes_bob, carol)

    def test_can_concatenate_path_and_path(self):
        result = TraversableGraph(alice, alice_knows_bob, bob) + TraversableGraph(bob, carol_dislikes_bob, carol)
        assert result == TraversableGraph(alice, alice_knows_bob, bob, carol_dislikes_bob, carol)

    def test_cannot_concatenate_different_endpoints(self):
        try:
            result = alice + bob
        except ValueError:
            assert True
        else:
            assert False


class UnionTestCase(TestCase):

    def test_graph_union(self):
        graph_1 = (alice_knows_bob | alice_likes_carol | carol_dislikes_bob)
        graph_2 = (carol_dislikes_bob | carol_married_to_dave | dave_works_for_dave)
        graph = graph_1 | graph_2
        assert graph.order == 4
        assert graph.size == 5
        assert graph.nodes == (alice | bob | carol | dave).nodes


class IntersectionTestCase(TestCase):

    def test_graph_intersection(self):
        graph_1 = (alice_knows_bob | alice_likes_carol | carol_dislikes_bob)
        graph_2 = (carol_dislikes_bob | carol_married_to_dave | dave_works_for_dave)
        graph = graph_1 & graph_2
        assert graph.order == 2
        assert graph.size == 1
        assert graph.nodes == (bob | carol).nodes


class DifferenceTestCase(TestCase):

    def test_graph_difference(self):
        graph_1 = (alice_knows_bob | alice_likes_carol | carol_dislikes_bob)
        graph_2 = (carol_dislikes_bob | carol_married_to_dave | dave_works_for_dave)
        graph = graph_1 - graph_2
        assert graph.order == 3
        assert graph.size == 2
        assert graph.nodes == (alice | bob | carol).nodes


class SymmetricDifferenceTestCase(TestCase):

    def test_graph_symmetric_difference(self):
        graph_1 = (alice_knows_bob | alice_likes_carol | carol_dislikes_bob)
        graph_2 = (carol_dislikes_bob | carol_married_to_dave | dave_works_for_dave)
        graph = graph_1 ^ graph_2
        assert graph.order == 4
        assert graph.size == 4
        assert graph.nodes == (alice | bob | carol | dave).nodes
        assert graph.relationships == frozenset(alice_knows_bob | alice_likes_carol |
                                                carol_married_to_dave | dave_works_for_dave)


if __name__ == "__main__":
    main()
