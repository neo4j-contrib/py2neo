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

from py2neo.data import PropertySet, PropertyContainer
from py2neo.graph import Subgraph, TraversableSubgraph, Node, Relationship, Path, traverse
from py2neo.cypher import Record


alice = Node("Person", "Employee", name="Alice", age=33)
bob = Node("Person")
carol = Node("Person")
dave = Node("Person")

alice_knows_bob = Relationship(alice, "KNOWS", bob, since=1999)
alice_likes_carol = Relationship(alice, "LIKES", carol)
carol_dislikes_bob = Relationship(carol, "DISLIKES", bob)
carol_married_to_dave = Relationship(carol, "MARRIED_TO", dave)
dave_works_for_dave = Relationship(dave, "WORKS_FOR", dave)

record_keys = ["employee_id", "Person"]
record_a = Record(record_keys, [1001, alice])
record_b = Record(record_keys, [1002, bob])
record_c = Record(record_keys, [1003, carol])
record_d = Record(record_keys, [1004, dave])


class PropertyCoercionTestCase(TestCase):

    def test_boolean(self):
        props = PropertySet({"value": True})
        assert props == {"value": True}

    def test_integer_in_range(self):
        props = PropertySet({"value": 1})
        assert props == {"value": 1}

    def test_integer_too_high(self):
        with self.assertRaises(ValueError):
            PropertySet({"value": 2 ** 64})

    def test_integer_too_low(self):
        with self.assertRaises(ValueError):
            PropertySet({"value": -(2 ** 64)})

    def test_float(self):
        props = PropertySet({"value": 3.141})
        assert props == {"value": 3.141}

    def test_byte_strings_are_supported(self):
        props = PropertySet({"value": b"hello, world"})
        assert props == {"value": u"hello, world"}

    def test_unicode_strings_are_supported(self):
        props = PropertySet({"value": u"hello, world"})
        assert props == {"value": u"hello, world"}

    def test_byte_arrays_are_not_supported(self):
        with self.assertRaises(TypeError):
            PropertySet({"value": bytearray(b"hello, world")})

    def test_homogenous_list(self):
        props = PropertySet({"value": [1, 2, 3]})
        assert props == {"value": [1, 2, 3]}

    def test_homogenous_list_of_strings(self):
        props = PropertySet({"value": [u"hello", b"world"]})
        assert props == {"value": [u"hello", u"world"]}

    def test_heterogenous_list(self):
        with self.assertRaises(TypeError):
            PropertySet({"value": [True, 2, u"three"]})


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

    def test_clear(self):
        container = PropertyContainer(name="Alice", age=33)
        container.clear()
        assert len(container) == 0

    def test_getter_where_exists(self):
        container = PropertyContainer(name="Alice", age=33)
        assert container["age"] == 33

    def test_getter_where_non_existent(self):
        container = PropertyContainer(name="Alice")
        assert container["age"] is None

    def test_get_method(self):
        container = PropertyContainer(name="Alice", age=33)
        assert container.get("age") == 33

    def test_get_method_with_default(self):
        container = PropertyContainer(name="Alice")
        assert container.get("age", 33) == 33

    def test_setter_where_exists(self):
        container = PropertyContainer(name="Alice", age=33)
        container["age"] = 34
        assert dict(container) == {"name": "Alice", "age": 34}

    def test_setter_where_non_existent(self):
        container = PropertyContainer(name="Alice")
        container["age"] = 34
        assert dict(container) == {"name": "Alice", "age": 34}

    def test_setdefault_where_exists(self):
        container = PropertyContainer(name="Alice", age=33)
        assert container.setdefault("age", 44) == 33

    def test_setdefault_where_missing(self):
        container = PropertyContainer(name="Alice")
        assert container.setdefault("age", 44) == 44

    def test_deleter_where_exists(self):
        container = PropertyContainer(name="Alice", age=33)
        del container["age"]
        assert dict(container) == {"name": "Alice"}

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

    def test_keys(self):
        container = PropertyContainer(name="Alice", age=33)
        assert set(container.keys()) == {"name", "age"}

    def test_values(self):
        container = PropertyContainer(name="Alice", age=33)
        assert set(container.values()) == {'Alice', 33}

    def test_update(self):
        container = PropertyContainer(name="Alice")
        container.update({"name": "Alice", "age": 33})
        assert dict(container) == {"name": "Alice", "age": 33}


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
        assert self.subgraph.order() == 4

    def test_size(self):
        assert self.subgraph.size() == 5

    def test_can_infer_nodes_through_relationships(self):
        subgraph = Subgraph([], [alice_knows_bob])
        assert subgraph.order() == 2
        assert subgraph.size() == 1
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


class TraversableSubgraphTestCase(TestCase):

    sequence = (alice, alice_knows_bob, bob, carol_dislikes_bob, carol)
    subgraph = TraversableSubgraph(*sequence)

    def test_nodes(self):
        nodes = self.subgraph.nodes()
        assert isinstance(nodes, tuple)
        assert nodes == (alice, bob, carol)

    def test_relationships(self):
        relationships = self.subgraph.relationships()
        assert isinstance(relationships, tuple)
        assert relationships == (alice_knows_bob, carol_dislikes_bob)

    def test_order(self):
        assert self.subgraph.order() == 3

    def test_size(self):
        assert self.subgraph.size() == 2

    def test_equality(self):
        other_subgraph = TraversableSubgraph(*self.sequence)
        assert self.subgraph == other_subgraph
        assert hash(self.subgraph) == hash(other_subgraph)

    def test_inequality(self):
        other_subgraph = TraversableSubgraph(alice, alice_likes_carol, carol,
                                             carol_dislikes_bob, bob)
        assert self.subgraph != other_subgraph
        assert hash(self.subgraph) != hash(other_subgraph)

    def test_inequality_with_other_types(self):
        assert self.subgraph != "this is not a graph"

    def test_iteration(self):
        assert tuple(iter(self.subgraph)) == (alice_knows_bob, carol_dislikes_bob)

    def test_slicing(self):
        sequence = (alice, alice_knows_bob, bob, carol_dislikes_bob, carol,
                    carol_married_to_dave, dave, dave_works_for_dave, dave)
        subgraph = TraversableSubgraph(*sequence)
        assert subgraph[0] == alice_knows_bob
        assert subgraph[1] == carol_dislikes_bob
        assert subgraph[2] == carol_married_to_dave
        assert subgraph[3] == dave_works_for_dave
        assert subgraph[0:0] == TraversableSubgraph(alice)
        assert subgraph[0:1] == TraversableSubgraph(alice, alice_knows_bob, bob)
        assert subgraph[0:2] == TraversableSubgraph(alice, alice_knows_bob, bob,
                                                    carol_dislikes_bob, carol)
        assert subgraph[0:3] == TraversableSubgraph(alice, alice_knows_bob, bob,
                                                    carol_dislikes_bob, carol,
                                                    carol_married_to_dave, dave)
        assert subgraph[0:4] == TraversableSubgraph(alice, alice_knows_bob, bob,
                                                    carol_dislikes_bob, carol,
                                                    carol_married_to_dave, dave,
                                                    dave_works_for_dave, dave)
        assert subgraph[0:5] == TraversableSubgraph(alice, alice_knows_bob, bob,
                                                    carol_dislikes_bob, carol,
                                                    carol_married_to_dave, dave,
                                                    dave_works_for_dave, dave)
        assert subgraph[0:] == TraversableSubgraph(alice, alice_knows_bob, bob,
                                                   carol_dislikes_bob, carol,
                                                   carol_married_to_dave, dave,
                                                   dave_works_for_dave, dave)
        assert subgraph[:1] == TraversableSubgraph(alice, alice_knows_bob, bob)
        assert subgraph[1:1] == TraversableSubgraph(bob)
        assert subgraph[1:2] == TraversableSubgraph(bob, carol_dislikes_bob, carol)
        assert subgraph[1:3] == TraversableSubgraph(bob, carol_dislikes_bob, carol,
                                                    carol_married_to_dave, dave)
        assert subgraph[1:4] == TraversableSubgraph(bob, carol_dislikes_bob, carol,
                                                    carol_married_to_dave, dave,
                                                    dave_works_for_dave, dave)
        assert subgraph[1:5] == TraversableSubgraph(bob, carol_dislikes_bob, carol,
                                                    carol_married_to_dave, dave,
                                                    dave_works_for_dave, dave)
        assert subgraph[1:] == TraversableSubgraph(bob, carol_dislikes_bob, carol,
                                                   carol_married_to_dave, dave,
                                                   dave_works_for_dave, dave)
        assert subgraph[:2] == TraversableSubgraph(alice, alice_knows_bob, bob,
                                                   carol_dislikes_bob, carol)
        assert subgraph[2:2] == TraversableSubgraph(carol)
        assert subgraph[2:3] == TraversableSubgraph(carol, carol_married_to_dave, dave)
        assert subgraph[2:4] == TraversableSubgraph(carol, carol_married_to_dave, dave,
                                                    dave_works_for_dave, dave)
        assert subgraph[2:5] == TraversableSubgraph(carol, carol_married_to_dave, dave,
                                                    dave_works_for_dave, dave)
        assert subgraph[2:] == TraversableSubgraph(carol, carol_married_to_dave, dave,
                                                   dave_works_for_dave, dave)
        assert subgraph[1:-1] == TraversableSubgraph(bob, carol_dislikes_bob, carol,
                                                     carol_married_to_dave, dave)
        assert subgraph[-3:-1] == TraversableSubgraph(bob, carol_dislikes_bob, carol,
                                                      carol_married_to_dave, dave)


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
        assert alice.order() == 1

    def test_size(self):
        assert alice.size() == 0

    def test_empty_node(self):
        n = Node()
        assert not n.__bool__()
        assert not n.__nonzero__()
        assert len(n) == 0

    def test_node(self):
        assert repr(alice)
        assert alice.start_node() == alice
        assert alice.end_node() == alice
        assert alice.__bool__()
        assert alice.__nonzero__()
        assert len(alice) == 2
        assert alice.length() == 0
        assert list(alice.traverse()) == [alice]
        assert alice.labels() == {"Person", "Employee"}
        assert dict(alice) == {"name": "Alice", "age": 33}
        assert dict(alice)["name"] == "Alice"
        assert alice["name"] == "Alice"
        assert alice.order() == 1
        assert alice.size() == 0
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
        node.labels().add("Employee")
        assert node.labels() == {"Person", "Employee"}

    def test_add_label_is_idempotent(self):
        node = Node("Person", name="Alice")
        node.labels().add("Employee")
        node.labels().add("Employee")
        assert node.labels() == {"Person", "Employee"}

    def test_can_remove_label(self):
        node = Node("Person", "Employee", name="Alice")
        node.labels().remove("Employee")
        assert node.labels() == {"Person"}

    def test_removing_non_existent_label_fails(self):
        node = Node("Person", name="Alice")
        with self.assertRaises(KeyError):
            node.labels().remove("Employee")

    def test_can_discard_label(self):
        node = Node("Person", "Employee", name="Alice")
        node.labels().discard("Employee")
        assert node.labels() == {"Person"}

    def test_discarding_non_existent_label_is_ignored(self):
        node = Node("Person", name="Alice")
        node.labels().discard("Employee")
        assert node.labels() == {"Person"}

    def test_can_clear_labels(self):
        node = Node("Person", "Employee", name="Alice")
        node.labels().clear()
        assert node.labels() == set()

    def test_has_label(self):
        node = Node("Person", name="Alice")
        assert "Person" in node.labels()
        assert "Employee" not in node.labels()

    def test_update_labels(self):
        node = Node("Person", name="Alice")
        node.labels().update({"Person", "Employee"})
        assert node.labels() == {"Person", "Employee"}


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
        assert alice_knows_bob.order() == 2

    def test_size(self):
        assert alice_knows_bob.size() == 1

    def test_relationship(self):
        assert repr(alice_knows_bob)
        assert alice_knows_bob.start_node() == alice
        assert alice_knows_bob.end_node() == bob
        assert alice_knows_bob.length() == 1
        assert list(alice_knows_bob.traverse()) == [alice, alice_knows_bob, bob]
        assert alice_knows_bob.type() == "KNOWS"
        assert dict(alice_knows_bob) == {"since": 1999}
        assert alice_knows_bob["since"] == 1999
        assert alice_knows_bob.order() == 2
        assert alice_knows_bob.size() == 1
        assert set(alice_knows_bob.nodes()) == {alice, bob}
        assert set(alice_knows_bob.relationships()) == {alice_knows_bob}

    def test_loop(self):
        assert dave_works_for_dave.start_node() == dave
        assert dave_works_for_dave.end_node() == dave
        assert dave_works_for_dave.length() == 1
        assert list(dave_works_for_dave.traverse()) == [dave, dave_works_for_dave, dave]
        assert dave_works_for_dave.order() == 1
        assert dave_works_for_dave.size() == 1
        assert set(dave_works_for_dave.nodes()) == {dave}
        assert set(dave_works_for_dave.relationships()) == {dave_works_for_dave}

    def test_construction_from_no_arguments(self):
        with self.assertRaises(TypeError):
            _ = Relationship()

    def test_construction_from_one_argument(self):
        rel = Relationship(alice)
        assert repr(rel)
        assert rel.start_node() is alice
        assert rel.end_node() is alice
        assert rel.type() is None

    def test_construction_from_two_node_arguments(self):
        rel = Relationship(alice, bob)
        assert repr(rel)
        assert rel.start_node() is alice
        assert rel.end_node() is bob
        assert rel.type() is None

    def test_construction_from_node_and_type_arguments(self):
        rel = Relationship(alice, "LIKES")
        assert repr(rel)
        assert rel.start_node() is alice
        assert rel.end_node() is alice
        assert rel.type() == "LIKES"

    def test_construction_from_three_arguments(self):
        rel = Relationship(alice, "KNOWS", bob)
        assert repr(rel)
        assert rel.start_node() is alice
        assert rel.end_node() is bob
        assert rel.type() == "KNOWS"

    def test_construction_with_explicit_none_type(self):
        rel = Relationship(alice, None, bob)
        assert repr(rel)
        assert rel.start_node() is alice
        assert rel.end_node() is bob
        assert rel.type() is None

    def test_construction_from_subclass(self):
        class WorksWith(Relationship):
            pass
        rel = WorksWith(alice, bob)
        assert repr(rel)
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
        assert self.loop.order() == 1

    def test_size(self):
        assert self.loop.size() == 1


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
        assert self.path.order() == 3

    def test_size(self):
        assert self.path.size() == 2

    def test_length(self):
        assert self.path.length() == 3

    def test_construction_of_path_length_0(self):
        sequence = [alice]
        path = Path(*sequence)
        assert repr(path)
        assert path.order() == 1
        assert path.size() == 0
        assert path.length() == 0
        assert set(path.nodes()) == {alice}
        assert set(path.relationships()) == set()
        assert path.start_node() == alice
        assert path.end_node() == alice
        assert path.length() == 0
        assert list(path.traverse()) == sequence

    def test_construction_of_path_length_1(self):
        sequence = [alice, alice_knows_bob, bob]
        path = Path(*sequence)
        assert repr(path)
        assert path.order() == 2
        assert path.size() == 1
        assert path.length() == 1
        assert set(path.nodes()) == {alice, bob}
        assert set(path.relationships()) == {alice_knows_bob}
        assert path.start_node() == alice
        assert path.end_node() == bob
        assert path.length() == 1
        assert list(path.traverse()) == sequence

    def test_construction_of_path_length_2(self):
        sequence = [alice, alice_knows_bob, bob, carol_dislikes_bob, carol]
        path = Path(*sequence)
        assert repr(path)
        assert path.order() == 3
        assert path.size() == 2
        assert path.length() == 2
        assert set(path.nodes()) == {alice, bob, carol}
        assert set(path.relationships()) == {alice_knows_bob, carol_dislikes_bob}
        assert path.start_node() == alice
        assert path.end_node() == carol
        assert path.length() == 2
        assert list(path.traverse()) == sequence

    def test_construction_of_path_with_revisits(self):
        sequence = [alice, alice_knows_bob, bob, carol_dislikes_bob, carol,
                    alice_likes_carol, alice, alice_knows_bob, bob]
        path = Path(*sequence)
        assert repr(path)
        assert path.order() == 3
        assert path.size() == 3
        assert path.length() == 4
        assert set(path.nodes()) == {alice, bob, carol}
        assert set(path.relationships()) == {alice_knows_bob, alice_likes_carol, carol_dislikes_bob}
        assert path.start_node() == alice
        assert path.end_node() == bob
        assert path.length() == 4
        assert list(path.traverse()) == sequence

    def test_construction_of_path_with_loop(self):
        sequence = [carol, carol_married_to_dave, dave, dave_works_for_dave, dave]
        path = Path(*sequence)
        assert repr(path)
        assert path.order() == 2
        assert path.size() == 2
        assert path.length() == 2
        assert set(path.nodes()) == {carol, dave}
        assert set(path.relationships()) == {carol_married_to_dave, dave_works_for_dave}
        assert path.start_node() == carol
        assert path.end_node() == dave
        assert path.length() == 2
        assert list(path.traverse()) == sequence

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


class TraversalTestCase(TestCase):

    def test_can_traverse_nothing(self):
        result = list(traverse())
        assert result == []

    def test_can_traverse_node(self):
        result = list(traverse(alice))
        assert result == [alice]

    def test_can_traverse_node_twice(self):
        result = list(traverse(alice, alice))
        assert result == [alice]

    def test_can_traverse_node_and_relationship(self):
        result = list(traverse(alice, alice_knows_bob))
        assert result == [alice, alice_knows_bob, bob]

    def test_can_traverse_node_relationship_and_node(self):
        result = list(traverse(alice, alice_knows_bob, bob))
        assert result == [alice, alice_knows_bob, bob]

    def test_can_traverse_node_relationship_and_node_in_reverse(self):
        result = list(traverse(bob, alice_knows_bob, alice))
        assert result == [bob, alice_knows_bob, alice]

    def test_cannot_traverse_non_traversable_as_first_argument(self):
        with self.assertRaises(TypeError):
            list(traverse(object()))

    def test_cannot_traverse_non_traversable_as_second_argument(self):
        with self.assertRaises(TypeError):
            list(traverse(alice, object()))


class ConcatenationTestCase(TestCase):

    def test_can_concatenate_node_and_node(self):
        result = alice + alice
        assert result == TraversableSubgraph(alice)

    def test_can_concatenate_node_and_relationship(self):
        result = alice + alice_knows_bob
        assert result == TraversableSubgraph(alice, alice_knows_bob, bob)

    def test_can_concatenate_node_and_reversed_relationship(self):
        bob_knows_alice = Relationship(bob, "KNOWS", alice)
        result = alice + bob_knows_alice
        assert result == TraversableSubgraph(alice, bob_knows_alice, bob)

    def test_can_concatenate_node_and_path(self):
        path = TraversableSubgraph(alice, alice_knows_bob, bob)
        result = alice + path
        assert result == path

    def test_can_concatenate_node_and_reversed_path(self):
        result = alice + TraversableSubgraph(bob, alice_knows_bob, alice)
        assert result == TraversableSubgraph(alice, alice_knows_bob, bob)

    def test_can_concatenate_relationship_and_node(self):
        result = alice_knows_bob + bob
        assert result == TraversableSubgraph(alice, alice_knows_bob, bob)

    def test_can_concatenate_relationship_and_relationship(self):
        result = alice_knows_bob + carol_dislikes_bob
        assert result == TraversableSubgraph(alice, alice_knows_bob, bob, carol_dislikes_bob, carol)

    def test_can_concatenate_relationship_and_path(self):
        result = alice_knows_bob + TraversableSubgraph(bob, carol_dislikes_bob, carol)
        assert result == TraversableSubgraph(alice, alice_knows_bob, bob, carol_dislikes_bob, carol)

    def test_can_concatenate_path_and_node(self):
        result = TraversableSubgraph(alice, alice_knows_bob, bob) + bob
        assert result == TraversableSubgraph(alice, alice_knows_bob, bob)

    def test_can_concatenate_path_and_relationship(self):
        result = TraversableSubgraph(alice, alice_knows_bob, bob) + carol_dislikes_bob
        assert result == TraversableSubgraph(alice, alice_knows_bob, bob, carol_dislikes_bob, carol)

    def test_can_concatenate_path_and_path(self):
        result = (TraversableSubgraph(alice, alice_knows_bob, bob) +
                  TraversableSubgraph(bob, carol_dislikes_bob, carol))
        assert result == TraversableSubgraph(alice, alice_knows_bob, bob, carol_dislikes_bob, carol)

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
        assert graph.order() == 4
        assert graph.size() == 5
        assert graph.nodes() == (alice | bob | carol | dave).nodes()


class IntersectionTestCase(TestCase):

    def test_graph_intersection(self):
        graph_1 = (alice_knows_bob | alice_likes_carol | carol_dislikes_bob)
        graph_2 = (carol_dislikes_bob | carol_married_to_dave | dave_works_for_dave)
        graph = graph_1 & graph_2
        assert graph.order() == 2
        assert graph.size() == 1
        assert graph.nodes() == (bob | carol).nodes()


class DifferenceTestCase(TestCase):

    def test_graph_difference(self):
        graph_1 = (alice_knows_bob | alice_likes_carol | carol_dislikes_bob)
        graph_2 = (carol_dislikes_bob | carol_married_to_dave | dave_works_for_dave)
        graph = graph_1 - graph_2
        assert graph.order() == 3
        assert graph.size() == 2
        assert graph.nodes() == (alice | bob | carol).nodes()


class SymmetricDifferenceTestCase(TestCase):

    def test_graph_symmetric_difference(self):
        graph_1 = (alice_knows_bob | alice_likes_carol | carol_dislikes_bob)
        graph_2 = (carol_dislikes_bob | carol_married_to_dave | dave_works_for_dave)
        graph = graph_1 ^ graph_2
        assert graph.order() == 4
        assert graph.size() == 4
        assert graph.nodes() == (alice | bob | carol | dave).nodes()
        assert graph.relationships() == frozenset(alice_knows_bob | alice_likes_carol |
                                                  carol_married_to_dave | dave_works_for_dave)


class RecordTestCase(TestCase):

    def test_can_build_record(self):
        record = Record(["name", "age"], ["Alice", 33])
        assert len(record) == 2
        assert record.keys() == ("name", "age")
        assert record.values() == ("Alice", 33)
        r = repr(record)
        assert r.startswith("(") and r.endswith(")")

    def test_cannot_build_record_with_mismatched_keys_and_values(self):
        with self.assertRaises(ValueError):
            Record(["name"], ["Alice", 33])

    def test_can_coerce_record(self):
        record = Record(["name", "age"], ["Alice", 33])
        assert tuple(record) == ("Alice", 33)
        assert list(record) == ["Alice", 33]
        assert dict(record) == {"name": "Alice", "age": 33}

    def test_can_get_record_value_by_name(self):
        record = Record(["one", "two", "three"], ["eins", "zwei", "drei"])
        assert record["one"] == "eins"
        assert record["two"] == "zwei"
        assert record["three"] == "drei"

    def test_cannot_get_record_value_by_missing_name(self):
        record = Record(["one", "two", "three"], ["eins", "zwei", "drei"])
        with self.assertRaises(KeyError):
            _ = record["four"]

    def test_can_get_record_value_by_index(self):
        record = Record(["one", "two", "three"], ["eins", "zwei", "drei"])
        assert record[0] == "eins"
        assert record[1] == "zwei"
        assert record[2] == "drei"
        assert record[-1] == "drei"

    def test_can_get_record_values_by_slice(self):
        record = Record(["one", "two", "three"], ["eins", "zwei", "drei"])
        assert record[0:2] == Record(["one", "two"], ["eins", "zwei"])
        assert record[1:2] == Record(["two"], ["zwei"])
        assert record[1:3] == Record(["two", "three"], ["zwei", "drei"])
        assert record[1:] == Record(["two", "three"], ["zwei", "drei"])

    def test_can_get_record_values_by_slice_using_getitem(self):
        record = Record(["one", "two", "three"], ["eins", "zwei", "drei"])
        assert record.__getitem__(slice(0, 2)) == Record(["one", "two"], ["eins", "zwei"])

    def test_can_get_record_values_by_slice_using_getslice(self):
        record = Record(["one", "two", "three"], ["eins", "zwei", "drei"])
        try:
            s = record.__getslice__(0, 2)
        except AttributeError:
            assert True
        else:
            assert s == Record(["one", "two"], ["eins", "zwei"])

    def test_cannot_get_record_value_by_anything_else(self):
        record = Record(["one", "two", "three"], ["eins", "zwei", "drei"])
        with self.assertRaises(TypeError):
            _ = record[None]

    def test_record_can_be_exposed_as_graph(self):
        keys = ["a", "b", "ab", "msg"]
        values = [alice, bob, alice_knows_bob, "hello, world"]
        record = Record(keys, values)
        assert len(record) == 4
        assert record.order() == 2
        assert record.size() == 1
        assert record.nodes() == {alice, bob}
        assert record.relationships() == {alice_knows_bob}
        assert list(record.keys()) == keys
        assert list(record.values()) == values
