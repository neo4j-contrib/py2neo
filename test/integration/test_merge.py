#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2020, Nigel Small
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


from pytest import raises

from py2neo import Node, Relationship
from py2neo.data.operations import UniquenessError


def test_can_merge_node_that_does_not_exist(graph, make_unique_id):
    label = make_unique_id()
    alice = Node(label, name="Alice")
    old_order = len(graph.nodes)
    graph.merge(alice, label, "name")
    assert alice.graph is graph
    assert alice.identity is not None
    assert graph.exists(alice)
    new_order = len(graph.nodes)
    assert new_order == old_order + 1


def test_can_merge_node_that_does_exist(graph, make_unique_id):
    label = make_unique_id()
    graph.create(Node(label, name="Alice"))
    alice = Node(label, name="Alice")
    old_order = len(graph.nodes)
    graph.merge(alice, label, "name")
    assert alice.graph is graph
    assert alice.identity is not None
    assert graph.exists(alice)
    new_order = len(graph.nodes)
    assert new_order == old_order


def test_cannot_merge_node_where_two_exist(graph, make_unique_id):
    label = make_unique_id()
    graph.create(Node(label, name="Alice"))
    graph.create(Node(label, name="Alice"))
    alice = Node(label, name="Alice")
    with raises(UniquenessError):
        graph.merge(alice, label, "name")


def test_can_merge_bound_node(graph, make_unique_id):
    label = make_unique_id()
    alice = Node(label, name="Alice")
    graph.create(alice)
    old_order = len(graph.nodes)
    graph.merge(alice, label, "name")
    assert alice.graph is graph
    assert alice.identity is not None
    assert graph.exists(alice)
    new_order = len(graph.nodes)
    assert new_order == old_order


def test_can_merge_node_that_does_not_exist_on_specific_label_and_key(graph, make_unique_id):
    label = make_unique_id()
    label_2 = make_unique_id()
    alice = Node(label, label_2, name="Alice", age=33)
    old_order = len(graph.nodes)
    graph.merge(alice, label, "name")
    assert alice.graph is graph
    assert alice.identity is not None
    assert graph.exists(alice)
    new_order = len(graph.nodes)
    assert new_order == old_order + 1


def test_can_merge_node_that_does_exist_on_specific_label_and_key_with_extra_properties(graph, make_unique_id):
    label = make_unique_id()
    label_2 = make_unique_id()
    graph.create(Node(label, name="Alice"))
    alice = Node(label, label_2, name="Alice", age=33)
    old_order = len(graph.nodes)
    graph.merge(alice, label, "name")
    assert alice.graph is graph
    assert alice.identity is not None
    assert graph.exists(alice)
    new_order = len(graph.nodes)
    assert new_order == old_order


def test_can_merge_node_that_does_exist_on_specific_label_and_key_with_other_properties(graph, make_unique_id):
    label = make_unique_id()
    label_2 = make_unique_id()
    graph.create(Node(label, name="Alice", age=44))
    alice = Node(label, label_2, name="Alice", age=33)
    old_order = len(graph.nodes)
    graph.merge(alice, label, "name")
    assert alice.graph is graph
    assert alice.identity is not None
    assert graph.exists(alice)
    new_order = len(graph.nodes)
    assert new_order == old_order


def test_can_merge_relationship_that_does_not_exist(graph, make_unique_id):
    label = make_unique_id()
    alice = Node(label, name="Alice")
    bob = Node(label, name="Bob")
    ab = Relationship(alice, "KNOWS", bob)
    old_order = len(graph.nodes)
    old_size = len(graph.relationships)
    graph.merge(ab, label, "name")
    assert alice.graph is graph
    assert alice.identity is not None
    assert bob.graph is graph
    assert bob.identity is not None
    assert ab.graph is graph
    assert ab.identity is not None
    assert graph.exists(alice | bob | ab)
    new_order = len(graph.nodes)
    new_size = len(graph.relationships)
    assert new_order == old_order + 2
    assert new_size == old_size + 1


def test_can_merge_relationship_where_one_node_exists(graph, make_unique_id):
    label = make_unique_id()
    alice = Node(label, name="Alice")
    graph.create(alice)
    bob = Node(label, name="Bob")
    ab = Relationship(alice, "KNOWS", bob)
    old_order = len(graph.nodes)
    old_size = len(graph.relationships)
    graph.merge(ab, label, "name")
    assert alice.graph is graph
    assert alice.identity is not None
    assert bob.graph is graph
    assert bob.identity is not None
    assert ab.graph is graph
    assert ab.identity is not None
    assert graph.exists(alice | bob | ab)
    new_order = len(graph.nodes)
    new_size = len(graph.relationships)
    assert new_order == old_order + 1
    assert new_size == old_size + 1


def test_can_merge_relationship_where_all_exist(graph, make_unique_id):
    label = make_unique_id()
    alice = Node(label, name="Alice")
    graph.create(Relationship(alice, "KNOWS", Node(label, name="Bob")))
    bob = Node(label, name="Bob")
    ab = Relationship(alice, "KNOWS", bob)
    old_order = len(graph.nodes)
    old_size = len(graph.relationships)
    graph.merge(ab, label, "name")
    assert alice.graph is graph
    assert alice.identity is not None
    assert bob.graph is graph
    assert bob.identity is not None
    assert ab.graph is graph
    assert ab.identity is not None
    assert graph.exists(alice | bob | ab)
    new_order = len(graph.nodes)
    new_size = len(graph.relationships)
    assert new_order == old_order
    assert new_size == old_size


def test_can_merge_relationship_with_space_in_name(graph, make_unique_id):
    label = make_unique_id()
    alice = Node(label, name="Alice")
    bob = Node(label, name="Bob")
    ab = Relationship(alice, "MARRIED TO", bob)
    old_order = len(graph.nodes)
    old_size = len(graph.relationships)
    graph.merge(ab, label, "name")
    assert alice.graph is graph
    assert alice.identity is not None
    assert bob.graph is graph
    assert bob.identity is not None
    assert ab.graph is graph
    assert ab.identity is not None
    assert graph.exists(alice | bob | ab)
    new_order = len(graph.nodes)
    new_size = len(graph.relationships)
    assert new_order == old_order + 2
    assert new_size == old_size + 1


def test_cannot_merge_non_subgraph(graph, make_unique_id):
    with raises(TypeError):
        graph.merge("this string is definitely not a subgraph")


def test_can_merge_three_nodes_where_none_exist(graph, make_unique_id):
    label = make_unique_id()
    alice = Node(label, name="Alice")
    bob = Node(label, name="Bob")
    carol = Node(label, name="Carol")
    old_order = len(graph.nodes)
    subgraph = alice | bob | carol
    graph.merge(subgraph, label, "name")
    for node in subgraph.nodes:
        assert node.graph is graph
        assert node.identity is not None
        assert graph.exists(node)
    new_order = len(graph.nodes)
    assert new_order == old_order + 3


def test_can_merge_three_nodes_where_one_exists(graph, make_unique_id):
    label = make_unique_id()
    alice = Node(label, name="Alice")
    bob = Node(label, name="Bob")
    carol = Node(label, name="Carol")
    graph.create(alice)
    old_order = len(graph.nodes)
    subgraph = alice | bob | carol
    graph.merge(subgraph, label, "name")
    for node in subgraph.nodes:
        assert node.graph is graph
        assert node.identity is not None
        assert graph.exists(node)
    new_order = len(graph.nodes)
    assert new_order == old_order + 2


def test_can_merge_three_nodes_where_two_exist(graph, make_unique_id):
    label = make_unique_id()
    alice = Node(label, name="Alice")
    bob = Node(label, name="Bob")
    carol = Node(label, name="Carol")
    graph.create(alice | bob)
    old_order = len(graph.nodes)
    subgraph = alice | bob | carol
    graph.merge(subgraph, label, "name")
    for node in subgraph.nodes:
        assert node.graph is graph
        assert node.identity is not None
        assert graph.exists(node)
    new_order = len(graph.nodes)
    assert new_order == old_order + 1


def test_can_merge_three_nodes_where_three_exist(graph, make_unique_id):
    label = make_unique_id()
    alice = Node(label, name="Alice")
    bob = Node(label, name="Bob")
    carol = Node(label, name="Carol")
    graph.create(alice | bob | carol)
    old_order = len(graph.nodes)
    subgraph = alice | bob | carol
    graph.merge(subgraph, label, "name")
    for node in subgraph.nodes:
        assert node.graph is graph
        assert node.identity is not None
        assert graph.exists(node)
    new_order = len(graph.nodes)
    assert new_order == old_order


def test_can_merge_long_straight_walkable(graph, make_unique_id):
    label = make_unique_id()
    a = Node(label, name="Alice")
    b = Node(label, name="Bob")
    c = Node(label, name="Carol")
    d = Node(label, name="Dave")
    ab = Relationship(a, "KNOWS", b)
    cb = Relationship(c, "KNOWS", b)
    cd = Relationship(c, "KNOWS", d)
    graph.create(a)
    old_order = len(graph.nodes)
    old_size = len(graph.relationships)
    graph.merge(ab + cb + cd, label, "name")
    new_order = len(graph.nodes)
    new_size = len(graph.relationships)
    assert new_order == old_order + 3
    assert new_size == old_size + 3


def test_can_merge_long_walkable_with_repeats(graph, make_unique_id):
    label = make_unique_id()
    a = Node(label, name="Alice")
    b = Node(label, name="Bob")
    c = Node(label, name="Carol")
    d = Node(label, name="Dave")
    ab = Relationship(a, "KNOWS", b)
    cb = Relationship(c, "KNOWS", b)
    cd = Relationship(c, "KNOWS", d)
    bd = Relationship(b, "KNOWS", d)
    graph.create(a)
    old_order = len(graph.nodes)
    old_size = len(graph.relationships)
    graph.merge(ab + cb + cb + bd + cd, label, "name")
    new_order = len(graph.nodes)
    new_size = len(graph.relationships)
    assert new_order == old_order + 3
    assert new_size == old_size + 4


def test_cannot_merge_without_arguments(graph, make_unique_id):
    node = Node()
    with raises(ValueError):
        graph.merge(node)


def test_can_merge_with_arguments(graph, make_unique_id):
    label_a = make_unique_id()
    label_b = make_unique_id()
    a = Node(label_a, a=1)
    b = Node(label_b, b=2)
    graph.create(a | b)
    a_id = a.identity
    b_id = b.identity
    node = Node(label_a, label_b, a=1, b=2)
    graph.merge(node, label_a, "a")
    assert node.identity == a_id
    assert node.identity != b_id


def test_merge_with_magic_values_overrides_arguments(graph, make_unique_id):
    label_a = make_unique_id()
    label_b = make_unique_id()
    a = Node(label_a, a=1)
    b = Node(label_b, b=2)
    graph.create(a | b)
    a_id = a.identity
    b_id = b.identity
    node = Node(label_a, label_b, a=1, b=2)
    node.__primarylabel__ = label_b
    node.__primarykey__ = "b"
    graph.merge(node, label_a, "a")
    assert node.identity != a_id
    assert node.identity == b_id
