#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2019, Nigel Small
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

from py2neo import Node, Relationship, Path


def test_cannot_push_non_graphy_object(graph):
    with raises(TypeError):
        graph.push("this is not a graphy object")


def test_can_graph_push_node(graph):
    alice_1 = Node("Person", name="Alice")
    alice_2 = Node()
    graph.create(alice_2)
    assert set(alice_2.labels) == set()
    assert dict(alice_2) == {}
    alice_1.graph = alice_2.graph
    alice_1.identity = alice_2.identity
    graph.push(alice_1)
    graph.pull(alice_2)
    assert set(alice_1.labels) == set(alice_2.labels)
    assert dict(alice_1) == dict(alice_2)


def test_can_push_relationship(graph):
    a = Node()
    b = Node()
    ab = Relationship(a, "KNOWS", b)
    graph.create(ab)
    value = graph.evaluate("MATCH ()-[ab:KNOWS]->() WHERE id(ab)=$i "
                           "RETURN ab.since", i=ab.identity)
    assert value is None
    ab["since"] = 1999
    graph.push(ab)
    value = graph.evaluate("MATCH ()-[ab:KNOWS]->() WHERE id(ab)=$i "
                           "RETURN ab.since", i=ab.identity)
    assert value == 1999


def test_can_push_path(graph):
    alice = Node(name="Alice")
    bob = Node(name="Bob")
    carol = Node(name="Carol")
    dave = Node(name="Dave")
    path = Path(alice, "LOVES", bob, Relationship(carol, "HATES", bob), carol, "KNOWS", dave)
    graph.create(path)
    statement = ("MATCH ()-[ab]->() WHERE id(ab)=$ab "
                 "MATCH ()-[bc]->() WHERE id(bc)=$bc "
                 "MATCH ()-[cd]->() WHERE id(cd)=$cd "
                 "RETURN ab.amount, bc.amount, cd.since")
    parameters = {"ab": path[0].identity, "bc": path[1].identity, "cd": path[2].identity}
    path[0]["amount"] = "lots"
    path[1]["amount"] = "some"
    path[2]["since"] = 1999
    ab_amount, bc_amount, cd_since = next(graph.run(statement, parameters))
    assert ab_amount is None
    assert bc_amount is None
    assert cd_since is None
    graph.push(path)
    ab_amount, bc_amount, cd_since = next(graph.run(statement, parameters))
    assert ab_amount == "lots"
    assert bc_amount == "some"
    assert cd_since == 1999


def assert_has_labels(graph, node_id, expected):
    actual = graph.evaluate("MATCH (_) WHERE id(_) = $x return labels(_)", x=node_id)
    assert set(actual) == set(expected)


def test_should_push_no_labels_onto_no_labels(graph):
    node = Node()
    graph.create(node)
    node_id = node.identity
    assert_has_labels(graph, node_id, {})
    graph.push(node)
    assert_has_labels(graph, node_id, {})


def test_should_push_no_labels_onto_one_label(graph):
    node = Node("A")
    graph.create(node)
    node_id = node.identity
    assert_has_labels(graph, node_id, {"A"})
    node.clear_labels()
    graph.push(node)
    assert_has_labels(graph, node_id, {})


def test_should_push_one_label_onto_no_labels(graph):
    node = Node()
    graph.create(node)
    node_id = node.identity
    assert_has_labels(graph, node_id, {})
    node.add_label("A")
    graph.push(node)
    assert_has_labels(graph, node_id, {"A"})


def test_should_push_one_label_onto_same_label(graph):
    node = Node("A")
    graph.create(node)
    node_id = node.identity
    assert_has_labels(graph, node_id, {"A"})
    graph.push(node)
    assert_has_labels(graph, node_id, {"A"})


def test_should_push_one_additional_label(graph):
    node = Node("A")
    graph.create(node)
    node_id = node.identity
    assert_has_labels(graph, node_id, {"A"})
    node.add_label("B")
    graph.push(node)
    assert_has_labels(graph, node_id, {"A", "B"})


def test_should_push_one_label_onto_different_label(graph):
    node = Node("A")
    graph.create(node)
    node_id = node.identity
    assert_has_labels(graph, node_id, {"A"})
    node.clear_labels()
    node.add_label("B")
    graph.push(node)
    assert_has_labels(graph, node_id, {"B"})


def test_should_push_multiple_labels_with_overlap(graph):
    node = Node("A", "B")
    graph.create(node)
    node_id = node.identity
    assert_has_labels(graph, node_id, {"A", "B"})
    node.remove_label("A")
    node.add_label("C")
    graph.push(node)
    assert_has_labels(graph, node_id, {"B", "C"})
