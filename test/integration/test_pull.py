#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
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


def test_cannot_pull_non_graphy_object(graph):
    with raises(TypeError):
        graph.pull("this is not a graphy object")


def test_can_graph_pull_node(graph):
    alice_1 = Node()
    alice_2 = Node("Person", name="Alice")
    graph.create(alice_2)
    assert set(alice_1.labels) == set()
    assert dict(alice_1) == {}
    alice_1.graph = alice_2.graph
    alice_1.identity = alice_2.identity
    graph.pull(alice_1)
    assert set(alice_1.labels) == set(alice_2.labels)
    assert dict(alice_1) == dict(alice_2)


def test_can_pull_path(graph):
    alice = Node(name="Alice")
    bob = Node(name="Bob")
    carol = Node(name="Carol")
    dave = Node(name="Dave")
    path = Path(alice, "LOVES", bob, Relationship(carol, "HATES", bob), carol, "KNOWS", dave)
    graph.create(path)
    assert path[0]["amount"] is None
    assert path[1]["amount"] is None
    assert path[2]["since"] is None
    statement = ("MATCH ()-[ab]->() WHERE id(ab)=$ab "
                 "MATCH ()-[bc]->() WHERE id(bc)=$bc "
                 "MATCH ()-[cd]->() WHERE id(cd)=$cd "
                 "SET ab.amount = 'lots', bc.amount = 'some', cd.since = 1999")
    id_0 = path[0].identity
    id_1 = path[1].identity
    id_2 = path[2].identity
    parameters = {"ab": id_0, "bc": id_1, "cd": id_2}
    graph.run(statement, parameters)
    graph.pull(path)
    assert path[0]["amount"] == "lots"
    assert path[1]["amount"] == "some"
    assert path[2]["since"] == 1999


def test_node_label_pull_scenarios(graph):
    label_sets = [set(), {"Foo"}, {"Foo", "Bar"}, {"Spam"}]
    for old_labels in label_sets:
        for new_labels in label_sets:
            node = Node(*old_labels)
            graph.create(node)
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
                graph.run("MATCH (a) WHERE id(a)=$x %s %s" %
                          (remove_clause, set_clause), x=node_id)
                graph.pull(node)
                assert set(node.labels) == new_labels, \
                    "Failed to pull new labels %r over old labels %r" % \
                    (new_labels, old_labels)


def test_node_property_pull_scenarios(graph):
    property_sets = [{}, {"name": "Alice"}, {"name": "Alice", "age": 33}, {"name": "Bob"}]
    for old_props in property_sets:
        for new_props in property_sets:
            node = Node(**old_props)
            graph.create(node)
            node_id = node.identity
            assert dict(node) == old_props
            graph.run("MATCH (a) WHERE id(a)=$x SET a=$y", x=node_id, y=new_props)
            graph.pull(node)
            assert dict(node) == new_props,\
                "Failed to pull new properties %r over old properties %r" % \
                (new_props, old_props)


def test_relationship_property_pull_scenarios(graph):
    property_sets = [{}, {"name": "Alice"}, {"name": "Alice", "age": 33}, {"name": "Bob"}]
    for old_props in property_sets:
        for new_props in property_sets:
            a = Node()
            b = Node()
            relationship = Relationship(a, "TO", b, **old_props)
            graph.create(relationship)
            relationship_id = relationship.identity
            assert dict(relationship) == old_props
            graph.run("MATCH ()-[r]->() WHERE id(r)=$x SET r=$y",
                      x=relationship_id, y=new_props)
            graph.pull(relationship)
            assert dict(relationship) == new_props, \
                "Failed to pull new properties %r over old properties %r" % \
                (new_props, old_props)
