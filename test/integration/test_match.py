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


from pytest import fixture, raises

from py2neo import Node, Relationship


KNOWS = Relationship.type("KNOWS")
LOVES = Relationship.type("LOVES")


@fixture()
def friends(graph):
    graph.delete_all()
    alice = Node(name="Alice")
    bob = Node(name="Bob")
    carol = Node(name="Carol")
    s = (Relationship(alice, "LOVES", bob) |
         Relationship(bob, "LOVES", alice) |
         Relationship(alice, "KNOWS", bob) |
         Relationship(bob, "KNOWS", alice) |
         Relationship(bob, "KNOWS", carol) |
         Relationship(carol, "KNOWS", bob))
    graph.create(s)
    return s


def test_can_match_start_node(friends):
    nodes = {node["name"]: node for node in friends.nodes}
    relationships = list(friends.graph.match(nodes=(nodes["Alice"], None)))
    assert len(relationships) == 2
    assert "KNOWS" in [type(rel).__name__ for rel in relationships]
    assert "LOVES" in [type(rel).__name__ for rel in relationships]
    assert nodes["Bob"] in [rel.end_node for rel in relationships]


def test_can_match_start_node_and_type(friends):
    nodes = {node["name"]: node for node in friends.nodes}
    relationships = list(friends.graph.match(nodes=(nodes["Alice"], None), r_type="KNOWS"))
    assert len(relationships) == 1
    assert nodes["Bob"] in [rel.end_node for rel in relationships]


def test_can_match_start_node_and_end_node(friends):
    nodes = {node["name"]: node for node in friends.nodes}
    relationships = list(friends.graph.match(nodes=(nodes["Alice"], nodes["Bob"])))
    assert len(relationships) == 2
    assert "KNOWS" in [type(rel).__name__ for rel in relationships]
    assert "LOVES" in [type(rel).__name__ for rel in relationships]


def test_can_match_type_and_end_node(friends):
    nodes = {node["name"]: node for node in friends.nodes}
    relationships = list(friends.graph.match(nodes=(None, nodes["Bob"]), r_type="KNOWS"))
    assert len(relationships) == 2
    assert nodes["Alice"] in [rel.start_node for rel in relationships]
    assert nodes["Carol"] in [rel.start_node for rel in relationships]


def test_can_bidi_match_start_node(friends):
    nodes = {node["name"]: node for node in friends.nodes}
    relationships = list(friends.graph.match(nodes={nodes["Bob"]}))
    assert len(relationships) == 6
    assert "KNOWS" in [type(rel).__name__ for rel in relationships]
    assert "LOVES" in [type(rel).__name__ for rel in relationships]
    assert nodes["Alice"] in [rel.start_node for rel in relationships]
    assert nodes["Bob"] in [rel.start_node for rel in relationships]
    assert nodes["Carol"] in [rel.start_node for rel in relationships]
    assert nodes["Alice"] in [rel.end_node for rel in relationships]
    assert nodes["Bob"] in [rel.end_node for rel in relationships]
    assert nodes["Carol"] in [rel.end_node for rel in relationships]


def test_can_bidi_match_start_node_and_type(friends):
    nodes = {node["name"]: node for node in friends.nodes}
    relationships = list(friends.graph.match(nodes={nodes["Bob"]}, r_type="KNOWS"))
    assert len(relationships) == 4
    assert nodes["Alice"] in [rel.start_node for rel in relationships]
    assert nodes["Bob"] in [rel.start_node for rel in relationships]
    assert nodes["Carol"] in [rel.start_node for rel in relationships]
    assert nodes["Alice"] in [rel.end_node for rel in relationships]
    assert nodes["Bob"] in [rel.end_node for rel in relationships]
    assert nodes["Carol"] in [rel.end_node for rel in relationships]


def test_can_bidi_match_start_node_and_end_node(friends):
    nodes = {node["name"]: node for node in friends.nodes}
    relationships = list(friends.graph.match(nodes={nodes["Alice"], nodes["Bob"]}))
    assert len(relationships) == 4
    assert "KNOWS" in [type(rel).__name__ for rel in relationships]
    assert "LOVES" in [type(rel).__name__ for rel in relationships]
    assert nodes["Alice"] in [rel.start_node for rel in relationships]
    assert nodes["Bob"] in [rel.start_node for rel in relationships]
    assert nodes["Alice"] in [rel.end_node for rel in relationships]
    assert nodes["Bob"] in [rel.end_node for rel in relationships]


def test_can_bidi_match_type_and_end_node(friends):
    nodes = {node["name"]: node for node in friends.nodes}
    relationships = list(friends.graph.match(nodes={nodes["Bob"]}, r_type="KNOWS"))
    assert len(relationships) == 4
    assert nodes["Alice"] in [rel.start_node for rel in relationships]
    assert nodes["Carol"] in [rel.start_node for rel in relationships]
    assert nodes["Alice"] in [rel.start_node for rel in relationships]
    assert nodes["Bob"] in [rel.start_node for rel in relationships]
    assert nodes["Carol"] in [rel.start_node for rel in relationships]
    assert nodes["Alice"] in [rel.end_node for rel in relationships]
    assert nodes["Bob"] in [rel.end_node for rel in relationships]
    assert nodes["Carol"] in [rel.end_node for rel in relationships]


def test_can_match_with_limit(friends):
    relationships = list(friends.graph.match(limit=3))
    assert len(relationships) == 3


def test_can_match_one_when_some_exist(friends):
    rel = friends.graph.match_one()
    assert isinstance(rel, Relationship)


def test_can_match_one_when_none_exist(friends, make_unique_id):
    unique_id = make_unique_id()
    rel = friends.graph.match_one(r_type=unique_id)
    assert rel is None


def test_can_match_none(friends):
    relationships = list(friends.graph.match(r_type="X", limit=1))
    assert len(relationships) == 0


def test_can_match_start_node_and_multiple_types(friends):
    nodes = {node["name"]: node for node in friends.nodes}
    relationships = list(friends.graph.match(nodes=(nodes["Alice"], None), r_type=("LOVES", "KNOWS")))
    assert len(relationships) == 2


def test_relationship_start_node_must_be_bound(friends):
    with raises(ValueError):
        list(friends.graph.match(nodes=(Node(), None)))


def test_relationship_end_node_must_be_bound(friends):
    with raises(ValueError):
        list(friends.graph.match(nodes=(None, Node())))


def test_relationship_start_and_end_node_must_be_bound(friends):
    with raises(ValueError):
        list(friends.graph.match(nodes=(Node(), Node())))


def test_can_match_node_tuple(friends):
    nodes = {node["name"]: node for node in friends.nodes}
    relationships = list(friends.graph.match(nodes=(nodes["Alice"], nodes["Bob"])))
    assert len(relationships) == 2


def test_can_match_node_list(friends):
    nodes = {node["name"]: node for node in friends.nodes}
    relationships = list(friends.graph.match(nodes=[nodes["Alice"], nodes["Bob"]]))
    assert len(relationships) == 2


def test_can_match_node_set(friends):
    nodes = {node["name"]: node for node in friends.nodes}
    relationships = list(friends.graph.match(nodes={nodes["Alice"], nodes["Bob"]}))
    assert len(relationships) == 4


def test_can_match_by_relationship_type_object(friends):
    nodes = {node["name"]: node for node in friends.nodes}
    relationships = list(friends.graph.match(nodes=[nodes["Alice"]], r_type=(LOVES, KNOWS)))
    assert len(relationships) == 2


def test_can_count_relationship_matches(friends):
    nodes = {node["name"]: node for node in friends.nodes}
    assert len(friends.graph.match(nodes=[nodes["Alice"]], r_type=(LOVES, KNOWS))) == 2
