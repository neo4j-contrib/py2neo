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


from pytest import fixture, raises

from py2neo import Node, Relationship


@fixture
def new_graph(graph):
    graph.run("MATCH (n) DETACH DELETE n")
    return graph


def test_can_create_node(graph):
    a = Node("Person", name="Alice")
    graph.play(lambda tx: tx.create(a))
    assert a.graph == graph
    assert a.identity is not None


def test_can_create_relationship(graph):
    a = Node("Person", name="Alice")
    b = Node("Person", name="Bob")
    r = Relationship(a, "KNOWS", b, since=1999)
    graph.play(lambda tx: tx.create(r))
    assert a.graph == graph
    assert a.identity is not None
    assert b.graph == graph
    assert b.identity is not None
    assert r.graph == graph
    assert r.identity is not None
    assert r.start_node == a
    assert r.end_node == b


def test_can_create_nodes_and_relationship_1(new_graph):
    tx = new_graph.begin()
    a = Node("Person", name="Alice")
    b = Node("Person", name="Bob")
    tx.create(a)
    tx.create(b)
    r = Relationship(a, "KNOWS", b, since=1999)
    tx.create(r)
    new_graph.commit(tx)
    assert a.graph == new_graph
    assert a.identity is not None
    assert b.graph == new_graph
    assert b.identity is not None
    assert r.graph == new_graph
    assert r.identity is not None
    assert r.start_node == a
    assert r.end_node == b
    assert len(new_graph.nodes) == 2
    assert len(new_graph.relationships) == 1


def test_can_create_nodes_and_relationship_2(new_graph):
    tx = new_graph.begin()
    a = Node("Person", name="Alice")
    b = Node("Person", name="Bob")
    tx.create(a)
    tx.create(b)
    r = Relationship(a, "KNOWS", b, since=1999)
    tx.create(r)
    new_graph.commit(tx)
    assert a.graph == new_graph
    assert a.identity is not None
    assert b.graph == new_graph
    assert b.identity is not None
    assert r.graph == new_graph
    assert r.identity is not None
    assert r.start_node == a
    assert r.end_node == b
    assert len(new_graph.nodes) == 2
    assert len(new_graph.relationships) == 1


def test_can_create_nodes_and_relationship_3(new_graph):
    tx =  new_graph.begin()
    a = Node("Person", name="Alice")
    b = Node("Person", name="Bob")
    r = Relationship(a, "KNOWS", b, since=1999)
    tx.create(a)
    tx.create(b)
    tx.create(r)
    new_graph.commit(tx)
    assert a.graph == new_graph
    assert a.identity is not None
    assert b.graph == new_graph
    assert b.identity is not None
    assert r.graph == new_graph
    assert r.identity is not None
    assert r.start_node == a
    assert r.end_node == b
    assert len(new_graph.nodes) == 2
    assert len(new_graph.relationships) == 1


def test_can_create_nodes_and_relationship_4(new_graph):
    tx = new_graph.begin()
    a = Node()
    b = Node()
    c = Node()
    ab = Relationship(a, "TO", b)
    bc = Relationship(b, "TO", c)
    ca = Relationship(c, "TO", a)
    tx.create(ab | bc | ca)
    new_graph.commit(tx)
    assert a.graph == new_graph
    assert a.identity is not None
    assert b.graph == new_graph
    assert b.identity is not None
    assert c.graph == new_graph
    assert c.identity is not None
    assert ab.graph == new_graph
    assert ab.identity is not None
    assert ab.start_node == a
    assert ab.end_node == b
    assert bc.graph == new_graph
    assert bc.identity is not None
    assert bc.start_node == b
    assert bc.end_node == c
    assert ca.graph == new_graph
    assert ca.identity is not None
    assert ca.start_node == c
    assert ca.end_node == a
    assert len(new_graph.nodes) == 3
    assert len(new_graph.relationships) == 3


def test_create_is_idempotent(new_graph):
    a = Node()
    b = Node()
    r = Relationship(a, "TO", b)
    new_graph.play(lambda tx: tx.create(r))
    assert a.graph == new_graph
    assert a.identity is not None
    assert b.graph == new_graph
    assert b.identity is not None
    assert r.graph == new_graph
    assert r.identity is not None
    assert len(new_graph.nodes) == 2
    assert len(new_graph.relationships) == 1
    new_graph.play(lambda tx: tx.create(r))
    assert a.graph == new_graph
    assert a.identity is not None
    assert b.graph == new_graph
    assert b.identity is not None
    assert r.graph == new_graph
    assert r.identity is not None
    assert len(new_graph.nodes) == 2
    assert len(new_graph.relationships) == 1


def test_cannot_create_non_graphy_thing(graph):
    with raises(TypeError):
        graph.create("this string is definitely not graphy")
