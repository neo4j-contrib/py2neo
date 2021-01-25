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


from pytest import raises

from py2neo import Node, Relationship


def test_can_create_node(graph):
    a = Node("Person", name="Alice")
    with graph.begin() as tx:
        tx.create(a)
    assert a.graph == graph
    assert a.identity is not None


def test_can_create_relationship(graph):
    a = Node("Person", name="Alice")
    b = Node("Person", name="Bob")
    r = Relationship(a, "KNOWS", b, since=1999)
    with graph.begin() as tx:
        tx.create(r)
    assert a.graph == graph
    assert a.identity is not None
    assert b.graph == graph
    assert b.identity is not None
    assert r.graph == graph
    assert r.identity is not None
    assert r.start_node == a
    assert r.end_node == b


def test_can_create_nodes_and_relationship_1(graph):
    graph.delete_all()
    with graph.begin() as tx:
        a = Node("Person", name="Alice")
        b = Node("Person", name="Bob")
        tx.create(a)
        tx.create(b)
        r = Relationship(a, "KNOWS", b, since=1999)
        tx.create(r)
    assert a.graph == graph
    assert a.identity is not None
    assert b.graph == graph
    assert b.identity is not None
    assert r.graph == graph
    assert r.identity is not None
    assert r.start_node == a
    assert r.end_node == b
    assert len(graph.nodes) == 2
    assert len(graph.relationships) == 1


def test_can_create_nodes_and_relationship_2(graph):
    graph.delete_all()
    with graph.begin() as tx:
        a = Node("Person", name="Alice")
        b = Node("Person", name="Bob")
        tx.create(a)
        tx.create(b)
        r = Relationship(a, "KNOWS", b, since=1999)
        tx.create(r)
    assert a.graph == graph
    assert a.identity is not None
    assert b.graph == graph
    assert b.identity is not None
    assert r.graph == graph
    assert r.identity is not None
    assert r.start_node == a
    assert r.end_node == b
    assert len(graph.nodes) == 2
    assert len(graph.relationships) == 1


def test_can_create_nodes_and_relationship_3(graph):
    graph.delete_all()
    with graph.begin() as tx:
        a = Node("Person", name="Alice")
        b = Node("Person", name="Bob")
        r = Relationship(a, "KNOWS", b, since=1999)
        tx.create(a)
        tx.create(b)
        tx.create(r)
    assert a.graph == graph
    assert a.identity is not None
    assert b.graph == graph
    assert b.identity is not None
    assert r.graph == graph
    assert r.identity is not None
    assert r.start_node == a
    assert r.end_node == b
    assert len(graph.nodes) == 2
    assert len(graph.relationships) == 1


def test_can_create_nodes_and_relationship_4(graph):
    graph.delete_all()
    with graph.begin() as tx:
        a = Node()
        b = Node()
        c = Node()
        ab = Relationship(a, "TO", b)
        bc = Relationship(b, "TO", c)
        ca = Relationship(c, "TO", a)
        tx.create(ab | bc | ca)
    assert a.graph == graph
    assert a.identity is not None
    assert b.graph == graph
    assert b.identity is not None
    assert c.graph == graph
    assert c.identity is not None
    assert ab.graph == graph
    assert ab.identity is not None
    assert ab.start_node == a
    assert ab.end_node == b
    assert bc.graph == graph
    assert bc.identity is not None
    assert bc.start_node == b
    assert bc.end_node == c
    assert ca.graph == graph
    assert ca.identity is not None
    assert ca.start_node == c
    assert ca.end_node == a
    assert len(graph.nodes) == 3
    assert len(graph.relationships) == 3


def test_create_is_idempotent(graph):
    graph.delete_all()
    a = Node()
    b = Node()
    r = Relationship(a, "TO", b)
    with graph.begin() as tx:
        tx.create(r)
    assert a.graph == graph
    assert a.identity is not None
    assert b.graph == graph
    assert b.identity is not None
    assert r.graph == graph
    assert r.identity is not None
    assert len(graph.nodes) == 2
    assert len(graph.relationships) == 1
    with graph.begin() as tx:
        tx.create(r)
    assert a.graph == graph
    assert a.identity is not None
    assert b.graph == graph
    assert b.identity is not None
    assert r.graph == graph
    assert r.identity is not None
    assert len(graph.nodes) == 2
    assert len(graph.relationships) == 1


def test_cannot_create_non_graphy_thing(graph):
    with raises(TypeError):
        graph.create("this string is definitely not graphy")
