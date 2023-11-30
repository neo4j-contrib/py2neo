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


from pytest import raises, skip, mark

from py2neo import Graph, Node, Relationship


def test_graph_len_returns_number_of_rels(graph):
    size = len(graph)
    statement = "MATCH ()-[r]->() RETURN COUNT(r)"
    num_rels = graph.evaluate(statement)
    assert size == num_rels


def test_graph_bool_returns_true(graph):
    assert graph.__bool__()
    assert graph.__nonzero__()


def test_graph_contains(graph):
    node = Node()
    graph.create(node)
    assert node.graph is graph


def test_graph_is_not_equal_to_non_graph(graph):
    assert graph != object()


def test_can_create_and_delete_node(graph):
    a = Node()
    graph.create(a)
    assert isinstance(a, Node)
    assert a.graph == graph
    assert a.identity is not None
    assert graph.exists(a)
    graph.delete(a)
    assert not graph.exists(a)


def test_can_create_and_delete_relationship(graph):
    ab = Relationship(Node(), "KNOWS", Node())
    graph.create(ab)
    assert isinstance(ab, Relationship)
    assert ab.graph == graph
    assert ab.identity is not None
    assert graph.exists(ab)
    graph.delete(ab | ab.start_node | ab.end_node)
    assert not graph.exists(ab)


def test_can_get_node_by_id(graph):
    node = Node()
    graph.create(node)
    got = graph.nodes.get(node.identity)
    assert got == node


def test_get_non_existent_node_by_id(graph):
    node = Node()
    graph.create(node)
    node_id = node.identity
    graph.delete(node)
    with raises(KeyError):
        _ = graph.nodes[node_id]
    assert graph.nodes.get(node_id) is None


def test_create_single_empty_node(graph):
    a = Node()
    graph.create(a)
    assert a.graph == graph
    assert a.identity is not None


def test_get_node_by_id(graph):
    a1 = Node(foo="bar")
    graph.create(a1)
    a2 = graph.nodes.get(a1.identity)
    assert a1 == a2


def test_create_node_with_mixed_property_types(graph):
    a = Node(number=13, foo="bar", true=False, fish="chips")
    graph.create(a)
    assert len(a) == 4
    assert a["fish"] == "chips"
    assert a["foo"] == "bar"
    assert a["number"] == 13
    assert not a["true"]


def test_create_node_with_null_properties(graph):
    a = Node(foo="bar", no_foo=None)
    graph.create(a)
    assert a["foo"] == "bar"
    assert a["no_foo"] is None


@mark.skip
def test_bolt_connection_pool_usage_for_autocommit(graph):
    connector = graph.service.connector
    if "bolt" not in connector.scheme:
        skip("Bolt tests are only valid for Bolt connectors")
    pool = connector.pool
    address = connector.connection_data["host"], connector.connection_data["port"]
    n = len(pool.connections)
    assert pool.in_use_connection_count(address) == 0
    cursor = graph.run("RETURN 1")
    assert 1 <= len(pool.connections) <= n + 1
    assert pool.in_use_connection_count(address) in {0, 1}
    n = len(pool.connections)
    cursor.summary()
    assert len(pool.connections) == n
    assert pool.in_use_connection_count(address) == 0


@mark.skip
def test_bolt_connection_reuse_for_autocommit(graph):
    connector = graph.service.connector
    if "bolt" not in connector.scheme:
        skip("Bolt tests are only valid for Bolt connectors")
    pool = connector.pool
    address = connector.connection_data["host"], connector.connection_data["port"]
    n = len(pool.connections)
    assert pool.in_use_connection_count(address) == 0
    cursor = graph.run("RETURN 1")
    assert 1 <= len(pool.connections) <= n + 1
    assert pool.in_use_connection_count(address) in {0, 1}
    n = len(pool.connections)
    cursor.summary()
    assert len(pool.connections) == n
    assert pool.in_use_connection_count(address) == 0
    cursor = graph.run("RETURN 1")
    assert len(pool.connections) == n
    assert pool.in_use_connection_count(address) in {0, 1}
    cursor.summary()
    assert len(pool.connections) == n
    assert pool.in_use_connection_count(address) == 0


@mark.skip
def test_bolt_connection_pool_usage_for_begin_commit(graph):
    connector = graph.service.connector
    if "bolt" not in connector.scheme:
        skip("Bolt tests are only valid for Bolt connectors")
    pool = connector.pool
    address = connector.connection_data["host"], connector.connection_data["port"]
    n = len(pool.connections)
    assert pool.in_use_connection_count(address) == 0
    tx = graph.begin()
    assert 1 <= len(pool.connections[address]) <= n + 1
    assert pool.in_use_connection_count(address) == 1
    n = len(pool.connections)
    tx.commit()
    assert len(pool.connections) == n
    assert pool.in_use_connection_count(address) == 0


@mark.skip
def test_bolt_connection_pool_usage_for_begin_rollback(graph):
    connector = graph.service.connector
    if "bolt" not in connector.scheme:
        skip("Bolt tests are only valid for Bolt connectors")
    pool = connector.pool
    address = connector.connection_data["host"], connector.connection_data["port"]
    n = len(pool.connections)
    assert pool.in_use_connection_count(address) == 0
    tx = graph.begin()
    assert 1 <= len(pool.connections) <= n + 1
    assert pool.in_use_connection_count(address) == 1
    n = len(pool.connections)
    tx.rollback()
    assert len(pool.connections) == n
    assert pool.in_use_connection_count(address) == 0
