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


from __future__ import absolute_import

from collections import deque

from pytest import mark, skip, raises

from py2neo import ClientError
from py2neo.cypher import Cursor, Record


@mark.skip
def test_bolt_connection_pool_usage_for_autocommit(connector):
    if "bolt" not in connector.scheme:
        skip("Bolt tests are only valid for Bolt connectors")
    pool = connector.pool
    address = connector.connection_data["host"], connector.connection_data["port"]
    n = len(pool.connections)
    assert pool.in_use_connection_count(address) == 0
    cursor = connector.auto_run("RETURN 1")
    assert 1 <= len(pool.connections) <= n + 1
    assert pool.in_use_connection_count(address) == 1
    n = len(pool.connections)
    cursor.summary()
    assert len(pool.connections) == n
    assert pool.in_use_connection_count(address) == 0


@mark.skip
def test_bolt_connection_reuse_for_autocommit(connector):
    if "bolt" not in connector.scheme:
        skip("Bolt tests are only valid for Bolt connectors")
    pool = connector.pool
    address = connector.connection_data["host"], connector.connection_data["port"]
    n = len(pool.connections)
    assert pool.in_use_connection_count(address) == 0
    cursor = connector.auto_run("RETURN 1")
    assert 1 <= len(pool.connections) <= n + 1
    assert pool.in_use_connection_count(address) == 1
    n = len(pool.connections)
    cursor.summary()
    assert len(pool.connections) == n
    assert pool.in_use_connection_count(address) == 0
    cursor = connector.auto_run("RETURN 1")
    assert len(pool.connections) == n
    assert pool.in_use_connection_count(address) == 1
    cursor.summary()
    assert len(pool.connections) == n
    assert pool.in_use_connection_count(address) == 0


@mark.skip
def test_bolt_connection_pool_usage_for_begin_commit(connector):
    if "bolt" not in connector.scheme:
        skip("Bolt tests are only valid for Bolt connectors")
    pool = connector.pool
    address = connector.connection_data["host"], connector.connection_data["port"]
    n = len(pool.connections)
    assert pool.in_use_connection_count(address) == 0
    tx = connector.begin()
    assert 1 <= len(pool.connections) <= n + 1
    assert pool.in_use_connection_count(address) == 1
    n = len(pool.connections)
    connector.commit(tx)
    assert len(pool.connections) == n
    assert pool.in_use_connection_count(address) == 0


@mark.skip
def test_bolt_connection_pool_usage_for_begin_rollback(connector):
    if "bolt" not in connector.scheme:
        skip("Bolt tests are only valid for Bolt connectors")
    pool = connector.pool
    address = connector.connection_data["host"], connector.connection_data["port"]
    n = len(pool.connections)
    assert pool.in_use_connection_count(address) == 0
    tx = connector.begin()
    assert 1 <= len(pool.connections) <= n + 1
    assert pool.in_use_connection_count(address) == 1
    n = len(pool.connections)
    connector.rollback(tx)
    assert len(pool.connections) == n
    assert pool.in_use_connection_count(address) == 0


def test_keys(connector):
    cursor = Cursor(connector.auto_run("RETURN 'Alice' AS name, 33 AS age"))
    expected = ["name", "age"]
    actual = cursor.keys()
    assert expected == actual


def test_records(connector):
    cursor = Cursor(
        connector.auto_run("UNWIND range(1, $x) AS n RETURN n, n * n AS n_sq", {"x": 3}))
    expected = deque([(1, 1), (2, 4), (3, 9)])
    for actual_record in cursor:
        expected_record = Record(["n", "n_sq"], expected.popleft())
        assert expected_record == actual_record


def test_stats(connector):
    cursor = Cursor(connector.auto_run("CREATE ()"))
    stats = cursor.stats()
    assert stats["nodes_created"] == 1


# def test_explain_plan(connector, neo4j_minor_version):
#     cursor = Cursor(connector.run("EXPLAIN RETURN $x", {"x": 1}))
#     expected = CypherPlan(
#         operator_type='ProduceResults',
#         identifiers=['$x'],
#         children=[
#             CypherPlan(
#                 operator_type='Projection',
#                 identifiers=['$x'],
#                 children=[],
#                 args={
#                     'estimated_rows': 1.0,
#                     'expressions': '{$x : $x}',
#                 },
#             ),
#         ],
#         args={
#             'estimated_rows': 1.0,
#             'planner': 'COST',
#             'planner_impl': 'IDP',
#             'planner_version': neo4j_minor_version,
#             'runtime': 'COMPILED',
#             'runtime_impl': 'COMPILED',
#             'runtime_version': neo4j_minor_version,
#             'version': 'CYPHER %s' % neo4j_minor_version,
#         },
#     )
#     actual = cursor.plan()
#     assert expected == actual


# def test_profile_plan(connector, neo4j_version):
#     cursor = Cursor(connector.run("PROFILE RETURN $x", {"x": 1}))
#     actual = cursor.plan()
#     expected = CypherPlan(
#         operator_type='ProduceResults',
#         identifiers=['$x'],
#         children=[
#             CypherPlan(
#                 operator_type='Projection',
#                 identifiers=['$x'],
#                 children=[],
#                 args={
#                     'db_hits': 0,
#                     'estimated_rows': 1.0,
#                     'expressions': '{$x : $x}',
#                     'page_cache_hit_ratio': 0.0,
#                     'page_cache_hits': 0,
#                     'page_cache_misses': 0,
#                     'rows': 1,
#                     'time': actual.children[0].args["time"],
#                 },
#             ),
#         ],
#         args={
#             'db_hits': 0,
#             'estimated_rows': 1.0,
#             'page_cache_hit_ratio': 0.0,
#             'page_cache_hits': 0,
#             'page_cache_misses': 0,
#             'planner': 'COST',
#             'planner_impl': 'IDP',
#             'planner_version': neo4j_version,
#             'rows': 1,
#             'runtime': 'COMPILED',
#             'runtime_impl': 'COMPILED',
#             'runtime_version': neo4j_version,
#             'time': actual.args["time"],
#             'version': 'CYPHER %s' % neo4j_version,
#         },
#     )
#     assert expected == actual


# def skip_if_no_multidb_support(graph):
#     if graph.service.kernel_version < (4, 0):
#         skip("MultiDB tests are only valid for Neo4j 4.0+")
#
#
# def test_db_extra(graph, connector):
#     skip_if_no_multidb_support(graph)
#     cursor = Cursor(connector.run("RETURN 1", {}, db="system"))
#     expected = CypherStats(nodes_created=1)
#     actual = cursor.stats()
#     assert expected == actual


def test_bad_cypher(connector):
    with raises(ClientError):
        _ = connector.auto_run("X")
