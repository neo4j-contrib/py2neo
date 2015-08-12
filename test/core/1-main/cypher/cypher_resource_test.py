#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2014, Nigel Small
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


from __future__ import unicode_literals

from py2neo.core import Node
from py2neo.cypher.core import CypherResource, CypherTransaction
from py2neo.cypher.util import StartOrMatch


def test_can_create_cypher_resource_with_transaction_uri():
    uri = "http://localhost:7474/db/data/transaction"
    cypher = CypherResource(uri)
    assert cypher.uri == uri


def test_cypher_resources_with_identical_arguments_are_same_objects():
    uri = "http://localhost:7474/db/data/transaction"
    cypher_1 = CypherResource(uri)
    cypher_2 = CypherResource(uri)
    assert cypher_1 is cypher_2


def test_can_execute_cypher_statement(graph):
    results = graph.cypher.execute("MERGE (a:Person {name:'Alice'}) RETURN a")
    result = results[0].a
    assert isinstance(result, Node)
    assert result.labels == {"Person"}
    assert result.properties == {"name": "Alice"}


def test_can_execute_parametrised_cypher_statement(graph):
    results = graph.cypher.execute("MERGE (a:Person {name:{N}}) RETURN a", {"N": "Alice"})
    result = results[0].a
    assert isinstance(result, Node)
    assert result.labels == {"Person"}
    assert result.properties == {"name": "Alice"}


def test_can_execute_cypher_statement_with_node_parameter(graph):
    alice = Node(name="Alice")
    graph.create(alice)
    statement = "MATCH (a) WHERE id(a)={N} RETURN a"
    results = graph.cypher.execute(statement, {"N": alice})
    result = results[0].a
    assert result is alice


def test_can_execute_one_cypher_statement(graph):
    result = graph.cypher.execute_one("MERGE (a:Person {name:'Alice'}) RETURN a")
    assert isinstance(result, Node)
    assert result.labels == {"Person"}
    assert result.properties == {"name": "Alice"}


def test_can_execute_one_parametrised_cypher_statement(graph):
    result = graph.cypher.execute_one("MERGE (a:Person {name:{N}}) RETURN a", {"N": "Alice"})
    assert isinstance(result, Node)
    assert result.labels == {"Person"}
    assert result.properties == {"name": "Alice"}


def test_execute_one_with_no_results_returns_none(graph):
    result = graph.cypher.execute_one("CREATE (a {name:{N}})", {"N": "Alice"})
    assert result is None


def test_can_begin_transaction():
    uri = "http://localhost:7474/db/data/transaction"
    cypher = CypherResource(uri)
    tx = cypher.begin()
    assert isinstance(tx, CypherTransaction)
