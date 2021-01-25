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


from pytest import skip


def test_simple_evaluation(graph):
    value = graph.evaluate("RETURN 1")
    assert value == 1


def test_simple_evaluation_with_parameters(graph):
    value = graph.evaluate("RETURN $x", x=1)
    assert value == 1


def test_run_and_consume_multiple_records(graph):
    cursor = graph.run("UNWIND range(1, 3) AS n RETURN n")
    record = next(cursor)
    assert record[0] == 1
    record = next(cursor)
    assert record[0] == 2
    record = next(cursor)
    assert record[0] == 3


def test_can_run_cypher_while_in_transaction(graph):
    tx = graph.begin()
    outer_result = tx.run("UNWIND range(1, 10) AS n RETURN n")
    inner_result = graph.run("CREATE (a) RETURN a")
    outer_result_list = list(map(tuple, outer_result))
    tx.rollback()
    record = next(inner_result)
    created = record[0]
    assert outer_result_list == [(1,), (2,), (3,), (4,), (5,), (6,), (7,), (8,), (9,), (10,)]
    assert graph.exists(created)


def test_can_list_procedure_directory(graph):
    assert "dbms.components" in dir(graph.call)


def test_can_list_procedure_subdirectory(graph):
    assert "components" in dir(graph.call.dbms)


def test_can_call_procedure_by_attribute(graph):
    data = graph.call.dbms.components().data()
    assert data[0]["name"] == "Neo4j Kernel"


def test_can_call_procedure_by_item(graph):
    data = graph.call["dbms"]["components"]().data()
    assert data[0]["name"] == "Neo4j Kernel"


def test_can_call_procedure_by_name(graph):
    data = graph.call("dbms.components").data()
    assert data[0]["name"] == "Neo4j Kernel"


def test_readonly_query(graph):
    if not graph.service.connector.supports_readonly_transactions():
        skip("The underlying connection profile "
             "does not support readonly transactions")
    data = graph.read("RETURN 1 AS x").data()
    assert data[0]["x"] == 1
