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


from pytest import mark, raises

from py2neo import Node


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
    graph.rollback(tx)
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
    data = graph.query("RETURN 1 AS x").data()
    assert data[0]["x"] == 1


def test_can_run_single_statement_transaction(graph):
    tx = graph.begin()
    cursor = tx.run("CREATE (a) RETURN a")
    graph.commit(tx)
    records = list(cursor)
    assert len(records) == 1
    for record in records:
        assert isinstance(record["a"], Node)


def test_can_run_query_that_returns_map_literal(graph):
    tx = graph.begin()
    cursor = tx.run("RETURN {foo:'bar'}")
    graph.commit(tx)
    value = cursor.evaluate()
    assert value == {"foo": "bar"}


def test_can_run_multi_statement_transaction(graph):
    tx = graph.begin()
    cursor_1 = tx.run("CREATE (a) RETURN a")
    cursor_2 = tx.run("CREATE (a) RETURN a")
    cursor_3 = tx.run("CREATE (a) RETURN a")
    graph.commit(tx)
    for cursor in (cursor_1, cursor_2, cursor_3):
        records = list(cursor)
        assert len(records) == 1
        for record in records:
            assert isinstance(record["a"], Node)


def test_can_run_multi_execute_transaction(graph):
    tx = graph.begin()
    for i in range(10):
        cursor_1 = tx.run("CREATE (a) RETURN a")
        cursor_2 = tx.run("CREATE (a) RETURN a")
        cursor_3 = tx.run("CREATE (a) RETURN a")
        for cursor in (cursor_1, cursor_2, cursor_3):
            records = list(cursor)
            assert len(records) == 1
            for record in records:
                assert isinstance(record["a"], Node)
    graph.commit(tx)


def test_can_rollback_transaction(graph):
    tx = graph.begin()
    for i in range(10):
        cursor_1 = tx.run("CREATE (a) RETURN a")
        cursor_2 = tx.run("CREATE (a) RETURN a")
        cursor_3 = tx.run("CREATE (a) RETURN a")
        # tx.process()
        for cursor in (cursor_1, cursor_2, cursor_3):
            records = list(cursor)
            assert len(records) == 1
            for record in records:
                assert isinstance(record["a"], Node)
    graph.rollback(tx)


def test_cannot_append_after_transaction_finished(graph):
    tx = graph.begin()
    graph.rollback(tx)
    with raises(TypeError):
        tx.run("CREATE (a) RETURN a")


def test_update_with_simple_function(graph):
    collected = []

    def work(tx):
        collected.append(tx.evaluate("RETURN 1"))

    graph.update(work)
    assert collected == [1]


def test_update_with_generator_function(graph):
    collected = []

    def work(tx):
        collected.append(tx.evaluate("RETURN 1"))
        yield

    graph.update(work)
    assert collected == [1]


def test_update_with_function_and_args_and_kwargs(graph):
    collected = []

    def work(tx, x, y):
        collected.append(tx.evaluate("RETURN $x * $y", x=x, y=y))

    graph.update(work, ([2], {"y": 3}))
    assert collected == [6]


def test_update_with_function_and_args(graph):
    collected = []

    def work(tx, x):
        collected.append(tx.evaluate("RETURN $x", x=x))

    graph.update(work, [1])
    assert collected == [1]


def test_update_with_function_and_kwargs(graph):
    collected = []

    def work(tx, x):
        collected.append(tx.evaluate("RETURN $x", x=x))

    graph.update(work, {"x": 1})
    assert collected == [1]


def test_update_with_bad_parameter_type(graph):
    with raises(TypeError):
        graph.update(lambda tx: None, object())


@mark.skip("Chaining functionality not yet implemented")
def test_update_with_chained_functions(graph):
    collected = []

    def work(tx, x):
        collected.append(tx.evaluate("RETURN $x", x=x))

    first = graph.update(work, {"x": 1})
    if graph.service.connector.profile.protocol == "bolt":
        graph.update(work, {"x": 2}, after=first)
        assert collected == [1, 2]
    else:
        with raises(TypeError):
            graph.update(work, {"x": 2}, after=first)
        assert collected == [1]


@mark.skip("Chaining functionality not yet implemented")
def test_update_with_multi_chained_functions(graph):
    collected = []

    def work(tx, x):
        collected.append(tx.evaluate("RETURN $x", x=x))

    first = graph.update(work, {"x": 1})
    second = graph.update(work, {"x": 2})
    if graph.service.connector.profile.protocol == "bolt":
        graph.update(work, {"x": 3}, after=(first, second))
        assert collected == [1, 2, 3]
    else:
        with raises(TypeError):
            graph.update(work, {"x": 3}, after=(first, second))
        assert collected == [1, 2]


def test_update_with_non_callable(graph):
    with raises(TypeError):
        graph.update(object())
