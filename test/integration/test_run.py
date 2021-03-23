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

from py2neo import Node


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
