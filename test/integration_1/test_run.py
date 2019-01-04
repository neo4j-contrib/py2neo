#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2019, Nigel Small
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


from py2neo import Node, TransactionError


def test_can_run_single_statement_transaction(graph):
    tx = graph.begin()
    assert not tx.finished()
    cursor = tx.run("CREATE (a) RETURN a")
    tx.commit()
    records = list(cursor)
    assert len(records) == 1
    for record in records:
        assert isinstance(record["a"], Node)
    assert tx.finished()


def test_can_run_query_that_returns_map_literal(graph):
    tx = graph.begin()
    cursor = tx.run("RETURN {foo:'bar'}")
    tx.commit()
    value = cursor.evaluate()
    assert value == {"foo": "bar"}


def test_can_run_transaction_as_with_statement(graph):
    with graph.begin() as tx:
        assert not tx.finished()
        tx.run("CREATE (a) RETURN a")
    assert tx.finished()


def test_can_run_multi_statement_transaction(graph):
    tx = graph.begin()
    assert not tx.finished()
    cursor_1 = tx.run("CREATE (a) RETURN a")
    cursor_2 = tx.run("CREATE (a) RETURN a")
    cursor_3 = tx.run("CREATE (a) RETURN a")
    tx.commit()
    for cursor in (cursor_1, cursor_2, cursor_3):
        records = list(cursor)
        assert len(records) == 1
        for record in records:
            assert isinstance(record["a"], Node)
    assert tx.finished()


def test_can_run_multi_execute_transaction(graph):
    tx = graph.begin()
    for i in range(10):
        assert not tx.finished()
        cursor_1 = tx.run("CREATE (a) RETURN a")
        cursor_2 = tx.run("CREATE (a) RETURN a")
        cursor_3 = tx.run("CREATE (a) RETURN a")
        tx.process()
        for cursor in (cursor_1, cursor_2, cursor_3):
            records = list(cursor)
            assert len(records) == 1
            for record in records:
                assert isinstance(record["a"], Node)
    tx.commit()
    assert tx.finished()


def test_can_rollback_transaction(graph):
    tx = graph.begin()
    for i in range(10):
        assert not tx.finished()
        cursor_1 = tx.run("CREATE (a) RETURN a")
        cursor_2 = tx.run("CREATE (a) RETURN a")
        cursor_3 = tx.run("CREATE (a) RETURN a")
        tx.process()
        for cursor in (cursor_1, cursor_2, cursor_3):
            records = list(cursor)
            assert len(records) == 1
            for record in records:
                assert isinstance(record["a"], Node)
    tx.rollback()
    assert tx.finished()


def test_cannot_append_after_transaction_finished(graph):
    tx = graph.begin()
    tx.rollback()
    try:
        tx.run("CREATE (a) RETURN a")
    except TransactionError as error:
        assert error.args[0] is tx
    else:
        assert False
