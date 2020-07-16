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


from pytest import raises, skip

from py2neo import ClientError, TransactionFinished


def test_can_generate_transaction_error(graph):
    tx = graph.begin()
    with raises(ClientError) as e:
        tx.run("X")
    assert e.value.code == "Neo.ClientError.Statement.SyntaxError"
    with raises(TransactionFinished):
        tx.commit()


def test_unique_path_not_unique_raises_cypher_transaction_error_in_transaction(graph):
    tx = graph.begin()
    cursor = tx.run("CREATE (a), (b) RETURN a, b")
    record = next(cursor)
    parameters = {"A": record["a"].identity, "B": record["b"].identity}
    statement = ("MATCH (a) WHERE id(a)=$A MATCH (b) WHERE id(b)=$B "
                 "CREATE (a)-[:KNOWS]->(b)")
    tx.run(statement, parameters)
    tx.run(statement, parameters)
    statement = ("MATCH (a) WHERE id(a)=$A MATCH (b) WHERE id(b)=$B "
                 "CREATE UNIQUE (a)-[:KNOWS]->(b)")
    with raises(ClientError) as e:
        tx.run(statement, parameters)
    if e.value.code == "Neo.ClientError.Statement.SyntaxError":
        skip("CREATE UNIQUE is not supported on this version of Neo4j")
    assert e.value.code == "Neo.ClientError.Statement.ConstraintVerificationFailed"
    with raises(TransactionFinished):
        tx.commit()
