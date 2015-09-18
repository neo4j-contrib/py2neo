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


from py2neo.http.cypher import CypherError, CypherTransactionError
from py2neo.error import GraphError
from py2neo.packages.httpstream import ClientError as _ClientError, Response as _Response

from .util import assert_error


def test_invalid_syntax_raises_cypher_error(graph):
    cypher = graph.cypher
    try:
        cypher.execute("X")
    except CypherTransactionError as error:
        assert error.code == "Neo.ClientError.Statement.InvalidSyntax"
    except CypherError as error:
        assert_error(
            error, (CypherError, GraphError), "org.neo4j.cypher.SyntaxException",
            (_ClientError, _Response), 400)
    else:
        assert False


def test_unique_path_not_unique_raises_cypher_error(graph):
    cypher = graph.cypher
    results = cypher.execute("CREATE (a), (b) RETURN a, b")
    parameters = {"A": results[0].a, "B": results[0].b}
    statement = ("MATCH (a) WHERE id(a)={A} MATCH (b) WHERE id(b)={B}" +
                 "CREATE (a)-[:KNOWS]->(b)")
    cypher.execute(statement, parameters)
    cypher.execute(statement, parameters)
    try:
        statement = ("MATCH (a) WHERE id(a)={A} MATCH (b) WHERE id(b)={B}" +
                     "CREATE UNIQUE (a)-[:KNOWS]->(b)")
        cypher.execute(statement, parameters)
    except CypherTransactionError as error:
        assert error.code == "Neo.ClientError.Statement.ConstraintViolation"
    except CypherError as error:
        assert_error(
            error, (CypherError, GraphError), "org.neo4j.cypher.UniquePathNotUniqueException",
            (_ClientError, _Response), 400)
    else:
        assert False
