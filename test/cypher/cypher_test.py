#/usr/bin/env python
# -*- coding: utf-8 -*-

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

import pytest

from py2neo.cypher import CypherQuery, CypherError
from py2neo.core import Node, Relationship, Path


def alice_and_bob(graph):
    return graph.create(
        {"name": "Alice", "age": 66},
        {"name": "Bob", "age": 77},
        (0, "KNOWS", 1),
    )


def test_nonsense_query(graph):
    query = "SELECT z=nude(0) RETURNS x"
    try:
        CypherQuery(graph, query).execute()
    except CypherError:
        assert True
    else:
        assert False


def test_can_run(graph):
    query = CypherQuery(graph, "CREATE (a {name:'Alice'}) "
                                        "RETURN a.name")
    query.run()
    assert True


def test_can_execute(graph):
    query = CypherQuery(graph, "CREATE (a {name:'Alice'}) "
                                        "RETURN a.name")
    results = query.execute()
    assert len(results) == 1
    assert len(results[0]) == 1
    assert results[0][0] == "Alice"


def test_can_execute_one(graph):
    query = CypherQuery(graph, "CREATE (a {name:'Alice'}) "
                                        "RETURN a.name")
    result = query.execute_one()
    assert result == "Alice"


def test_can_stream(graph):
    query = CypherQuery(graph, "CREATE (a {name:'Alice'}) "
                                        "RETURN a.name")
    stream = query.stream()
    results = list(stream)
    assert len(results) == 1
    assert len(results[0]) == 1
    assert results[0][0] == "Alice"


def test_many_queries(graph):
    node, = graph.create({})
    query = "START z=node({0}) RETURN z".format(node._id)
    for i in range(40):
        with CypherQuery(graph, query).execute() as records:
            for record in records:
                assert record.columns == ("z",)
                assert record.values == (node,)
    graph.delete(node)


class TestCypher(object):

    @pytest.fixture(autouse=True)
    def setup(self, graph):
        self.graph = graph

    def test_nonsense_query_with_error_handler(self):
        with pytest.raises(CypherError):
            self.graph.cypher.execute("SELECT z=nude(0) RETURNS x")

    def test_query(self):
        a, b, ab = alice_and_bob(self.graph)
        results = self.graph.cypher.execute((
            "start a=node({0}),b=node({1}) "
            "match a-[ab:KNOWS]->b "
            "return a,b,ab,a.name,b.name"
        ).format(a._id, b._id))
        assert len(results) == 1
        for row in results:
            assert len(row) == 5
            assert isinstance(row[0], Node)
            assert isinstance(row[1], Node)
            assert isinstance(row[2], Relationship)
            assert row[3] == "Alice"
            assert row[4] == "Bob"
        assert len(results.columns) == 5
        assert results.columns[0] == "a"
        assert results.columns[1] == "b"
        assert results.columns[2] == "ab"
        assert results.columns[3] == "a.name"
        assert results.columns[4] == "b.name"

    def test_query_can_return_path(self):
        a, b, ab = alice_and_bob(self.graph)
        results = self.graph.cypher.execute((
            "start a=node({0}),b=node({1}) "
            "match p=(a-[ab:KNOWS]->b) "
            "return p"
        ).format(a._id, b._id))
        assert len(results) == 1
        for row in results:
            assert len(row) == 1
            assert isinstance(row[0], Path)
            assert len(row[0].nodes) == 2
            assert row[0].nodes[0] == a
            assert row[0].nodes[1] == b
            assert row[0].relationships[0].type == "KNOWS"
        assert len(results.columns) == 1
        assert results.columns[0] == "p"

    def test_query_can_return_collection(self):
        node, = self.graph.create({})
        query = "START a = node({N}) RETURN collect(a);"
        params = {"N": node._id}
        results = self.graph.cypher.execute(query, params)
        assert results[0][0] == [node]

    def test_param_used_once(self):
        node, = self.graph.create({})
        query = (
            "START a=node({X}) "
            "RETURN a"
        )
        params = {"X": node._id}
        results = self.graph.cypher.execute(query, params)
        record = results[0]
        assert record.columns == ("a",)
        assert record.values == (node,)

    def test_param_used_twice(self):
        node, = self.graph.create({})
        query = (
            "START a=node({X}), b=node({X}) "
            "RETURN a, b"
        )
        params = {"X": node._id}
        results = self.graph.cypher.execute(query, params)
        record = results[0]
        assert record.columns == ("a", "b")
        assert record.values == (node, node)

    def test_param_used_thrice(self):
        node, = self.graph.create({})
        query = (
            "START a=node({X}), b=node({X}), c=node({X})"
            "RETURN a, b, c"
        )
        params = {"X": node._id}
        results = self.graph.cypher.execute(query, params)
        record = results[0]
        assert record.columns == ("a", "b", "c")
        assert record.values == (node, node, node)

    def test_param_reused_once_after_with_statement(self):
        a, b, ab = alice_and_bob(self.graph)
        query = (
            "START a=node({A}) "
            "MATCH (a)-[:KNOWS]->(b) "
            "WHERE a.age > {min_age} "
            "WITH a "
            "MATCH (a)-[:KNOWS]->(b) "
            "WHERE b.age > {min_age} "
            "RETURN b"
        )
        params = {"A": a._id, "min_age": 50}
        results = self.graph.cypher.execute(query, params)
        record = results[0]
        assert record.values == (b,)

    def test_param_reused_twice_after_with_statement(self):
        a, b, ab = alice_and_bob(self.graph)
        c, bc = self.graph.create(
            {"name": "Carol", "age": 88},
            (b, "KNOWS", 0),
        )
        query = (
            "START a=node({A}) "
            "MATCH (a)-[:KNOWS]->(b) "
            "WHERE a.age > {min_age} "
            "WITH a "
            "MATCH (a)-[:KNOWS]->(b) "
            "WHERE b.age > {min_age} "
            "WITH b "
            "MATCH (b)-[:KNOWS]->(c) "
            "WHERE c.age > {min_age} "
            "RETURN c"
        )
        params = {"A": a._id, "min_age": 50}
        results = self.graph.cypher.execute(query, params)
        record = results[0]
        assert record.values == (c,)

