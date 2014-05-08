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

from py2neo import cypher, neo4j


def alice_and_bob(graph):
    return graph.create(
        {"name": "Alice", "age": 66},
        {"name": "Bob", "age": 77},
        (0, "KNOWS", 1),
    )


def test_nonsense_query(graph):
    query = "SELECT z=nude(0) RETURNS x"
    try:
        neo4j.CypherQuery(graph, query).execute()
    except neo4j.CypherError:
        assert True
    else:
        assert False


def test_can_run(graph):
    query = neo4j.CypherQuery(graph, "CREATE (a {name:'Alice'}) "
                                        "RETURN a.name")
    query.run()
    assert True


def test_can_execute(graph):
    query = neo4j.CypherQuery(graph, "CREATE (a {name:'Alice'}) "
                                        "RETURN a.name")
    results = query.execute()
    assert len(results) == 1
    assert len(results[0]) == 1
    assert results[0][0] == "Alice"


def test_can_execute_one(graph):
    query = neo4j.CypherQuery(graph, "CREATE (a {name:'Alice'}) "
                                        "RETURN a.name")
    result = query.execute_one()
    assert result == "Alice"


def test_can_stream(graph):
    query = neo4j.CypherQuery(graph, "CREATE (a {name:'Alice'}) "
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
        with neo4j.CypherQuery(graph, query).execute() as records:
            for record in records:
                assert record.columns == ("z",)
                assert record.values == (node,)
    graph.delete(node)


class CypherTestCase(object):
    @pytest.fixture(autouse=True)
    def setup(self, graph):
        self.graph = graph

    def test_nonsense_query_with_error_handler(self):
        def print_error(message, exception, stacktrace):
            print(message)
            self.assertTrue(len(message) > 0)
        cypher.execute(self.graph, (
            "SELECT z=nude(0) "
            "RETURNS x"
        ), error_handler=print_error)
        self.assertTrue(True)

    def test_query(self):
        a, b, ab = alice_and_bob(self.graph)
        data, metadata = cypher.execute(self.graph, (
            "start a=node({0}),b=node({1}) "
            "match a-[ab:KNOWS]->b "
            "return a,b,ab,a.name,b.name"
        ).format(a._id, b._id))
        self.assertEqual(1, len(data))
        for row in data:
            self.assertEqual(5, len(row))
            self.assertTrue(isinstance(row[0], neo4j.Node))
            self.assertTrue(isinstance(row[1], neo4j.Node))
            self.assertTrue(isinstance(row[2], neo4j.Relationship))
            self.assertEqual("Alice", row[3])
            self.assertEqual("Bob", row[4])
        self.assertEqual(5, len(metadata.columns))
        self.assertEqual("a", metadata.columns[0])
        self.assertEqual("b", metadata.columns[1])
        self.assertEqual("ab", metadata.columns[2])
        self.assertEqual("a.name", metadata.columns[3])
        self.assertEqual("b.name", metadata.columns[4])

    def test_query_with_handlers(self):
        a, b, ab = alice_and_bob(self.graph)
        def check_metadata(metadata):
            self.assertEqual(5, len(metadata.columns))
            self.assertEqual("a", metadata.columns[0])
            self.assertEqual("b", metadata.columns[1])
            self.assertEqual("ab", metadata.columns[2])
            self.assertEqual("a.name", metadata.columns[3])
            self.assertEqual("b.name", metadata.columns[4])
        def check_row(row):
            self.assertTrue(isinstance(row, list))
            self.assertEqual(5, len(row))
            self.assertTrue(isinstance(row[0], neo4j.Node))
            self.assertTrue(isinstance(row[1], neo4j.Node))
            self.assertTrue(isinstance(row[2], neo4j.Relationship))
            self.assertEqual(row[0], a)
            self.assertEqual(row[1], b)
            self.assertEqual(row[2], ab)
            self.assertEqual(row[3], "Alice")
            self.assertEqual(row[4], "Bob")
        query = (
            "START a=node({0}), b=node({1}) "
            "MATCH a-[ab]-b "
            "RETURN a,b,ab,a.name,b.name"
        ).format(a._id, b._id)
        cypher.execute(self.graph, query,
            row_handler=check_row, metadata_handler=check_metadata
        )

    def test_query_with_params(self):
        a, b, ab = alice_and_bob(self.graph)
        def check_metadata(metadata):
            self.assertEqual(5, len(metadata.columns))
            self.assertEqual("a", metadata.columns[0])
            self.assertEqual("b", metadata.columns[1])
            self.assertEqual("ab", metadata.columns[2])
            self.assertEqual("a.name", metadata.columns[3])
            self.assertEqual("b.name", metadata.columns[4])
        def check_row(row):
            self.assertTrue(isinstance(row, list))
            self.assertEqual(5, len(row))
            self.assertTrue(isinstance(row[0], neo4j.Node))
            self.assertTrue(isinstance(row[1], neo4j.Node))
            self.assertTrue(isinstance(row[2], neo4j.Relationship))
            self.assertEqual(row[0], a)
            self.assertEqual(row[1], b)
            self.assertEqual(row[2], ab)
            self.assertEqual(row[3], "Alice")
            self.assertEqual(row[4], "Bob")
        query = (
            "START a=node({A}),b=node({B}) "
            "MATCH a-[ab]-b "
            "RETURN a,b,ab,a.name,b.name"
        )
        cypher.execute(self.graph, query, {"A": a._id, "B": b._id},
            row_handler=check_row, metadata_handler=check_metadata
        )

    def test_query_can_return_path(self):
        a, b, ab = alice_and_bob(self.graph)
        data, metadata = cypher.execute(self.graph, (
            "start a=node({0}),b=node({1}) "
            "match p=(a-[ab:KNOWS]->b) "
            "return p"
        ).format(a._id, b._id))
        self.assertEqual(1, len(data))
        for row in data:
            self.assertEqual(1, len(row))
            self.assertTrue(isinstance(row[0], neo4j.Path))
            self.assertEqual(2, len(row[0].nodes))
            self.assertEqual(a, row[0].nodes[0])
            self.assertEqual(b, row[0].nodes[1])
            self.assertEqual("KNOWS", row[0].relationships[0].type)
        self.assertEqual(1, len(metadata.columns))
        self.assertEqual("p", metadata.columns[0])

    def test_query_can_return_collection(self):
        node, = self.graph.create({})
        query = "START a = node({N}) RETURN collect(a);"
        params = {"N": node._id}
        data, metadata = cypher.execute(self.graph, query, params)
        assert data[0][0] == [node]

    def test_param_used_once(self):
        node, = self.graph.create({})
        query = (
            "START a=node({X}) "
            "RETURN a"
        )
        params = {"X": node._id}
        data, metadata = cypher.execute(self.graph, query, params)
        assert data[0] == [node]

    def test_param_used_twice(self):
        node, = self.graph.create({})
        query = (
            "START a=node({X}), b=node({X}) "
            "RETURN a, b"
        )
        params = {"X": node._id}
        data, metadata = cypher.execute(self.graph, query, params)
        assert data[0] == [node, node]

    def test_param_used_thrice(self):
        node, = self.graph.create({})
        query = (
            "START a=node({X}), b=node({X}), c=node({X})"
            "RETURN a, b, c"
        )
        params = {"X": node._id}
        data, metadata = cypher.execute(self.graph, query, params)
        assert data[0] == [node, node, node]

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
        data, metadata = cypher.execute(self.graph, query, params)
        assert data[0] == [b]

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
        data, metadata = cypher.execute(self.graph, query, params)
        assert data[0] == [c]


class CypherDumpTestCase(object):
    def test_can_dump_string(self):
        assert cypher.dumps('hello') == '"hello"'

    def test_can_dump_number(self):
        assert cypher.dumps(42) == '42'

    def test_can_dump_map(self):
        assert cypher.dumps({"one": 1}) == '{one: 1}'

    def test_can_dump_map_with_space_in_key(self):
        assert cypher.dumps({"number one": 1}) == '{`number one`: 1}'

    def test_can_dump_map_with_backticks_in_key(self):
        assert cypher.dumps({"number `one`": 1}) == '{`number ``one```: 1}'

    def test_can_dump_list(self):
        assert cypher.dumps([4, 5, 6]) == '[4, 5, 6]'
