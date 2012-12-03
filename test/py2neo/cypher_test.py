#/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright 2011-2012 Nigel Small
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

from __future__ import print_function

import sys
PY3K = sys.version_info[0] >= 3

from py2neo import cypher, neo4j
import unittest


__author__    = "Nigel Small <py2neo@nigelsmall.org>"
__copyright__ = "Copyright 2011-2012 Nigel Small"
__license__   = "Apache License, Version 2.0"

class CypherTestCase(unittest.TestCase):

    def setUp(self):
        super(CypherTestCase, self).setUp()
        self.graph_db = neo4j.GraphDatabaseService("http://localhost:7474/db/data/")
        self.node_a, self.node_b = self.graph_db.create({"name": "Alice"}, {"name": "Bob"})
        self.rel_ab = self.node_a.create_relationship_to(self.node_b, "KNOWS")

    def test_nonsense_query(self):
        self.assertRaises(cypher.CypherError, cypher.execute,
            self.graph_db, "select z=nude(0) returns x"
        )

    def test_nonsense_query_with_error_handler(self):
        def print_error(message, exception, stacktrace):
            print(message)
            self.assertTrue(len(message) > 0)
        cypher.execute(
            self.graph_db, "select z=nude(0) returns x",
            error_handler=print_error
        )
        self.assertTrue(True)

    def test_query(self):
        rows, metadata = cypher.execute(self.graph_db,
            "start a=node({0}),b=node({1}) match a-[ab:KNOWS]->b return a,b,ab,a.name,b.name".format(
                self.node_a.id, self.node_b.id
            )
        )
        self.assertEqual(1, len(rows))
        for row in rows:
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
        a, b = self.graph_db.create(
            {"name": "Alice"},
            {"name": "Bob"}
        )
        ab = a.create_relationship_to(b, "KNOWS")
        def check_metadata(metadata):
            print(metadata)
            self.assertTrue(isinstance(metadata.columns, list))
            self.assertEqual(5, len(metadata.columns))
            self.assertEqual("a", metadata.columns[0])
            self.assertEqual("b", metadata.columns[1])
            self.assertEqual("ab", metadata.columns[2])
            self.assertEqual("a.name", metadata.columns[3])
            self.assertEqual("b.name", metadata.columns[4])
        def check_row(row):
            print(row)
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
        query = """\
        start a=node({0}),b=node({1})\
        match a-[ab]-b\
        return a,b,ab,a.name,b.name""".format(a.id, b.id)
        cypher.execute(self.graph_db, query,
            row_handler=check_row, metadata_handler=check_metadata
        )

    def test_query_with_params(self):
        a, b = self.graph_db.create(
                {"name": "Alice"},
                {"name": "Bob"}
        )
        ab = a.create_relationship_to(b, "KNOWS")
        def check_metadata(metadata):
            self.assertTrue(isinstance(metadata.columns, list))
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
        query = """\
        start a=node({A}),b=node({B})\
        match a-[ab]-b\
        return a,b,ab,a.name,b.name"""
        cypher.execute(self.graph_db, query, {"A": a.id, "B": b.id},
            row_handler=check_row, metadata_handler=check_metadata
        )

    def test_many_queries(self):
        node, = self.graph_db.create({})
        query = "start z=node(" + str(node._id) + ") return z"
        for i in range(2000):
            data, metadata = cypher.execute(self.graph_db, query)
            self.assertEqual(1, len(data))
        self.graph_db.delete(node)


class PathTestCase(unittest.TestCase):

    def setUp(self):
        super(PathTestCase, self).setUp()
        self.graph_db = neo4j.GraphDatabaseService("http://localhost:7474/db/data/")
        self.node_a, self.node_b, self.rel_ab = self.graph_db.create(
            {"name": "Alice"}, {"name": "Bob"}, (0, "KNOWS", 1)
        )

    def test_query_returning_path(self):
        rows, metadata = cypher.execute(self.graph_db,
            "start a=node({0}),b=node({1}) match p=(a-[ab:KNOWS]->b) return p".format(
                self.node_a.id, self.node_b.id
            )
        )
        self.assertEqual(1, len(rows))
        for row in rows:
            self.assertEqual(1, len(row))
            self.assertTrue(isinstance(row[0], neo4j.Path))
            self.assertEqual(2, len(row[0].nodes))
            self.assertEqual(self.node_a, row[0].nodes[0])
            self.assertEqual(self.node_b, row[0].nodes[1])
            self.assertEqual("KNOWS", row[0].relationships[0].type)
            self.assertEqual(id(self.graph_db), id(row[0].nodes[0]._graph_db))
            self.assertEqual(id(self.graph_db), id(row[0].nodes[1]._graph_db))
            self.assertEqual(id(self.graph_db), id(row[0].relationships[0]._graph_db))
        self.assertEqual(1, len(metadata.columns))
        self.assertEqual("p", metadata.columns[0])


class CollectionTestCase(unittest.TestCase):

    def setUp(self):
        self.graph_db = neo4j.GraphDatabaseService("http://localhost:7474/db/data/")
        self.graph_db.clear()

    def test_query_returning_collection(self):
        node, = self.graph_db.create({})
        query = "START a = node({N}) RETURN collect(a);"
        params = {"N": node._id}
        data, metadata = cypher.execute(self.graph_db, query, params)
        print(data)
        assert data[0][0] == [node]


class ReusedParamsTestCase(unittest.TestCase):

    def setUp(self):
        self.graph_db = neo4j.GraphDatabaseService("http://localhost:7474/db/data/")
        self.graph_db.clear()

    def test_param_used_once(self):
        node, = self.graph_db.create({})
        query = "START a=node({X})RETURN a"
        params = {"X": node._id}
        data, metadata = cypher.execute(self.graph_db, query, params)
        assert data[0] == [node]

    def test_param_used_twice(self):
        node, = self.graph_db.create({})
        query = "START a=node({X}), b=node({X}) RETURN a, b"
        params = {"X": node._id}
        data, metadata = cypher.execute(self.graph_db, query, params)
        assert data[0] == [node, node]

    def test_param_used_thrice(self):
        node, = self.graph_db.create({})
        query = "START a=node({X}), b=node({X}), c=node({X}) RETURN a, b, c"
        params = {"X": node._id}
        data, metadata = cypher.execute(self.graph_db, query, params)
        assert data[0] == [node, node, node]

    def test_param_reused_after_with_statement(self):
        a, b, ab = self.graph_db.create(
            {"name": "Alice", "age": 66},
            {"name": "Bob", "age": 77},
            (0, "KNOWS", 1),
        )
        query = "START a=node({A}) " \
                "MATCH (a)-[:KNOWS]->(b) " \
                "WHERE a.age > {min_age} " \
                "WITH a " \
                "MATCH (a)-[:KNOWS]->(b) " \
                "WHERE b.age > {min_age} " \
                "RETURN a, b"
        params = {"A": a._id, "min_age": 50}
        data, metadata = cypher.execute(self.graph_db, query, params)
        assert data[0] == [a, b]


if __name__ == '__main__':
    unittest.main()
