#/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
PY3K = sys.version_info[0] >= 3

__author__    = "Nigel Small <py2neo@nigelsmall.org>"
__copyright__ = "Copyright 2011 Nigel Small"
__license__   = "Apache License, Version 2.0"

import unittest

from py2neo import cypher, neo4j

if PY3K:
    from io import StringIO
else:
    from cStringIO import StringIO


class CypherTestCase(unittest.TestCase):

    def setUp(self):
        super(CypherTestCase, self).setUp()
        self.graph_db = neo4j.GraphDatabaseService("http://localhost:7474/db/data/")
        self.node_a = self.graph_db.create_node(name="Alice")
        self.node_b = self.graph_db.create_node(name="Bob")
        self.rel_ab = self.node_a.create_relationship_to(self.node_b, "KNOWS")

    def test_query(self):
        rows, metadata = cypher.execute("start a=node({}) match a-[ab:KNOWS]->b return a,b,ab,a.name,b.name".format(self.node_a.get_id(), self.node_b.get_id()), self.graph_db)
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

    def test_output_query_as_delimited(self):
        file = StringIO()
        cypher.execute_and_output_as_delimited("start n=node(0) return n", self.graph_db, out=file)
        self.assertEqual("""\
"n"
"(0)"
""", file.getvalue())

    def test_output_query_as_geoff(self):
        file = StringIO()
        cypher.execute_and_output_as_geoff("start n=node(0) return n", self.graph_db, out=file)
        self.assertEqual("""\
(0)\t{}
""", file.getvalue())

    def test_output_query_as_json(self):
        file = StringIO()
        cypher.execute_and_output_as_json("start n=node(0) return n", self.graph_db, out=file)
        self.assertEqual("""\
[
\t{"n": "(0)"}
]
""", file.getvalue())

    def test_output_query_as_text(self):
        file = StringIO()
        cypher.execute_and_output_as_text("start n=node(0) return n", self.graph_db, out=file)
        self.assertEqual("""\
+--------+
| n      |
+--------+
| (0) {} |
+--------+
""", file.getvalue())

    def test_query_with_handlers(self):
        a, b = self.graph_db.create_nodes(
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
        start a=node({}),b=node({})\
        match a-[ab]-b\
        return a,b,ab,a.name,b.name""".format(a.id, b.id)
        cypher.execute(query, self.graph_db,
            row_handler=check_row, metadata_handler=check_metadata
        )

if __name__ == '__main__':
    unittest.main()
