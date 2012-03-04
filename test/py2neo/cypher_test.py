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


if __name__ == '__main__':
	unittest.main()
