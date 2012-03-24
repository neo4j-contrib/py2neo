#/usr/bin/env python
# -*- coding: utf-8 -*-

__author__    = "Nigel Small <py2neo@nigelsmall.org>"
__copyright__ = "Copyright 2011 Nigel Small"
__license__   = "Apache License, Version 2.0"

from py2neo.geoff import Subgraph
from py2neo import neo4j
import sys
import unittest

gdb_uri = "http://localhost:7474/db/data/"

def get_gdb():
	try:
		return neo4j.GraphDatabaseService(gdb_uri)
	except IOError:
		sys.exit("Unable to attach to GraphDatabaseService at {0}".format(gdb_uri))


class SubgraphTest(unittest.TestCase):

	def setUp(self):
		self.gdb = get_gdb()
		print("Neo4j Version: {0}".format(repr(self.gdb._neo4j_version)))

	def test_subgraph_creation(self):
		subgraph = Subgraph(
			'(A) {"name": "Alice"}',
			'(B) {"name": "Bob"}',
			'(A)-[:KNOWS]->(B)'
		)
		self.assertEqual(
			'["(A) {\\\"name\\\": \\\"Alice\\\"}", "(B) {\\\"name\\\": \\\"Bob\\\"}", "(A)-[:KNOWS]->(B) {}"]',
			subgraph.__json__()
		)

	def test_subgraph_load(self):
		subgraph = Subgraph()
		subgraph.loads("""\
(A) {"name": "Alice"}
(B) {"name": "Bob"}
(A)-[:KNOWS]->(B)
""")
		self.assertEqual(
			'["(A) {\\\"name\\\": \\\"Alice\\\"}", "(B) {\\\"name\\\": \\\"Bob\\\"}", "(A)-[:KNOWS]->(B) {}"]',
			subgraph.__json__()
		)

	def test_subgraph_load_json(self):
		subgraph = Subgraph()
		subgraph.loads('["(A) {\\\"name\\\": \\\"Alice\\\"}","(B) {\\\"name\\\": \\\"Bob\\\"}","(A)-[:KNOWS]->(B)"]')
		self.assertEqual(
			'["(A) {\\\"name\\\": \\\"Alice\\\"}", "(B) {\\\"name\\\": \\\"Bob\\\"}", "(A)-[:KNOWS]->(B) {}"]',
			subgraph.__json__()
		)

if __name__ == '__main__':
	unittest.main()

