#/usr/bin/env python
# -*- coding: utf-8 -*-

__author__    = "Nigel Small <py2neo@nigelsmall.org>"
__copyright__ = "Copyright 2011 Nigel Small"
__license__   = "Apache License, Version 2.0"

from py2neo import neo4j, geoff
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

    def test_subgraph_creation(self):
        subgraph = geoff.Subgraph(
            '(A) {"name": "Alice"}',
            '(B) {"name": "Bob"}',
            '(A)-[:KNOWS]->(B)'
        )
        self.assertEqual(
            '["(A) {\\\"name\\\": \\\"Alice\\\"}", "(B) {\\\"name\\\": \\\"Bob\\\"}", "(A)-[:KNOWS]->(B) {}"]',
            subgraph.__json__()
        )

    def test_subgraph_load(self):
        subgraph = geoff.Subgraph()
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
        subgraph = geoff.Subgraph()
        subgraph.loads('["(A) {\\\"name\\\": \\\"Alice\\\"}","(B) {\\\"name\\\": \\\"Bob\\\"}","(A)-[:KNOWS]->(B)"]')
        self.assertEqual(
            '["(A) {\\\"name\\\": \\\"Alice\\\"}", "(B) {\\\"name\\\": \\\"Bob\\\"}", "(A)-[:KNOWS]->(B) {}"]',
            subgraph.__json__()
        )

class DumperTest(unittest.TestCase):

    def setUp(self):
        self.graph_db = neo4j.GraphDatabaseService()

    def test_simple_dump(self):
        things = self.graph_db.create(
            {"name": "Alice"}, {"name": "Bob"}, (0, "KNOWS", 1)
        )
        print geoff.dumps(things)

if __name__ == '__main__':
    unittest.main()

