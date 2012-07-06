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

    def test_node_dump(self):
        a, = self.graph_db.create(
            {"name": "Alice"}
        )
        out = geoff.dumps([a])
        self.assertEqual('({0}) {{"name": "Alice"}}'.format(a.id), out)

    def test_subgraph_dump(self):
        a, b, ab = self.graph_db.create(
            {"name": "Alice"}, {"name": "Bob"}, (0, "KNOWS", 1)
        )
        out = geoff.dumps([a, b, ab])
        self.assertEqual('({0}) {{"name": "Alice"}}\n' \
                         '({1}) {{"name": "Bob"}}\n' \
                         '({0})-[{2}:KNOWS]->({1}) {{}}'.format(a.id, b.id, ab.id), out)

if __name__ == '__main__':
    unittest.main()

