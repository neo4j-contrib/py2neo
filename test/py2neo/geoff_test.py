#/usr/bin/env python
# -*- coding: utf-8 -*-

__author__    = "Nigel Small <py2neo@nigelsmall.org>"
__copyright__ = "Copyright 2011 Nigel Small"
__license__   = "Apache License, Version 2.0"

from py2neo import neo4j, geoff
import unittest


class ParseTest(unittest.TestCase):

    def test_parsing_empty_string(self):
        nodes, rels = geoff._parse('')
        self.assertEqual([], nodes)
        self.assertEqual([], rels)

    def test_parsing_blank_lines(self):
        nodes, rels = geoff._parse('\n\n\n')
        self.assertEqual([], nodes)
        self.assertEqual([], rels)

    def test_parsing_comment(self):
        nodes, rels = geoff._parse('# this is a comment')
        self.assertEqual([], nodes)
        self.assertEqual([], rels)

    def test_parsing_single_node(self):
        nodes, rels = geoff._parse('(A)')
        self.assertEqual([('A', {})], nodes)
        self.assertEqual([], rels)

    def test_parsing_single_node_with_data(self):
        nodes, rels = geoff._parse('(A) {"name": "Alice"}')
        self.assertEqual([('A', {u'name': u'Alice'})], nodes)
        self.assertEqual([], rels)

    def test_parsing_simple_graph(self):
        nodes, rels = geoff._parse(
            '(A) {"name": "Alice"}\n' \
            '(B) {"name": "Bob"}\n' \
            '(A)-[:KNOWS]->(B)\n'
        )
        self.assertEqual([('A', {u'name': u'Alice'}), ('B', {u'name': u'Bob'})], nodes)
        self.assertEqual([(None, ('A', 'KNOWS', 'B', {}))], rels)


class SubgraphTest(unittest.TestCase):

    def test_empty_subgraph_creation(self):
        s = geoff.Subgraph()
        self.assertEqual(0, len(s.nodes))
        self.assertEqual(0, len(s.relationships))

    def test_simple_subgraph_creation(self):
        s = geoff.Subgraph({"name": "Alice"}, {"name": "Bob"}, (0, "KNOWS", 1))
        self.assertEqual(2, len(s.nodes))
        self.assertEqual(1, len(s.relationships))

    def test_subgraph_creation_from_text(self):
        s = geoff.Subgraph(
            '(A) {"name": "Alice"}',
            '(B) {"name": "Bob"}',
            '(A)-[:KNOWS]->(B)'
        )
        self.assertEqual('(A) {"name": "Alice"}\n(B) {"name": "Bob"}\n(A)-[0:KNOWS]->(B) {}', s.dumps())

    def test_subgraph_creation_from_db(self):
        graph_db = neo4j.GraphDatabaseService()
        a, b, ab = graph_db.create(
            {"name": "Alice"}, {"name": "Bob"}, (0, "KNOWS", 1)
        )
        s = geoff.Subgraph(a, b, ab)
        self.assertEqual('(0) {"name": "Alice"}\n(1) {"name": "Bob"}\n(0)-[0:KNOWS]->(1) {}', s.dumps())

class DumpTest(unittest.TestCase):

    def setUp(self):
        self.graph_db = neo4j.GraphDatabaseService()

    def test_node_dump(self):
        a, = self.graph_db.create(
            {"name": "Alice"}
        )
        out = geoff.dumps([a])
        self.assertEqual('(0) {"name": "Alice"}', out)

    def test_subgraph_dump(self):
        a, b, ab = self.graph_db.create(
            {"name": "Alice"}, {"name": "Bob"}, (0, "KNOWS", 1)
        )
        out = geoff.dumps([a, b, ab])
        self.assertEqual('(0) {"name": "Alice"}\n' \
                         '(1) {"name": "Bob"}\n' \
                         '(0)-[0:KNOWS]->(1) {}', out)


class InsertTest(unittest.TestCase):

    def setUp(self):
        self.graph_db = neo4j.GraphDatabaseService()

    def test_insert(self):
        s = geoff.Subgraph(
            '(A) {"name": "Alice"}',
            '(B) {"name": "Bob"}',
            '(A)-[:KNOWS]->(B)'
        )
        params = s.insert_into(self.graph_db)
        self.assertIn("(A)", params)
        self.assertIn("(B)", params)
        self.assertIn("[0]", params)


class MergeTest(unittest.TestCase):

    def setUp(self):
        self.graph_db = neo4j.GraphDatabaseService()

    def test_merge(self):
        s = geoff.Subgraph(
            '(A) {"name": "Alice"}',
            '(B) {"name": "Bob"}',
            '(A)-[:KNOWS]->(B)'
        )
        params = s.merge_into(self.graph_db)
        self.assertIn("(A)", params)
        self.assertIn("(B)", params)
        self.assertIn("[0]", params)


if __name__ == '__main__':
    unittest.main()

