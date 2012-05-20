#/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
PY3K = sys.version_info[0] >= 3

__author__    = "Nigel Small <py2neo@nigelsmall.org>"
__copyright__ = "Copyright 2011 Nigel Small"
__license__   = "Apache License, Version 2.0"

from py2neo import neo4j, rest, batch

import unittest


def default_graph_db():
    return neo4j.GraphDatabaseService("http://localhost:7474/db/data/")


class NodeIndexTestCase(unittest.TestCase):

    def setUp(self):
        self.graph_db = default_graph_db()

    def test_get_node_index(self):
        index1 = self.graph_db.get_node_index("index1")
        self.assertIsNotNone(index1)

    def test_add_node_to_index(self):
        index1 = self.graph_db.get_node_index("index1")
        index1.remove("surname", "Smith")
        alice = self.graph_db.create_node({"name": "Alice Smith"})
        index1.add("surname", "Smith", alice)
        entities = index1.get("surname", "Smith")
        self.assertIsNotNone(entities)
        self.assertTrue(isinstance(entities, list))
        self.assertEqual(1, len(entities))
        self.assertEqual(alice, entities[0])

    def test_add_node_to_index_with_spaces(self):
        index1 = self.graph_db.get_node_index("index1")
        index1.remove("family name", "von Schmidt")
        alice = self.graph_db.create_node({"name": "Alice von Schmidt"})
        index1.add("family name", "von Schmidt", alice)
        entities = index1.get("family name", "von Schmidt")
        self.assertIsNotNone(entities)
        self.assertTrue(isinstance(entities, list))
        self.assertEqual(1, len(entities))
        self.assertEqual(alice, entities[0])

    def test_add_node_to_index_with_odd_chars(self):
        index1 = self.graph_db.get_node_index("index1")
        index1.remove("@!%#", "!\"£$%^&*()")
        alice = self.graph_db.create_node({"name": "Alice Smith"})
        index1.add("@!%#", "!\"£$%^&*()", alice)
        entities = index1.get("@!%#", "!\"£$%^&*()")
        self.assertIsNotNone(entities)
        self.assertTrue(isinstance(entities, list))
        self.assertEqual(1, len(entities))
        self.assertEqual(alice, entities[0])

    def test_add_multiple_nodes_to_index(self):
        index1 = self.graph_db.get_node_index("index1")
        index1.remove("surname", "Smith")
        alice, bob, carol = self.graph_db.create(
            {"name": "Alice Smith"},
            {"name": "Bob Smith"},
            {"name": "Carol Smith"}
        )
        index1.add("surname", "Smith", alice, bob, carol)
        entities = index1.get("surname", "Smith")
        self.assertIsNotNone(entities)
        self.assertTrue(isinstance(entities, list))
        self.assertEqual(3, len(entities))
        for entity in entities:
            self.assertTrue(entity in (alice, bob, carol))

    def test_get_or_create_node(self):
        index1 = self.graph_db.get_node_index("index1")
        index1.remove("surname", "Smith")
        alice, = index1.get_or_create("surname", "Smith", {"name": "Alice Smith"})
        self.assertIsNotNone(alice)
        self.assertTrue(isinstance(alice, neo4j.Node))
        self.assertEqual("Alice Smith", alice["name"])
        alice_id = alice.id
        for i in range(10):
            alice, = index1.get_or_create("surname", "Smith", {"name": "Alice Smith"})
            self.assertIsNotNone(alice)
            self.assertTrue(isinstance(alice, neo4j.Node))
            self.assertEqual("Alice Smith", alice["name"])
            self.assertEqual(alice_id, alice.id)

#    def test_node_index_query(self):
#        index1 = self.graph_db.get_node_index("index1")
#        node1 = self.graph_db.create_node()
#        node2 = self.graph_db.create_node()
#        node3 = self.graph_db.create_node()
#        index1.add("colour", "red", node1)
#        index1.add("colour", "green", node2)
#        index1.add("colour", "blue", node3)
#        s = index1.query("colour:*r*")
#        print("Found index entries: {0}".format(s))
#        self.assertTrue(node1 in s)
#        self.assertTrue(node2 in s)
#        self.assertFalse(node3 in s)


if __name__ == '__main__':
    unittest.main()

