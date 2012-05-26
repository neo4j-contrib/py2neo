#/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
PY3K = sys.version_info[0] >= 3

__author__    = "Nigel Small <py2neo@nigelsmall.org>"
__copyright__ = "Copyright 2011 Nigel Small"
__license__   = "Apache License, Version 2.0"

from py2neo import neo4j

import unittest


def default_graph_db():
    return neo4j.GraphDatabaseService("http://localhost:7474/db/data/")


class RelationshipTestCase(unittest.TestCase):

    def setUp(self):
        self.graph_db = default_graph_db()

    def test_create_relationship_to(self):
        alice, bob = self.graph_db.create(
            {"name": "Alice"}, {"name": "Bob"}
        )
        ab = alice.create_relationship_to(bob, "KNOWS")
        self.assertIsNotNone(ab)
        self.assertTrue(isinstance(ab, neo4j.Relationship))
        self.assertEqual(alice, ab.start_node)
        self.assertEqual("KNOWS", ab.type)
        self.assertEqual(bob, ab.end_node)

    def test_create_relationship_from(self):
        alice, bob = self.graph_db.create(
            {"name": "Alice"}, {"name": "Bob"}
        )
        ba = alice.create_relationship_from(bob, "LIKES")
        self.assertIsNotNone(ba)
        self.assertTrue(isinstance(ba, neo4j.Relationship))
        self.assertEqual(bob, ba.start_node)
        self.assertEqual("LIKES", ba.type)
        self.assertEqual(alice, ba.end_node)


class RelateTestCase(unittest.TestCase):

    def setUp(self):
        self.graph_db = default_graph_db()

    def test_relate(self):
        alice, bob, carol, dave = self.graph_db.create(
            {"name": "Alice"}, {"name": "Bob"},
            {"name": "Carol"}, {"name": "Dave"}
        )
        r = self.graph_db.relate((alice, "KNOWS", bob), (carol, "KNOWS", dave))
        print r[0], r[1]
        r = self.graph_db.relate((alice, "KNOWS", bob), (carol, "KNOWS", dave))
        print r[0], r[1]


if __name__ == '__main__':
    unittest.main()

