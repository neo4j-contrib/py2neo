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
        alice, bob = self.graph_db.create(
            {"name": "Alice"}, {"name": "Bob"}
        )
        rel, = self.graph_db.relate((alice, "KNOWS", bob))
        self.assertIsNotNone(rel)
        self.assertTrue(isinstance(rel, neo4j.Relationship))
        self.assertEqual(alice, rel.start_node)
        self.assertEqual("KNOWS", rel.type)
        self.assertEqual(bob, rel.end_node)

    def test_repeated_relate(self):
        alice, bob = self.graph_db.create(
            {"name": "Alice"}, {"name": "Bob"}
        )
        rel1, = self.graph_db.relate((alice, "KNOWS", bob))
        self.assertIsNotNone(rel1)
        self.assertTrue(isinstance(rel1, neo4j.Relationship))
        self.assertEqual(alice, rel1.start_node)
        self.assertEqual("KNOWS", rel1.type)
        self.assertEqual(bob, rel1.end_node)
        rel2, = self.graph_db.relate((alice, "KNOWS", bob))
        self.assertEqual(rel1, rel2)
        rel3, = self.graph_db.relate((alice, "KNOWS", bob))
        self.assertEqual(rel1, rel3)

    def test_relate_with_no_start_node(self):
        bob, = self.graph_db.create(
            {"name": "Bob"}
        )
        rel, = self.graph_db.relate((None, "KNOWS", bob))
        self.assertIsNotNone(rel)
        self.assertTrue(isinstance(rel, neo4j.Relationship))
        self.assertEqual("KNOWS", rel.type)
        self.assertEqual(bob, rel.end_node)

    def test_relate_with_no_end_node(self):
        alice, = self.graph_db.create(
            {"name": "Alice"}
        )
        rel, = self.graph_db.relate((alice, "KNOWS", None))
        self.assertIsNotNone(rel)
        self.assertTrue(isinstance(rel, neo4j.Relationship))
        self.assertEqual(alice, rel.start_node)
        self.assertEqual("KNOWS", rel.type)

    def test_relate_with_no_nodes(self):
        rel = (None, "KNOWS", None)
        self.assertRaises(ValueError, self.graph_db.relate, rel)

    def test_relate_with_data(self):
        alice, bob = self.graph_db.create(
            {"name": "Alice"}, {"name": "Bob"}
        )
        rel, = self.graph_db.relate((alice, "KNOWS", bob, {"since": 2006}))
        self.assertIsNotNone(rel)
        self.assertTrue(isinstance(rel, neo4j.Relationship))
        self.assertEqual(alice, rel.start_node)
        self.assertEqual("KNOWS", rel.type)
        self.assertEqual(bob, rel.end_node)
        self.assertTrue("since" in rel)
        self.assertEqual(2006, rel["since"])

    def test_repeated_relate_with_data(self):
        alice, bob = self.graph_db.create(
            {"name": "Alice"}, {"name": "Bob"}
        )
        rel1, = self.graph_db.relate((alice, "KNOWS", bob, {"since": 2006}))
        self.assertIsNotNone(rel1)
        self.assertTrue(isinstance(rel1, neo4j.Relationship))
        self.assertEqual(alice, rel1.start_node)
        self.assertEqual("KNOWS", rel1.type)
        self.assertEqual(bob, rel1.end_node)
        rel2, = self.graph_db.relate((alice, "KNOWS", bob, {"since": 2006}))
        self.assertEqual(rel1, rel2)
        rel3, = self.graph_db.relate((alice, "KNOWS", bob, {"since": 2006}))
        self.assertEqual(rel1, rel3)

    def test_relate_with_list_data(self):
        alice, bob = self.graph_db.create(
            {"name": "Alice"}, {"name": "Bob"}
        )
        rel, = self.graph_db.relate((alice, "LIKES", bob, {"reasons": ["looks", "wealth"]}))
        self.assertIsNotNone(rel)
        self.assertTrue(isinstance(rel, neo4j.Relationship))
        self.assertEqual(alice, rel.start_node)
        self.assertEqual("LIKES", rel.type)
        self.assertEqual(bob, rel.end_node)
        self.assertTrue("reasons" in rel)
        self.assertEqual("looks", rel["reasons"][0])
        self.assertEqual("wealth", rel["reasons"][1])

    def test_complex_relate(self):
        alice, bob, carol, dave = self.graph_db.create(
            {"name": "Alice"}, {"name": "Bob"},
            {"name": "Carol"}, {"name": "Dave"}
        )
        rels1 = self.graph_db.relate(
            (alice, "IS~MARRIED~TO", bob, {"since": 1996}),
            #(alice, "DISLIKES", carol, {"reasons": ["youth", "beauty"]}),
            (alice, "DISLIKES!", carol, {"reason": "youth"}),
        )
        self.assertIsNotNone(rels1)
        self.assertEqual(2, len(rels1))
        rels2 = self.graph_db.relate(
            (bob, "WORKS WITH", carol, {"since": 2004, "company": "Megacorp"}),
            #(alice, "DISLIKES", carol, {"reasons": ["youth", "beauty"]}),
            (alice, "DISLIKES!", carol, {"reason": "youth"}),
            (bob, "WORKS WITH", dave, {"since": 2009, "company": "Megacorp"}),
        )
        self.assertIsNotNone(rels2)
        self.assertEqual(3, len(rels2))
        self.assertEqual(rels1[1], rels2[1])


if __name__ == '__main__':
    unittest.main()

