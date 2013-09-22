#/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright 2011-2013, Nigel Small
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

import sys
PY3K = sys.version_info[0] >= 3

from py2neo import neo4j

import unittest


def default_graph_db():
    return neo4j.GraphDatabaseService("http://localhost:7474/db/data/")


class IsolateTestCase(unittest.TestCase):

    def setUp(self):
        self.graph_db = neo4j.GraphDatabaseService()
        self.graph_db.clear()

    def test_can_isolate_node(self):
        posse = self.graph_db.create(
            {"name": "Alice"},
            {"name": "Bob"},
            {"name": "Carol"},
            {"name": "Dave"},
            {"name": "Eve"},
            {"name": "Frank"},
            (0, "KNOWS", 1),
            (0, "KNOWS", 2),
            (0, "KNOWS", 3),
            (0, "KNOWS", 4),
            (2, "KNOWS", 0),
            (3, "KNOWS", 0),
            (4, "KNOWS", 0),
            (5, "KNOWS", 0),
        )
        alice = posse[0]
        friendships = list(alice.match())
        assert len(friendships) == 8
        alice.isolate()
        friendships = list(alice.match())
        assert len(friendships) == 0


class RelationshipTestCase(unittest.TestCase):

    def setUp(self):
        self.graph_db = default_graph_db()

    def test_create_relationship_to(self):
        alice, bob = self.graph_db.create(
            {"name": "Alice"}, {"name": "Bob"}
        )
        ab = alice.create_path("KNOWS", bob).relationships[0]
        self.assertTrue(ab is not None)
        self.assertTrue(isinstance(ab, neo4j.Relationship))
        self.assertEqual(alice, ab.start_node)
        self.assertEqual("KNOWS", ab.type)
        self.assertEqual(bob, ab.end_node)

    def test_create_relationship_from(self):
        alice, bob = self.graph_db.create(
            {"name": "Alice"}, {"name": "Bob"}
        )
        ba = bob.create_path("LIKES", alice).relationships[0]
        self.assertTrue(ba is not None)
        self.assertTrue(isinstance(ba, neo4j.Relationship))
        self.assertEqual(bob, ba.start_node)
        self.assertEqual("LIKES", ba.type)
        self.assertEqual(alice, ba.end_node)

    def test_getting_no_relationships(self):
        alice, = self.graph_db.create({"name": "Alice"})
        rels = list(alice.match())
        self.assertTrue(rels is not None)
        self.assertTrue(isinstance(rels, list))
        self.assertEqual(0, len(rels))

    def test_get_relationship(self):
        alice, bob, ab = self.graph_db.create({"name": "Alice"}, {"name": "Bob"}, (0, "KNOWS", 1))
        rel = self.graph_db.relationship(ab._id)
        assert rel == ab

    def test_create_relationship_with_properties(self):
        alice, bob = self.graph_db.create(
            {"name": "Alice"}, {"name": "Bob"}
        )
        ab = alice.create_path(("KNOWS", {"since": 1999}), bob).relationships[0]
        self.assertTrue(ab is not None)
        self.assertTrue(isinstance(ab, neo4j.Relationship))
        self.assertEqual(alice, ab.start_node)
        self.assertEqual("KNOWS", ab.type)
        self.assertEqual(bob, ab.end_node)
        self.assertEqual(len(ab), 1)
        self.assertEqual(ab["since"], 1999)
        self.assertEqual(ab.get_properties(), {"since": 1999})
        ab["foo"] = "bar"
        self.assertEqual(len(ab), 2)
        self.assertEqual(ab["foo"], "bar")
        self.assertEqual(ab.get_properties(), {"since": 1999, "foo": "bar"})
        del ab["foo"]
        self.assertEqual(len(ab), 1)
        self.assertEqual(ab["since"], 1999)
        self.assertEqual(ab.get_properties(), {"since": 1999})
        ab.delete_properties()
        self.assertEqual(len(ab), 0)
        self.assertEqual(ab.get_properties(), {})


class RelateTestCase(unittest.TestCase):

    def setUp(self):
        self.graph_db = default_graph_db()

    def test_relate(self):
        alice, bob = self.graph_db.create(
            {"name": "Alice"}, {"name": "Bob"}
        )
        rel = alice.get_or_create_path("KNOWS", bob).relationships[0]
        self.assertTrue(rel is not None)
        self.assertTrue(isinstance(rel, neo4j.Relationship))
        self.assertEqual(alice, rel.start_node)
        self.assertEqual("KNOWS", rel.type)
        self.assertEqual(bob, rel.end_node)

    def test_repeated_relate(self):
        alice, bob = self.graph_db.create(
            {"name": "Alice"}, {"name": "Bob"}
        )
        rel1 = alice.get_or_create_path("KNOWS", bob).relationships[0]
        self.assertTrue(rel1 is not None)
        self.assertTrue(isinstance(rel1, neo4j.Relationship))
        self.assertEqual(alice, rel1.start_node)
        self.assertEqual("KNOWS", rel1.type)
        self.assertEqual(bob, rel1.end_node)
        rel2 = alice.get_or_create_path("KNOWS", bob).relationships[0]
        self.assertEqual(rel1, rel2)
        rel3 = alice.get_or_create_path("KNOWS", bob).relationships[0]
        self.assertEqual(rel1, rel3)

    def test_relate_with_no_end_node(self):
        alice, = self.graph_db.create(
            {"name": "Alice"}
        )
        rel = alice.get_or_create_path("KNOWS", None).relationships[0]
        self.assertTrue(rel is not None)
        self.assertTrue(isinstance(rel, neo4j.Relationship))
        self.assertEqual(alice, rel.start_node)
        self.assertEqual("KNOWS", rel.type)

    def test_relate_with_data(self):
        alice, bob = self.graph_db.create(
            {"name": "Alice"}, {"name": "Bob"}
        )
        rel = alice.get_or_create_path(("KNOWS", {"since": 2006}), bob).relationships[0]
        self.assertTrue(rel is not None)
        self.assertTrue(isinstance(rel, neo4j.Relationship))
        self.assertEqual(alice, rel.start_node)
        self.assertEqual("KNOWS", rel.type)
        self.assertEqual(bob, rel.end_node)
        self.assertTrue("since" in rel)
        self.assertEqual(2006, rel["since"])

    def test_relate_with_null_data(self):
        alice, bob = self.graph_db.create(
            {"name": "Alice"}, {"name": "Bob"}
        )
        rel = alice.get_or_create_path(("KNOWS", {"since": 2006, "dummy": None}), bob).relationships[0]
        self.assertTrue(rel is not None)
        self.assertTrue(isinstance(rel, neo4j.Relationship))
        self.assertEqual(alice, rel.start_node)
        self.assertEqual("KNOWS", rel.type)
        self.assertEqual(bob, rel.end_node)
        self.assertTrue("since" in rel)
        self.assertEqual(2006, rel["since"])
        self.assertEqual(None, rel["dummy"])

    def test_repeated_relate_with_data(self):
        alice, bob = self.graph_db.create(
            {"name": "Alice"}, {"name": "Bob"}
        )
        rel1 = alice.get_or_create_path(("KNOWS", {"since": 2006}), bob).relationships[0]
        self.assertTrue(rel1 is not None)
        self.assertTrue(isinstance(rel1, neo4j.Relationship))
        self.assertEqual(alice, rel1.start_node)
        self.assertEqual("KNOWS", rel1.type)
        self.assertEqual(bob, rel1.end_node)
        rel2 = alice.get_or_create_path(("KNOWS", {"since": 2006}), bob).relationships[0]
        self.assertEqual(rel1, rel2)
        rel3 = alice.get_or_create_path(("KNOWS", {"since": 2006}), bob).relationships[0]
        self.assertEqual(rel1, rel3)

    # disabled test known to fail due to server issues
    #
    #def test_relate_with_list_data(self):
    #    alice, bob = self.graph_db.create(
    #        {"name": "Alice"}, {"name": "Bob"}
    #    )
    #    rel, = self.graph_db.get_or_create_relationships((alice, "LIKES", bob, {"reasons": ["looks", "wealth"]}))
    #    self.assertTrue(rel is not None)
    #    self.assertTrue(isinstance(rel, neo4j.Relationship))
    #    self.assertEqual(alice, rel.start_node)
    #    self.assertEqual("LIKES", rel.type)
    #    self.assertEqual(bob, rel.end_node)
    #    self.assertTrue("reasons" in rel)
    #    self.assertEqual("looks", rel["reasons"][0])
    #    self.assertEqual("wealth", rel["reasons"][1])

    def test_complex_relate(self):
        alice, bob, carol, dave = self.graph_db.create(
            {"name": "Alice"}, {"name": "Bob"},
            {"name": "Carol"}, {"name": "Dave"}
        )
        batch = neo4j.WriteBatch(self.graph_db)
        batch.get_or_create_path(alice, ("IS~MARRIED~TO", {"since": 1996}), bob)
        #batch.get_or_create((alice, "DISLIKES", carol, {"reasons": ["youth", "beauty"]}))
        batch.get_or_create_path(alice, ("DISLIKES!", {"reason": "youth"}), carol)
        rels1 = batch.submit()
        self.assertTrue(rels1 is not None)
        self.assertEqual(2, len(rels1))
        batch = neo4j.WriteBatch(self.graph_db)
        batch.get_or_create_path(bob, ("WORKS WITH", {"since": 2004, "company": "Megacorp"}), carol)
        #batch.get_or_create((alice, "DISLIKES", carol, {"reasons": ["youth", "beauty"]}))
        batch.get_or_create_path(alice, ("DISLIKES!", {"reason": "youth"}), carol)
        batch.get_or_create_path(bob, ("WORKS WITH", {"since": 2009, "company": "Megacorp"}), dave)
        rels2 = batch.submit()
        self.assertTrue(rels2 is not None)
        self.assertEqual(3, len(rels2))
        self.assertEqual(rels1[1], rels2[1])


if __name__ == '__main__':
    unittest.main()

