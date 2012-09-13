#/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright 2011-2012 Nigel Small
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

__author__    = "Nigel Small <py2neo@nigelsmall.org>"
__copyright__ = "Copyright 2011-2012 Nigel Small"
__license__   = "Apache License, Version 2.0"

from py2neo import neo4j

import unittest


def default_graph_db():
    return neo4j.GraphDatabaseService("http://localhost:7474/db/data/")


class NodeIndexTestCase(unittest.TestCase):

    def setUp(self):
        self.graph_db = default_graph_db()

    def test_get_node_index(self):
        index1 = self.graph_db.get_or_create_index(neo4j.Node, "index1")
        self.assertIsNotNone(index1)
        self.assertEqual("index1", index1.name)
        self.assertEqual(neo4j.Node, index1.content_type)

    def test_add_node_to_index(self):
        index1 = self.graph_db.get_or_create_index(neo4j.Node, "index1")
        index1.remove("surname", "Smith")
        alice, = self.graph_db.create({"name": "Alice Smith"})
        index1.add("surname", "Smith", alice)
        entities = index1.get("surname", "Smith")
        self.assertIsNotNone(entities)
        self.assertTrue(isinstance(entities, list))
        self.assertEqual(1, len(entities))
        self.assertEqual(alice, entities[0])

    def test_add_node_to_index_with_spaces(self):
        index1 = self.graph_db.get_or_create_index(neo4j.Node, "index1")
        index1.remove("family name", "von Schmidt")
        alice, = self.graph_db.create({"name": "Alice von Schmidt"})
        index1.add("family name", "von Schmidt", alice)
        entities = index1.get("family name", "von Schmidt")
        self.assertIsNotNone(entities)
        self.assertTrue(isinstance(entities, list))
        self.assertEqual(1, len(entities))
        self.assertEqual(alice, entities[0])

    def test_add_node_to_index_with_odd_chars(self):
        index1 = self.graph_db.get_or_create_index(neo4j.Node, "index1")
        index1.remove("@!%#", "!\"£$%^&*()")
        alice = self.graph_db.create_node({"name": "Alice Smith"})
        index1.add("@!%#", "!\"£$%^&*()", alice)
        entities = index1.get("@!%#", "!\"£$%^&*()")
        self.assertIsNotNone(entities)
        self.assertTrue(isinstance(entities, list))
        self.assertEqual(1, len(entities))
        self.assertEqual(alice, entities[0])

    def test_add_multiple_nodes_to_index(self):
        index1 = self.graph_db.get_or_create_index(neo4j.Node, "index1")
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
        index1 = self.graph_db.get_or_create_index(neo4j.Node, "index1")
        index1.remove("surname", "Smith")
        alice = index1.get_or_create("surname", "Smith", {"name": "Alice Smith"})
        self.assertIsNotNone(alice)
        self.assertTrue(isinstance(alice, neo4j.Node))
        self.assertEqual("Alice Smith", alice["name"])
        alice_id = alice.id
        for i in range(10):
            alice = index1.get_or_create("surname", "Smith", {"name": "Alice Smith"})
            self.assertIsNotNone(alice)
            self.assertTrue(isinstance(alice, neo4j.Node))
            self.assertEqual("Alice Smith", alice["name"])
            self.assertEqual(alice_id, alice.id)

    def test_create_if_none(self):
        index1 = self.graph_db.get_or_create_index(neo4j.Node, "index1")
        index1.remove("surname", "Smith")
        alice = index1.create_if_none("surname", "Smith", {"name": "Alice Smith"})
        self.assertIsNotNone(alice)
        self.assertTrue(isinstance(alice, neo4j.Node))
        self.assertEqual("Alice Smith", alice["name"])
        for i in range(10):
            # subsequent calls fail as entity already exists
            alice = index1.create_if_none("surname", "Smith", {"name": "Alice Smith"})
            self.assertIsNone(alice)

    def test_add_node_if_none(self):
        index1 = self.graph_db.get_or_create_index(neo4j.Node, "index1")
        index1.remove("surname", "Smith")
        alice, bob = self.graph_db.create(
            {"name": "Alice Smith"}, {"name": "Bob Smith"}
        )
        # add Alice to the index - this should be successful
        result = index1.add_if_none("surname", "Smith", alice)
        self.assertEqual(alice, result)
        entities = index1.get("surname", "Smith")
        self.assertIsNotNone(entities)
        self.assertTrue(isinstance(entities, list))
        self.assertEqual(1, len(entities))
        self.assertEqual(alice, entities[0])
        # add Bob to the index - this should fail as Alice is already there
        result = index1.add_if_none("surname", "Smith", bob)
        self.assertIsNone(result)
        entities = index1.get("surname", "Smith")
        self.assertIsNotNone(entities)
        self.assertTrue(isinstance(entities, list))
        self.assertEqual(1, len(entities))
        self.assertEqual(alice, entities[0])

    def test_node_index_query(self):
        index1 = self.graph_db.get_or_create_index(neo4j.Node, "index1")
        index1.remove("colour", "red")
        index1.remove("colour", "green")
        index1.remove("colour", "blue")
        red, green, blue = self.graph_db.create({}, {}, {})
        index1.add("colour", "red", red)
        index1.add("colour", "green", green)
        index1.add("colour", "blue", blue)
        colours_containing_R = index1.query("colour:*r*")
        self.assertTrue(red in colours_containing_R)
        self.assertTrue(green in colours_containing_R)
        self.assertFalse(blue in colours_containing_R)

    def test_remove_node_from_index(self):
        index2 = self.graph_db.get_or_create_index(neo4j.Node, 'index2')
        node = self.graph_db.create({'email': 'rob@test.com'})[0]
        index2.add('email', 'rob@test.com', node)
        result = index2.get('email', 'rob@test.com')
        self.assertTrue(len(result) == 1)
        index2.remove_node(node)
        result = index2.get('email', 'rob@test.com')
        self.assertTrue(len(result) == 0)

if __name__ == '__main__':
    unittest.main()

