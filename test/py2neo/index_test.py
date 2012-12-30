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

__author__    = "Nigel Small <nasmall@gmail.com>"
__copyright__ = "Copyright 2011-2012 Nigel Small"
__license__   = "Apache License, Version 2.0"

from py2neo import neo4j

import unittest


def default_graph_db():
    return neo4j.GraphDatabaseService("http://localhost:7474/db/data/")


class CreationAndDeletionTests(unittest.TestCase):

    def setUp(self):
        self.graph_db = default_graph_db()

    def test_can_delete_create_and_delete_index(self):
        self.graph_db.delete_index(neo4j.Node, "foo")
        foo = self.graph_db.get_index(neo4j.Node, "foo")
        self.assertTrue(foo is None)
        foo = self.graph_db.get_or_create_index(neo4j.Node, "foo")
        self.assertIsNotNone(foo)
        self.assertIsInstance(foo, neo4j.Index)
        self.assertEqual("foo", foo.name)
        self.assertEqual(neo4j.Node, foo.content_type)
        self.graph_db.delete_index(neo4j.Node, "foo")
        foo = self.graph_db.get_index(neo4j.Node, "foo")
        self.assertTrue(foo is None)


class NodeIndexTestCase(unittest.TestCase):

    def setUp(self):
        self.graph_db = default_graph_db()
        self.index = self.graph_db.get_or_create_index(neo4j.Node, "node_test_index")

    def tearDown(self):
        self.graph_db.delete_index(self.index.content_type, self.index.name)

    def test_add_existing_node_to_index(self):
        alice, = self.graph_db.create({"name": "Alice Smith"})
        self.index.add("surname", "Smith", alice)
        entities = self.index.get("surname", "Smith")
        self.assertIsNotNone(entities)
        self.assertTrue(isinstance(entities, list))
        self.assertEqual(1, len(entities))
        self.assertEqual(alice, entities[0])

    def test_add_existing_node_to_index_with_spaces_in_key_and_value(self):
        alice, = self.graph_db.create({"name": "Alice von Schmidt"})
        self.index.add("family name", "von Schmidt", alice)
        entities = self.index.get("family name", "von Schmidt")
        self.assertIsNotNone(entities)
        self.assertTrue(isinstance(entities, list))
        self.assertEqual(1, len(entities))
        self.assertEqual(alice, entities[0])

    def test_add_existing_node_to_index_with_odd_chars_in_key_and_value(self):
        alice, = self.graph_db.create({"name": "Alice Smith"})
        self.index.add("@!%#", "!\"£$%^&*()", alice)
        entities = self.index.get("@!%#", "!\"£$%^&*()")
        self.assertIsNotNone(entities)
        self.assertTrue(isinstance(entities, list))
        self.assertEqual(1, len(entities))
        self.assertEqual(alice, entities[0])

    def test_add_multiple_existing_nodes_to_index_under_same_key_and_value(self):
        alice, bob, carol = self.graph_db.create(
            {"name": "Alice Smith"},
            {"name": "Bob Smith"},
            {"name": "Carol Smith"}
        )
        self.index.add("surname", "Smith", alice)
        self.index.add("surname", "Smith", bob)
        self.index.add("surname", "Smith", carol)
        entities = self.index.get("surname", "Smith")
        self.assertIsNotNone(entities)
        self.assertTrue(isinstance(entities, list))
        self.assertEqual(3, len(entities))
        for entity in entities:
            self.assertTrue(entity in (alice, bob, carol))

    def test_create_node(self):
        alice = self.index.create("surname", "Smith", {"name": "Alice Smith"})
        self.assertIsNotNone(alice)
        self.assertTrue(isinstance(alice, neo4j.Node))
        self.assertEqual("Alice Smith", alice["name"])
        smiths = self.index.get("surname", "Smith")
        self.assertTrue(alice in smiths)

    def test_get_or_create_node(self):
        alice = self.index.get_or_create("surname", "Smith", {"name": "Alice Smith"})
        self.assertIsNotNone(alice)
        self.assertTrue(isinstance(alice, neo4j.Node))
        self.assertEqual("Alice Smith", alice["name"])
        alice_id = alice.id
        for i in range(10):
            # subsequent calls return the same object as node already exists
            alice = self.index.get_or_create("surname", "Smith", {"name": "Alice Smith"})
            self.assertIsNotNone(alice)
            self.assertTrue(isinstance(alice, neo4j.Node))
            self.assertEqual("Alice Smith", alice["name"])
            self.assertEqual(alice_id, alice.id)

    def test_create_if_none(self):
        alice = self.index.create_if_none("surname", "Smith", {"name": "Alice Smith"})
        self.assertIsNotNone(alice)
        self.assertTrue(isinstance(alice, neo4j.Node))
        self.assertEqual("Alice Smith", alice["name"])
        for i in range(10):
            # subsequent calls fail as node already exists
            alice = self.index.create_if_none("surname", "Smith", {"name": "Alice Smith"})
            self.assertIsNone(alice)

    def test_add_node_if_none(self):
        alice, bob = self.graph_db.create(
            {"name": "Alice Smith"}, {"name": "Bob Smith"}
        )
        # add Alice to the index - this should be successful
        result = self.index.add_if_none("surname", "Smith", alice)
        self.assertEqual(alice, result)
        entities = self.index.get("surname", "Smith")
        self.assertIsNotNone(entities)
        self.assertTrue(isinstance(entities, list))
        self.assertEqual(1, len(entities))
        self.assertEqual(alice, entities[0])
        # add Bob to the index - this should fail as Alice is already there
        result = self.index.add_if_none("surname", "Smith", bob)
        self.assertIsNone(result)
        entities = self.index.get("surname", "Smith")
        self.assertIsNotNone(entities)
        self.assertTrue(isinstance(entities, list))
        self.assertEqual(1, len(entities))
        self.assertEqual(alice, entities[0])

    def test_node_index_query(self):
        red, green, blue = self.graph_db.create({}, {}, {})
        self.index.add("colour", "red", red)
        self.index.add("colour", "green", green)
        self.index.add("colour", "blue", blue)
        colours_containing_R = self.index.query("colour:*r*")
        self.assertTrue(red in colours_containing_R)
        self.assertTrue(green in colours_containing_R)
        self.assertFalse(blue in colours_containing_R)


class RemovalTests(unittest.TestCase):

    def setUp(self):
        self.graph_db = default_graph_db()
        self.index = self.graph_db.get_or_create_index(neo4j.Node, "node_removal_test_index")
        self.fred, self.wilma, = self.graph_db.create(
            {"name": "Fred Flintstone"}, {"name": "Wilma Flintstone"},
        )
        self.index.add("name", "Fred", self.fred)
        self.index.add("name", "Wilma", self.wilma)
        self.index.add("name", "Flintstone", self.fred)
        self.index.add("name", "Flintstone", self.wilma)
        self.index.add("flintstones", "%", self.fred)
        self.index.add("flintstones", "%", self.wilma)

    def tearDown(self):
        self.graph_db.delete(self.fred, self.wilma)
        self.graph_db.delete_index(self.index.content_type, self.index.name)

    def check(self, key, value, *entities):
        e = self.index.get(key, value)
        self.assertEqual(len(entities), len(e))
        for entity in entities:
            self.assertTrue(entity in e)

    def test_remove_key_value_entity(self):
        self.index.remove(key="name", value="Flintstone", entity=self.fred)
        self.check("name", "Fred", self.fred)
        self.check("name", "Wilma", self.wilma)
        self.check("name", "Flintstone", self.wilma)
        self.check("flintstones", "%", self.fred, self.wilma)

    def test_remove_key_value(self):
        self.index.remove(key="name", value="Flintstone")
        self.check("name", "Fred", self.fred)
        self.check("name", "Wilma", self.wilma)
        self.check("name", "Flintstone")
        self.check("flintstones", "%", self.fred, self.wilma)

    def test_remove_key_entity(self):
        self.index.remove(key="name", entity=self.fred)
        self.check("name", "Fred")
        self.check("name", "Wilma", self.wilma)
        self.check("name", "Flintstone", self.wilma)
        self.check("flintstones", "%", self.fred, self.wilma)

    def test_remove_entity(self):
        self.index.remove(entity=self.fred)
        self.check("name", "Fred")
        self.check("name", "Wilma", self.wilma)
        self.check("name", "Flintstone", self.wilma)
        self.check("flintstones", "%", self.wilma)


class IndexedNodeTests(unittest.TestCase):

    def test_get_or_create_indexed_node_with_int_property(self):
        graph_db = neo4j.GraphDatabaseService()
        fred = graph_db.get_or_create_indexed_node(index="person", key="name", value="Fred", properties={"level" : 1})
        assert isinstance(fred, neo4j.Node)
        assert fred["level"] == 1
        graph_db.delete(fred)


if __name__ == '__main__':
    unittest.main()

