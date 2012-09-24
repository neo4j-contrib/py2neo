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

from py2neo import neo4j, rest

import unittest


def default_graph_db():
    return neo4j.GraphDatabaseService("http://localhost:7474/db/data/")

def recycle(*entities):
    for entity in entities:
        try:
            entity.delete()
        except Exception:
            pass


class TestNodeCreation(unittest.TestCase):

    def setUp(self):
        self.batch = neo4j.Batch(default_graph_db())
        self.recycling = []

    def tearDown(self):
        recycle(*self.recycling)

    def test_can_create_single_empty_node(self):
        self.batch.create_node()
        node, = self.batch.submit()
        assert isinstance(node, neo4j.Node)
        assert node.get_properties() == {}
        self.recycling = [node]

    def test_can_create_multiple_nodes(self):
        self.batch.create_node({"name": "Alice"})
        self.batch.create_node({"name": "Bob"})
        alice, bob = self.batch.submit()
        assert isinstance(alice, neo4j.Node)
        assert isinstance(bob, neo4j.Node)
        assert alice["name"] == "Alice"
        assert bob["name"] == "Bob"
        self.recycling = [alice, bob]


class TestRelationshipCreation(unittest.TestCase):

    def setUp(self):
        self.batch = neo4j.Batch(default_graph_db())
        self.recycling = []

    def tearDown(self):
        recycle(*self.recycling)

    def test_can_create_relationship_with_new_nodes(self):
        self.batch.create_node({"name": "Alice"})
        self.batch.create_node({"name": "Bob"})
        self.batch.create_relationship(0, "KNOWS", 1)
        alice, bob, knows = self.batch.submit()
        assert isinstance(knows, neo4j.Relationship)
        assert knows.start_node == alice
        assert knows.type == "KNOWS"
        assert knows.end_node == bob
        assert knows.get_properties() == {}
        self.recycling = [knows, alice, bob]

    def test_can_create_relationship_with_existing_nodes(self):
        self.batch.create_node({"name": "Alice"})
        self.batch.create_node({"name": "Bob"})
        alice, bob = self.batch.submit()
        self.batch.create_relationship(alice, "KNOWS", bob)
        knows, = self.batch.submit()
        assert isinstance(knows, neo4j.Relationship)
        assert knows.start_node == alice
        assert knows.type == "KNOWS"
        assert knows.end_node == bob
        assert knows.get_properties() == {}
        self.recycling = [knows, alice, bob]

    def test_can_create_relationship_with_existing_start_node(self):
        self.batch.create_node({"name": "Alice"})
        alice, = self.batch.submit()
        self.batch.create_node({"name": "Bob"})
        self.batch.create_relationship(alice, "KNOWS", 0)
        bob, knows = self.batch.submit()
        assert isinstance(knows, neo4j.Relationship)
        assert knows.start_node == alice
        assert knows.type == "KNOWS"
        assert knows.end_node == bob
        assert knows.get_properties() == {}
        self.recycling = [knows, alice, bob]

    def test_can_create_relationship_with_existing_end_node(self):
        self.batch.create_node({"name": "Bob"})
        bob, = self.batch.submit()
        self.batch.create_node({"name": "Alice"})
        self.batch.create_relationship(0, "KNOWS", bob)
        alice, knows = self.batch.submit()
        assert isinstance(knows, neo4j.Relationship)
        assert knows.start_node == alice
        assert knows.type == "KNOWS"
        assert knows.end_node == bob
        assert knows.get_properties() == {}
        self.recycling = [knows, alice, bob]

    def test_can_create_multiple_relationships(self):
        self.batch.create_node({"name": "Alice"})
        self.batch.create_node({"name": "Bob"})
        self.batch.create_node({"name": "Carol"})
        self.batch.create_relationship(0, "KNOWS", 1)
        self.batch.create_relationship(1, "KNOWS", 2)
        self.batch.create_relationship(2, "KNOWS", 0)
        alice, bob, carol, ab, bc, ca = self.batch.submit()
        for rel in [ab, bc, ca]:
            assert isinstance(rel, neo4j.Relationship)
            assert rel.type == "KNOWS"
        self.recycling = [ab, bc, ca, alice, bob, carol]

    def test_can_create_overlapping_relationships(self):
        self.batch.create_node({"name": "Alice"})
        self.batch.create_node({"name": "Bob"})
        self.batch.create_relationship(0, "KNOWS", 1)
        self.batch.create_relationship(0, "KNOWS", 1)
        alice, bob, knows1, knows2 = self.batch.submit()
        assert isinstance(knows1, neo4j.Relationship)
        assert knows1.start_node == alice
        assert knows1.type == "KNOWS"
        assert knows1.end_node == bob
        assert knows1.get_properties() == {}
        assert isinstance(knows2, neo4j.Relationship)
        assert knows2.start_node == alice
        assert knows2.type == "KNOWS"
        assert knows2.end_node == bob
        assert knows2.get_properties() == {}
        self.recycling = [knows1, knows2, alice, bob]

    def test_can_create_relationship_with_properties(self):
        self.batch.create_node({"name": "Alice"})
        self.batch.create_node({"name": "Bob"})
        self.batch.create_relationship(0, "KNOWS", 1, {"since": 2000})
        alice, bob, knows = self.batch.submit()
        assert isinstance(knows, neo4j.Relationship)
        assert knows.start_node == alice
        assert knows.type == "KNOWS"
        assert knows.end_node == bob
        assert knows["since"] == 2000
        self.recycling = [knows, alice, bob]


class TestUniqueRelationshipCreation(unittest.TestCase):

    def setUp(self):
        self.batch = neo4j.Batch(default_graph_db())
        self.recycling = []

    def tearDown(self):
        recycle(*self.recycling)

    def test_can_create_relationship_if_none_exists(self):
        self.batch.create_node({"name": "Alice"})
        self.batch.create_node({"name": "Bob"})
        alice, bob = self.batch.submit()
        self.batch.get_or_create_relationship(alice, "KNOWS", bob, {"since": 2000})
        knows, = self.batch.submit()
        assert isinstance(knows, neo4j.Relationship)
        assert knows.start_node == alice
        assert knows.type == "KNOWS"
        assert knows.end_node == bob
        assert knows["since"] == 2000
        self.recycling = [knows, alice, bob]

    def test_will_get_relationship_if_one_exists(self):
        self.batch.create_node({"name": "Alice"})
        self.batch.create_node({"name": "Bob"})
        alice, bob = self.batch.submit()
        self.batch.get_or_create_relationship(alice, "KNOWS", bob, {"since": 2000})
        self.batch.get_or_create_relationship(alice, "KNOWS", bob, {"since": 2000})
        knows1, knows2 = self.batch.submit()
        assert isinstance(knows1, neo4j.Relationship)
        assert knows1.start_node == alice
        assert knows1.type == "KNOWS"
        assert knows1.end_node == bob
        assert knows1["since"] == 2000
        assert knows2 == knows1
        self.recycling = [knows1, knows2, alice, bob]

    def test_will_fail_batch_if_more_than_one_exists(self):
        self.batch.create_node({"name": "Alice"})
        self.batch.create_node({"name": "Bob"})
        self.batch.create_relationship(0, "KNOWS", 1)
        self.batch.create_relationship(0, "KNOWS", 1)
        alice, bob, k1, k2 = self.batch.submit()
        self.batch.get_or_create_relationship(alice, "KNOWS", bob)
        try:
            knows, = self.batch.submit()
            self.recycling = [knows, k1, k2, alice, bob]
            assert False
        except rest.BadRequest as err:
            sys.stderr.write(err.exception + ": " + err.message + "\n")
            self.recycling = [k1, k2, alice, bob]
            assert True

    def test_can_create_relationship_and_start_node(self):
        self.batch.create_node({"name": "Bob"})
        bob, = self.batch.submit()
        self.batch.get_or_create_relationship(None, "KNOWS", bob)
        knows, = self.batch.submit()
        alice = knows.start_node
        assert isinstance(knows, neo4j.Relationship)
        assert isinstance(alice, neo4j.Node)
        assert knows.type == "KNOWS"
        assert knows.end_node == bob
        self.recycling = [knows, alice, bob]

    def test_can_create_relationship_and_end_node(self):
        self.batch.create_node({"name": "Alice"})
        alice, = self.batch.submit()
        self.batch.get_or_create_relationship(alice, "KNOWS", None)
        knows, = self.batch.submit()
        bob = knows.end_node
        assert isinstance(knows, neo4j.Relationship)
        assert knows.start_node == alice
        assert knows.type == "KNOWS"
        assert isinstance(bob, neo4j.Node)
        self.recycling = [knows, alice, bob]

    def test_cannot_create_relationship_and_both_nodes(self):
        try:
            self.batch.get_or_create_relationship(None, "KNOWS", None)
            assert False
        except ValueError as err:
            sys.stderr.write(repr(err) + "\n")
            assert True


class TestDeletion(unittest.TestCase):

    def setUp(self):
        self.batch = neo4j.Batch(default_graph_db())
        self.recycling = []

    def tearDown(self):
        recycle(*self.recycling)

    def test_can_delete_relationship_and_related_nodes(self):
        self.batch.create_node({"name": "Alice"})
        self.batch.create_node({"name": "Bob"})
        self.batch.create_relationship(0, "KNOWS", 1)
        alice, bob, knows = self.batch.submit()
        assert alice.exists()
        assert bob.exists()
        assert knows.exists()
        self.batch.delete(knows)
        self.batch.delete(alice)
        self.batch.delete(bob)
        self.batch.submit()
        assert not alice.exists()
        assert not bob.exists()
        assert not knows.exists()


class TestPropertyManagement(unittest.TestCase):

    def setUp(self):
        self.batch = neo4j.Batch(default_graph_db())
        self.batch.create_node({"name": "Alice", "surname": "Allison"})
        self.alice, = self.batch.submit()
        self.recycling = []

    def tearDown(self):
        recycle(*self.recycling)
        self.alice.delete()

    def _check_properties(self, entity, expected_properties):
        actual_properties = entity.get_properties()
        assert len(actual_properties) == len(expected_properties)
        for key, value in expected_properties.items():
            assert key in actual_properties
            assert str(actual_properties[key]) == str(value)

    def test_can_add_new_property(self):
        self.batch.set_property(self.alice, "age", 33)
        self.batch.submit()
        self._check_properties(self.alice, {"name": "Alice", "surname": "Allison", "age": 33})

    def test_can_overwrite_existing_property(self):
        self.batch.set_property(self.alice, "name", "Alison")
        self.batch.submit()
        self._check_properties(self.alice, {"name": "Alison", "surname": "Allison"})

    def test_can_replace_all_properties(self):
        props = {"full_name": "Alice Allison", "age": 33}
        self.batch.set_properties(self.alice, props)
        self.batch.submit()
        self._check_properties(self.alice, props)

    def test_can_add_delete_property(self):
        self.batch.delete_property(self.alice, "surname")
        self.batch.submit()
        self._check_properties(self.alice, {"name": "Alice"})

    def test_can_add_delete_all_properties(self):
        self.batch.delete_properties(self.alice)
        self.batch.submit()
        self._check_properties(self.alice, {})
