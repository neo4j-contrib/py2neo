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

from py2neo import neo4j, node, rel

import logging
import unittest


logging.basicConfig(level=logging.DEBUG)


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
        self.graph_db = neo4j.GraphDatabaseService()
        self.graph_db.clear()
        self.batch = neo4j.WriteBatch(self.graph_db)

    def test_can_create_single_empty_node(self):
        self.batch.create(node())
        a, = self.batch.submit()
        assert isinstance(a, neo4j.Node)
        assert a.get_properties() == {}

    def test_can_create_multiple_nodes(self):
        self.batch.create({"name": "Alice"})
        self.batch.create(node({"name": "Bob"}))
        self.batch.create(node(name="Carol"))
        alice, bob, carol = self.batch.submit()
        assert isinstance(alice, neo4j.Node)
        assert isinstance(bob, neo4j.Node)
        assert isinstance(carol, neo4j.Node)
        assert alice["name"] == "Alice"
        assert bob["name"] == "Bob"
        assert carol["name"] == "Carol"


class TestRelationshipCreation(unittest.TestCase):

    def setUp(self):
        default_graph_db().clear()
        self.batch = neo4j.WriteBatch(default_graph_db())
        self.recycling = []

    def tearDown(self):
        recycle(*self.recycling)

    def test_can_create_relationship_with_new_nodes(self):
        self.batch.create({"name": "Alice"})
        self.batch.create({"name": "Bob"})
        self.batch.create((0, "KNOWS", 1))
        alice, bob, knows = self.batch.submit()
        assert isinstance(knows, neo4j.Relationship)
        assert knows.start_node == alice
        assert knows.type == "KNOWS"
        assert knows.end_node == bob
        assert knows.get_properties() == {}
        self.recycling = [knows, alice, bob]

    def test_can_create_relationship_with_new_indexed_nodes(self):
        self.batch.get_or_create_indexed_node("people", "name", "Alice", {"name": "Alice"})
        self.batch.get_or_create_indexed_node("people", "name", "Bob", {"name": "Bob"})
        self.batch.get_or_create_indexed_relationship("friendships", "names", "alice_bob", 0, "KNOWS", 1)
        #self.batch.create((0, "KNOWS", 1))
        alice, bob, knows = self.batch.submit()
        assert isinstance(knows, neo4j.Relationship)
        assert knows.start_node == alice
        assert knows.type == "KNOWS"
        assert knows.end_node == bob
        assert knows.get_properties() == {}
        self.recycling = [knows, alice, bob]

    def test_can_create_relationship_with_existing_nodes(self):
        self.batch.create({"name": "Alice"})
        self.batch.create({"name": "Bob"})
        alice, bob = self.batch.submit()
        self.batch.create((alice, "KNOWS", bob))
        knows, = self.batch.submit()
        assert isinstance(knows, neo4j.Relationship)
        assert knows.start_node == alice
        assert knows.type == "KNOWS"
        assert knows.end_node == bob
        assert knows.get_properties() == {}
        self.recycling = [knows, alice, bob]

    def test_can_create_relationship_with_existing_start_node(self):
        self.batch.create({"name": "Alice"})
        alice, = self.batch.submit()
        self.batch.create({"name": "Bob"})
        self.batch.create((alice, "KNOWS", 0))
        bob, knows = self.batch.submit()
        assert isinstance(knows, neo4j.Relationship)
        assert knows.start_node == alice
        assert knows.type == "KNOWS"
        assert knows.end_node == bob
        assert knows.get_properties() == {}
        self.recycling = [knows, alice, bob]

    def test_can_create_relationship_with_existing_end_node(self):
        self.batch.create({"name": "Bob"})
        bob, = self.batch.submit()
        self.batch.create({"name": "Alice"})
        self.batch.create((0, "KNOWS", bob))
        alice, knows = self.batch.submit()
        assert isinstance(knows, neo4j.Relationship)
        assert knows.start_node == alice
        assert knows.type == "KNOWS"
        assert knows.end_node == bob
        assert knows.get_properties() == {}
        self.recycling = [knows, alice, bob]

    def test_can_create_multiple_relationships(self):
        self.batch.create({"name": "Alice"})
        self.batch.create({"name": "Bob"})
        self.batch.create({"name": "Carol"})
        self.batch.create((0, "KNOWS", 1))
        self.batch.create((1, "KNOWS", 2))
        self.batch.create((2, "KNOWS", 0))
        alice, bob, carol, ab, bc, ca = self.batch.submit()
        for rel in [ab, bc, ca]:
            assert isinstance(rel, neo4j.Relationship)
            assert rel.type == "KNOWS"
        self.recycling = [ab, bc, ca, alice, bob, carol]

    def test_can_create_overlapping_relationships(self):
        self.batch.create({"name": "Alice"})
        self.batch.create({"name": "Bob"})
        self.batch.create((0, "KNOWS", 1))
        self.batch.create((0, "KNOWS", 1))
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
        self.batch.create({"name": "Alice"})
        self.batch.create({"name": "Bob"})
        self.batch.create((0, "KNOWS", 1, {"since": 2000}))
        alice, bob, knows = self.batch.submit()
        assert isinstance(knows, neo4j.Relationship)
        assert knows.start_node == alice
        assert knows.type == "KNOWS"
        assert knows.end_node == bob
        assert knows["since"] == 2000
        self.recycling = [knows, alice, bob]

    def test_create_function(self):
        self.batch.create(node(name="Alice"))
        self.batch.create(node(name="Bob"))
        self.batch.create(rel(0, "KNOWS", 1))
        alice, bob, ab = self.batch.submit()
        assert isinstance(alice, neo4j.Node)
        assert alice["name"] == "Alice"
        assert isinstance(bob, neo4j.Node)
        assert bob["name"] == "Bob"
        assert isinstance(ab, neo4j.Relationship)
        assert ab.start_node == alice
        assert ab.type == "KNOWS"
        assert ab.end_node == bob
        self.recycling = [ab, alice, bob]


class TestUniqueRelationshipCreation(unittest.TestCase):

    def setUp(self):
        self.batch = neo4j.WriteBatch(default_graph_db())
        self.recycling = []

    def tearDown(self):
        recycle(*self.recycling)

    def test_can_create_relationship_if_none_exists(self):
        self.batch.create({"name": "Alice"})
        self.batch.create({"name": "Bob"})
        alice, bob = self.batch.submit()
        self.batch.get_or_create_path(alice, ("KNOWS", {"since": 2000}), bob)
        path, = self.batch.submit()
        knows = path.relationships[0]
        assert isinstance(knows, neo4j.Relationship)
        assert knows.start_node == alice
        assert knows.type == "KNOWS"
        assert knows.end_node == bob
        assert knows["since"] == 2000
        self.recycling = [knows, alice, bob]

    def test_will_get_relationship_if_one_exists(self):
        self.batch.create({"name": "Alice"})
        self.batch.create({"name": "Bob"})
        alice, bob = self.batch.submit()
        self.batch.get_or_create_path(alice, ("KNOWS", {"since": 2000}), bob)
        self.batch.get_or_create_path(alice, ("KNOWS", {"since": 2000}), bob)
        path1, path2 = self.batch.submit()
        assert path1 == path2

    def test_will_fail_batch_if_more_than_one_exists(self):
        self.batch.create({"name": "Alice"})
        self.batch.create({"name": "Bob"})
        self.batch.create((0, "KNOWS", 1))
        self.batch.create((0, "KNOWS", 1))
        alice, bob, k1, k2 = self.batch.submit()
        self.batch.get_or_create_path(alice, "KNOWS", bob)
        try:
            path, = self.batch.submit()
            assert False
        except neo4j.BatchError:
            assert True

    def test_can_create_relationship_and_start_node(self):
        self.batch.create({"name": "Bob"})
        bob, = self.batch.submit()
        self.batch.get_or_create_path(None, "KNOWS", bob)
        path, = self.batch.submit()
        knows = path.relationships[0]
        alice = knows.start_node
        assert isinstance(knows, neo4j.Relationship)
        assert isinstance(alice, neo4j.Node)
        assert knows.type == "KNOWS"
        assert knows.end_node == bob
        self.recycling = [knows, alice, bob]

    def test_can_create_relationship_and_end_node(self):
        self.batch.create({"name": "Alice"})
        alice, = self.batch.submit()
        self.batch.get_or_create_path(alice, "KNOWS", None)
        path, = self.batch.submit()
        knows = path.relationships[0]
        bob = knows.end_node
        assert isinstance(knows, neo4j.Relationship)
        assert knows.start_node == alice
        assert knows.type == "KNOWS"
        assert isinstance(bob, neo4j.Node)
        self.recycling = [knows, alice, bob]


class TestDeletion(unittest.TestCase):

    def setUp(self):
        self.graph_db = neo4j.GraphDatabaseService()
        self.graph_db.clear()
        self.batch = neo4j.WriteBatch(self.graph_db)

    def test_can_delete_relationship_and_related_nodes(self):
        self.batch.create({"name": "Alice"})
        self.batch.create({"name": "Bob"})
        self.batch.create((0, "KNOWS", 1))
        alice, bob, ab = self.batch.submit()
        assert alice.exists()
        assert bob.exists()
        assert ab.exists()
        self.batch.delete(ab)
        self.batch.delete(alice)
        self.batch.delete(bob)
        self.batch.submit()
        assert not alice.exists()
        assert not bob.exists()
        assert not ab.exists()


class TestPropertyManagement(unittest.TestCase):

    def setUp(self):
        self.graph_db = default_graph_db()
        self.alice, self.bob, self.friends = self.graph_db.create(
            {"name": "Alice", "surname": "Allison"},
            {"name": "Bob", "surname": "Robertson"},
            (0, "KNOWS", 1, {"since": 2000}),
        )
        self.batch = neo4j.WriteBatch(self.graph_db)
        self.recycling = []

    def tearDown(self):
        recycle(*self.recycling)
        self.alice.delete_related()

    def _check_properties(self, entity, expected_properties):
        actual_properties = entity.get_properties()
        assert len(actual_properties) == len(expected_properties)
        for key, value in expected_properties.items():
            assert key in actual_properties
            assert str(actual_properties[key]) == str(value)

    def test_can_add_new_node_property(self):
        self.batch.set_property(self.alice, "age", 33)
        self.batch.submit()
        self._check_properties(self.alice, {"name": "Alice", "surname": "Allison", "age": 33})

    def test_can_overwrite_existing_node_property(self):
        self.batch.set_property(self.alice, "name", "Alison")
        self.batch.submit()
        self._check_properties(self.alice, {"name": "Alison", "surname": "Allison"})

    def test_can_replace_all_node_properties(self):
        props = {"full_name": "Alice Allison", "age": 33}
        self.batch.set_properties(self.alice, props)
        self.batch.submit()
        self._check_properties(self.alice, props)

    def test_can_add_delete_node_property(self):
        self.batch.delete_property(self.alice, "surname")
        self.batch.submit()
        self._check_properties(self.alice, {"name": "Alice"})

    def test_can_add_delete_all_node_properties(self):
        self.batch.delete_properties(self.alice)
        self.batch.submit()
        self._check_properties(self.alice, {})

    def test_can_add_new_relationship_property(self):
        self.batch.set_property(self.friends, "foo", "bar")
        self.batch.submit()
        self._check_properties(self.friends, {"since": 2000, "foo": "bar"})


class TestIndexedNodeCreation(unittest.TestCase):

    def setUp(self):
        self.graph_db = default_graph_db()
        self.people = self.graph_db.get_or_create_index(neo4j.Node, "People")
        self.batch = neo4j.WriteBatch(self.graph_db)
        self.recycling = []

    def tearDown(self):
        recycle(*self.recycling)
        self.graph_db.delete_index(neo4j.Node, "People")

    def test_can_create_single_indexed_node(self):
        properties = {"name": "Alice Smith"}
        # need to execute a pair of commands as "create in index" not available
        self.batch.create(properties)
        self.batch.add_indexed_node(self.people, "surname", "Smith", 0)
        alice, index_entry = self.batch.submit()
        assert isinstance(alice, neo4j.Node)
        assert alice.get_properties() == properties
        self.recycling = [alice]

    def test_can_create_two_similarly_indexed_nodes(self):
        # create Alice
        alice_props = {"name": "Alice Smith"}
        # need to execute a pair of commands as "create in index" not available
        self.batch.create(alice_props)
        self.batch.add_indexed_node(self.people, "surname", "Smith", 0)
        alice, alice_index_entry = self.batch.submit()
        assert isinstance(alice, neo4j.Node)
        assert alice.get_properties() == alice_props
        # create Bob
        bob_props = {"name": "Bob Smith"}
        # need to execute a pair of commands as "create in index" not available
        self.batch.create(bob_props)
        self.batch.add_indexed_node(self.people, "surname", "Smith", 0)
        bob, bob_index_entry = self.batch.submit()
        assert isinstance(bob, neo4j.Node)
        assert bob.get_properties() == bob_props
        # check entries
        smiths = self.people.get("surname", "Smith")
        assert len(smiths) == 2
        assert alice in smiths
        assert bob in smiths
        # done
        self.recycling = [alice, bob]

    def test_can_get_or_create_uniquely_indexed_node(self):
        # create Alice
        alice_props = {"name": "Alice Smith"}
        self.batch.get_or_create_indexed_node(self.people, "surname", "Smith", alice_props)
        alice, = self.batch.submit()
        assert isinstance(alice, neo4j.Node)
        assert alice.get_properties() == alice_props
        # create Bob
        bob_props = {"name": "Bob Smith"}
        self.batch.get_or_create_indexed_node(self.people, "surname", "Smith", bob_props)
        bob, = self.batch.submit()
        assert isinstance(bob, neo4j.Node)
        assert bob.get_properties() != bob_props
        assert bob.get_properties() == alice_props
        assert bob == alice
        # check entries
        smiths = self.people.get("surname", "Smith")
        assert len(smiths) == 1
        assert alice in smiths
        # done
        self.recycling = [alice, bob]

    def test_can_create_uniquely_indexed_node_or_fail(self):
        try:
            # create Alice
            alice_props = {"name": "Alice Smith"}
            self.batch.create_indexed_node_or_fail(self.people, "surname", "Smith", alice_props)
            alice, = self.batch.submit()
            assert isinstance(alice, neo4j.Node)
            assert alice.get_properties() == alice_props
            # create Bob
            try:
                bob_props = {"name": "Bob Smith"}
                self.batch.create_indexed_node_or_fail(self.people, "surname", "Smith", bob_props)
                self.batch.submit()
                assert False
            except neo4j.BatchError as err:
                assert True
            # check entries
            smiths = self.people.get("surname", "Smith")
            assert len(smiths) == 1
            assert alice in smiths
            # done
            self.recycling = [alice]
        except NotImplementedError:
            # uniqueness mode `create_or_fail` not available in server version
            assert True


class TestIndexedNodeAddition(unittest.TestCase):

    def setUp(self):
        self.graph_db = default_graph_db()
        self.people = self.graph_db.get_or_create_index(neo4j.Node, "People")
        self.batch = neo4j.WriteBatch(self.graph_db)
        self.recycling = []

    def tearDown(self):
        recycle(*self.recycling)
        self.graph_db.delete_index(neo4j.Node, "People")

    def test_can_add_single_node(self):
        alice, = self.graph_db.create({"name": "Alice Smith"})
        self.batch.add_indexed_node(self.people, "surname", "Smith", alice)
        self.batch.submit()
        # check entries
        smiths = self.people.get("surname", "Smith")
        assert len(smiths) == 1
        assert alice in smiths
        # done
        self.recycling = [alice]

    def test_can_add_two_similar_nodes(self):
        alice, bob = self.graph_db.create({"name": "Alice Smith"}, {"name": "Bob Smith"})
        self.batch.add_indexed_node(self.people, "surname", "Smith", alice)
        self.batch.add_indexed_node(self.people, "surname", "Smith", bob)
        nodes = self.batch.submit()
        assert nodes[0] != nodes[1]
        # check entries
        smiths = self.people.get("surname", "Smith")
        assert len(smiths) == 2
        assert alice in smiths
        assert bob in smiths
        # done
        self.recycling = [alice, bob]

    def test_can_add_nodes_only_if_none_exist(self):
        alice, bob = self.graph_db.create({"name": "Alice Smith"}, {"name": "Bob Smith"})
        self.batch.get_or_add_indexed_node(self.people, "surname", "Smith", alice)
        self.batch.get_or_add_indexed_node(self.people, "surname", "Smith", bob)
        nodes = self.batch.submit()
        assert nodes[0] == nodes[1]
        # check entries
        smiths = self.people.get("surname", "Smith")
        assert len(smiths) == 1
        assert alice in smiths
        # done
        self.recycling = [alice, bob]

    def test_can_add_nodes_or_fail(self):
        alice, bob = self.graph_db.create({"name": "Alice Smith"}, {"name": "Bob Smith"})
        try:
            self.batch.add_indexed_node_or_fail(self.people, "surname", "Smith", alice)
            self.batch.add_indexed_node_or_fail(self.people, "surname", "Smith", bob)
            try:
                self.batch.submit()
                assert False
            except neo4j.BatchError as err:
                assert True
        except NotImplementedError:
            pass
        self.recycling = [alice, bob]


class TestIndexedRelationshipCreation(unittest.TestCase):

    def setUp(self):
        self.graph_db = default_graph_db()
        self.friendships = self.graph_db.get_or_create_index(neo4j.Relationship, "Friendships")
        self.alice, self.bob = self.graph_db.create(
            {"name": "Alice"}, {"name": "Bob"}
        )
        self.batch = neo4j.WriteBatch(self.graph_db)
        self.recycling = []

    def tearDown(self):
        recycle(*self.recycling)
        recycle(self.alice, self.bob)
        self.graph_db.delete_index(neo4j.Relationship, "Friendships")

    def test_can_create_single_indexed_relationship(self):
        self.batch.get_or_create_indexed_relationship(self.friendships, "friends", "alice_&_bob", self.alice, "KNOWS", self.bob)
        rels = self.batch.submit()
        assert len(rels) == 1
        assert isinstance(rels[0], neo4j.Relationship)
        assert rels[0].start_node == self.alice
        assert rels[0].type == "KNOWS"
        assert rels[0].end_node == self.bob
        assert rels[0].get_properties() == {}
        self.recycling = rels

    def test_can_get_or_create_uniquely_indexed_relationship(self):
        self.batch.get_or_create_indexed_relationship(self.friendships, "friends", "alice_&_bob", self.alice, "KNOWS", self.bob)
        self.batch.get_or_create_indexed_relationship(self.friendships, "friends", "alice_&_bob", self.alice, "KNOWS", self.bob)
        rels = self.batch.submit()
        assert len(rels) == 2
        assert isinstance(rels[0], neo4j.Relationship)
        assert isinstance(rels[1], neo4j.Relationship)
        assert rels[0] == rels[1]
        self.recycling = rels

    def test_can_create_uniquely_indexed_relationship_or_fail(self):
        try:
            self.batch.create_indexed_relationship_or_fail(self.friendships, "friends", "alice_&_bob", self.alice, "KNOWS", self.bob)
            self.batch.create_indexed_relationship_or_fail(self.friendships, "friends", "alice_&_bob", self.alice, "KNOWS", self.bob)
            try:
                self.batch.submit()
                assert False
            except neo4j.BatchError as err:
                assert True
        except NotImplementedError:
            pass


class TestIndexedRelationshipAddition(unittest.TestCase):

    def setUp(self):
        self.graph_db = default_graph_db()
        self.friendships = self.graph_db.get_or_create_index(neo4j.Relationship, "Friendships")
        self.batch = neo4j.WriteBatch(self.graph_db)
        self.recycling = []

    def tearDown(self):
        recycle(*self.recycling)
        self.graph_db.delete_index(neo4j.Relationship, "Friendships")

    def test_can_add_single_relationship(self):
        alice, bob, ab = self.graph_db.create({"name": "Alice"}, {"name": "Bob"}, (0, "KNOWS", 1))
        self.batch.add_indexed_relationship(self.friendships, "friends", "alice_&_bob", ab)
        self.batch.submit()
        # check entries
        rels = self.friendships.get("friends", "alice_&_bob")
        assert len(rels) == 1
        assert ab in rels
        # done
        self.recycling = [ab, alice, bob]

    def test_can_add_two_similar_relationships(self):
        alice, bob, ab1, ab2 = self.graph_db.create({"name": "Alice"}, {"name": "Bob"}, (0, "KNOWS", 1), (0, "KNOWS", 1))
        self.batch.add_indexed_relationship(self.friendships, "friends", "alice_&_bob", ab1)
        self.batch.add_indexed_relationship(self.friendships, "friends", "alice_&_bob", ab2)
        self.batch.submit()
        # check entries
        entries = self.friendships.get("friends", "alice_&_bob")
        assert len(entries) == 2
        assert ab1 in entries
        assert ab2 in entries
        # done
        self.recycling = [ab1, ab2, alice, bob]

    def test_can_add_relationships_only_if_none_exist(self):
        alice, bob, ab1, ab2 = self.graph_db.create({"name": "Alice"}, {"name": "Bob"}, (0, "KNOWS", 1), (0, "KNOWS", 1))
        self.batch.get_or_add_indexed_relationship(self.friendships, "friends", "alice_&_bob", ab1)
        self.batch.get_or_add_indexed_relationship(self.friendships, "friends", "alice_&_bob", ab2)
        results = self.batch.submit()
        assert results[0] == results[1]
        # check entries
        entries = self.friendships.get("friends", "alice_&_bob")
        assert len(entries) == 1
        assert ab1 in entries
        # done
        self.recycling = [ab1, ab2, alice, bob]

    def test_can_add_relationships_or_fail(self):
        alice, bob, ab1, ab2 = self.graph_db.create({"name": "Alice"}, {"name": "Bob"}, (0, "KNOWS", 1), (0, "KNOWS", 1))
        try:
            self.batch.add_indexed_relationship_or_fail(self.friendships, "friends", "alice_&_bob", ab1)
            self.batch.add_indexed_relationship_or_fail(self.friendships, "friends", "alice_&_bob", ab2)
            try:
                self.batch.submit()
                assert False
            except neo4j.BatchError:
                assert True
        except NotImplementedError:
            pass
        self.recycling = [ab1, ab2, alice, bob]


class TestIndexedNodeRemoval(unittest.TestCase):

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
        self.batch = neo4j.WriteBatch(self.graph_db)

    def tearDown(self):
        self.graph_db.delete(self.fred, self.wilma)
        self.graph_db.delete_index(self.index.content_type, self.index.name)

    def check(self, key, value, *entities):
        e = self.index.get(key, value)
        self.assertEqual(len(entities), len(e))
        for entity in entities:
            self.assertTrue(entity in e)

    def test_remove_key_value_entity(self):
        self.batch.remove_indexed_node(self.index, key="name", value="Flintstone", node=self.fred)
        self.batch.submit()
        self.check("name", "Fred", self.fred)
        self.check("name", "Wilma", self.wilma)
        self.check("name", "Flintstone", self.wilma)
        self.check("flintstones", "%", self.fred, self.wilma)

    def test_remove_key_entity(self):
        self.batch.remove_indexed_node(self.index, key="name", node=self.fred)
        self.batch.submit()
        self.check("name", "Fred")
        self.check("name", "Wilma", self.wilma)
        self.check("name", "Flintstone", self.wilma)
        self.check("flintstones", "%", self.fred, self.wilma)

    def test_remove_entity(self):
        self.batch.remove_indexed_node(self.index, node=self.fred)
        self.batch.submit()
        self.check("name", "Fred")
        self.check("name", "Wilma", self.wilma)
        self.check("name", "Flintstone", self.wilma)
        self.check("flintstones", "%", self.wilma)


def test_can_use_return_values_as_references():
    batch = neo4j.WriteBatch(neo4j.GraphDatabaseService())
    a = batch.create(node(name="Alice"))
    b = batch.create(node(name="Bob"))
    batch.create(rel(a, "KNOWS", b))
    results = batch.submit()
    ab = results[2]
    assert isinstance(ab, neo4j.Relationship)
    assert ab.start_node["name"] == "Alice"
    assert ab.end_node["name"] == "Bob"


if __name__ == "__main__":
    unittest.main()
