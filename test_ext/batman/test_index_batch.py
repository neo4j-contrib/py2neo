#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2015, Nigel Small
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


from py2neo.types import Node, Relationship
from py2neo.ext.batman import ManualIndexWriteBatch
from .util import IndexTestCase


class IndexedNodeCreationTestCase(IndexTestCase):

    def setUp(self):
        try:
            self.index_manager.delete_index(Node, "People")
        except LookupError:
            pass
        self.people = self.index_manager.get_or_create_index(Node, "People")
        self.batch = ManualIndexWriteBatch(self.graph)

    def test_can_create_single_indexed_node(self):
        properties = {"name": "Alice Smith"}
        # need to execute a pair of commands as "create in index" not available
        self.batch.create(properties)
        self.batch.add_to_index(Node, self.people, "surname", "Smith", 0)
        alice, index_entry = self.batch.run()
        assert isinstance(alice, Node)
        assert dict(alice) == properties
        self.graph.delete(alice)

    def test_can_create_two_similarly_indexed_nodes(self):
        # create Alice
        alice_props = {"name": "Alice Smith"}
        # need to execute a pair of commands as "create in index" not available
        self.batch.create(alice_props)
        self.batch.add_to_index(Node, self.people, "surname", "Smith", 0)
        alice, alice_index_entry = self.batch.run()
        assert isinstance(alice, Node)
        assert dict(alice) == alice_props
        self.batch.jobs = []
        # create Bob
        bob_props = {"name": "Bob Smith"}
        # need to execute a pair of commands as "create in index" not available
        self.batch.create(bob_props)
        self.batch.add_to_index(Node, self.people, "surname", "Smith", 0)
        bob, bob_index_entry = self.batch.run()
        assert isinstance(bob, Node)
        assert dict(bob) == bob_props
        # check entries
        smiths = self.people.get("surname", "Smith")
        assert len(smiths) == 2
        assert alice in smiths
        assert bob in smiths
        # done
        self.graph.delete(alice | bob)

    def test_can_get_or_create_uniquely_indexed_node(self):
        # create Alice
        alice_props = {"name": "Alice Smith"}
        self.batch.get_or_create_in_index(Node, self.people, "surname", "Smith", alice_props)
        alice, = self.batch.run()
        assert isinstance(alice, Node)
        assert dict(alice) == alice_props
        self.batch.jobs = []
        # create Bob
        bob_props = {"name": "Bob Smith"}
        self.batch.get_or_create_in_index(Node, self.people, "surname", "Smith", bob_props)
        bob, = self.batch.run()
        assert isinstance(bob, Node)
        assert dict(bob) != bob_props
        assert dict(bob) == alice_props
        assert bob == alice
        # check entries
        smiths = self.people.get("surname", "Smith")
        assert len(smiths) == 1
        assert alice in smiths
        # done
        self.graph.delete(alice | bob)


class IndexedNodeAdditionTestCase(IndexTestCase):

    def setUp(self):
        try:
            self.index_manager.delete_index(Node, "People")
        except LookupError:
            pass
        self.people = self.index_manager.get_or_create_index(Node, "People")
        self.batch = ManualIndexWriteBatch(self.graph)

    def test_can_add_single_node(self):
        alice = Node(name="Alice Smith")
        self.graph.create(alice)
        self.batch.add_to_index(Node, self.people, "surname", "Smith", alice)
        self.batch.run()
        # check entries
        smiths = self.people.get("surname", "Smith")
        assert len(smiths) == 1
        assert alice in smiths
        # done
        self.graph.delete(alice)

    def test_can_add_two_similar_nodes(self):
        alice = Node(name="Alice Smith")
        bob = Node(name="Bob Smith")
        self.graph.create(alice | bob)
        self.batch.add_to_index(Node, self.people, "surname", "Smith", alice)
        self.batch.add_to_index(Node, self.people, "surname", "Smith", bob)
        nodes = self.batch.run()
        assert nodes[0] != nodes[1]
        # check entries
        smiths = self.people.get("surname", "Smith")
        assert len(smiths) == 2
        assert alice in smiths
        assert bob in smiths
        # done
        self.graph.delete(alice | bob)

    def test_can_add_nodes_only_if_none_exist(self):
        alice = Node(name="Alice Smith")
        bob = Node(name="Bob Smith")
        self.graph.create(alice | bob)
        self.batch.get_or_add_to_index(Node, self.people, "surname", "Smith", alice)
        self.batch.get_or_add_to_index(Node, self.people, "surname", "Smith", bob)
        nodes = self.batch.run()
        assert nodes[0] == nodes[1]
        # check entries
        smiths = self.people.get("surname", "Smith")
        assert len(smiths) == 1
        assert alice in smiths
        # done
        self.graph.delete(alice | bob)


class IndexedRelationshipAdditionTestCase(IndexTestCase):

    def setUp(self):
        try:
            self.index_manager.delete_index(Relationship, "Friendships")
        except LookupError:
            pass
        self.friendships = self.index_manager.get_or_create_index(Relationship, "Friendships")
        self.batch = ManualIndexWriteBatch(self.graph)

    def test_can_add_single_relationship(self):
        alice = Node(name="Alice Smith")
        bob = Node(name="Bob Smith")
        ab = Relationship(alice, "KNOWS", bob)
        self.graph.create(alice | bob | ab)
        self.batch.add_to_index(Relationship, self.friendships, "friends", "alice_&_bob", ab)
        self.batch.run()
        # check entries
        rels = self.friendships.get("friends", "alice_&_bob")
        assert len(rels) == 1
        assert ab in rels
        # done
        self.recycling = [ab, alice, bob]


class IndexedNodeRemovalTestCase(IndexTestCase):

    def setUp(self):
        try:
            self.index_manager.delete_index(Node, "node_removal_test_index")
        except LookupError:
            pass
        self.index = self.index_manager.get_or_create_index(Node, "node_removal_test_index")
        self.fred = Node(name="Fred Flintstone")
        self.wilma = Node(name="Wilma Flintstone")
        self.graph.create(self.fred | self.wilma)
        self.index.add("name", "Fred", self.fred)
        self.index.add("name", "Wilma", self.wilma)
        self.index.add("name", "Flintstone", self.fred)
        self.index.add("name", "Flintstone", self.wilma)
        self.index.add("flintstones", "%", self.fred)
        self.index.add("flintstones", "%", self.wilma)
        self.batch = ManualIndexWriteBatch(self.graph)

    def check(self, key, value, *entities):
        e = self.index.get(key, value)
        assert len(entities) == len(e)
        for entity in entities:
            assert entity in e

    def test_remove_key_value_entity(self):
        self.batch.remove_from_index(Node, self.index, key="name",
                                     value="Flintstone", entity=self.fred)
        self.batch.run()
        self.check("name", "Fred", self.fred)
        self.check("name", "Wilma", self.wilma)
        self.check("name", "Flintstone", self.wilma)
        self.check("flintstones", "%", self.fred, self.wilma)

    def test_remove_key_entity(self):
        self.batch.remove_from_index(Node, self.index, key="name", entity=self.fred)
        self.batch.run()
        self.check("name", "Fred")
        self.check("name", "Wilma", self.wilma)
        self.check("name", "Flintstone", self.wilma)
        self.check("flintstones", "%", self.fred, self.wilma)

    def test_remove_entity(self):
        self.batch.remove_from_index(Node, self.index, entity=self.fred)
        self.batch.run()
        self.check("name", "Fred")
        self.check("name", "Wilma", self.wilma)
        self.check("name", "Flintstone", self.wilma)
        self.check("flintstones", "%", self.wilma)
