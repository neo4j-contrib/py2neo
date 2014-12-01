#/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright 2011-2014, Nigel Small
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


import pytest

from py2neo.core import Node, Relationship
from py2neo.legacy import LegacyWriteBatch


class TestIndexedNodeCreation(object):

    @pytest.fixture(autouse=True)
    def setup(self, graph):
        try:
            graph.legacy.delete_index(Node, "People")
        except LookupError:
            pass
        self.people = graph.legacy.get_or_create_index(Node, "People")
        self.batch = LegacyWriteBatch(graph)
        self.graph = graph

    def test_can_create_single_indexed_node(self):
        properties = {"name": "Alice Smith"}
        # need to execute a pair of commands as "create in index" not available
        self.batch.create(properties)
        self.batch.add_to_index(Node, self.people, "surname", "Smith", 0)
        alice, index_entry = self.batch.submit()
        assert isinstance(alice, Node)
        assert alice.properties == properties
        self.graph.delete(alice)

    def test_can_create_two_similarly_indexed_nodes(self):
        # create Alice
        alice_props = {"name": "Alice Smith"}
        # need to execute a pair of commands as "create in index" not available
        self.batch.create(alice_props)
        self.batch.add_to_index(Node, self.people, "surname", "Smith", 0)
        alice, alice_index_entry = self.batch.submit()
        assert isinstance(alice, Node)
        assert alice.properties == alice_props
        self.batch.jobs = []
        # create Bob
        bob_props = {"name": "Bob Smith"}
        # need to execute a pair of commands as "create in index" not available
        self.batch.create(bob_props)
        self.batch.add_to_index(Node, self.people, "surname", "Smith", 0)
        bob, bob_index_entry = self.batch.submit()
        assert isinstance(bob, Node)
        assert bob.properties == bob_props
        # check entries
        smiths = self.people.get("surname", "Smith")
        assert len(smiths) == 2
        assert alice in smiths
        assert bob in smiths
        # done
        self.graph.delete(alice, bob)

    def test_can_get_or_create_uniquely_indexed_node(self):
        # create Alice
        alice_props = {"name": "Alice Smith"}
        self.batch.get_or_create_in_index(Node, self.people, "surname", "Smith", alice_props)
        alice, = self.batch.submit()
        assert isinstance(alice, Node)
        assert alice.properties == alice_props
        self.batch.jobs = []
        # create Bob
        bob_props = {"name": "Bob Smith"}
        self.batch.get_or_create_in_index(Node, self.people, "surname", "Smith", bob_props)
        bob, = self.batch.submit()
        assert isinstance(bob, Node)
        assert bob.properties != bob_props
        assert bob.properties == alice_props
        assert bob == alice
        # check entries
        smiths = self.people.get("surname", "Smith")
        assert len(smiths) == 1
        assert alice in smiths
        # done
        self.graph.delete(alice, bob)


class TestIndexedNodeAddition(object):

    @pytest.fixture(autouse=True)
    def setup(self, graph):
        try:
            graph.legacy.delete_index(Node, "People")
        except LookupError:
            pass
        self.people = graph.legacy.get_or_create_index(Node, "People")
        self.batch = LegacyWriteBatch(graph)
        self.graph = graph

    def test_can_add_single_node(self):
        alice, = self.graph.create({"name": "Alice Smith"})
        self.batch.add_to_index(Node, self.people, "surname", "Smith", alice)
        self.batch.run()
        # check entries
        smiths = self.people.get("surname", "Smith")
        assert len(smiths) == 1
        assert alice in smiths
        # done
        self.graph.delete(alice)

    def test_can_add_two_similar_nodes(self):
        alice, bob = self.graph.create(
            {"name": "Alice Smith"}, {"name": "Bob Smith"})
        self.batch.add_to_index(Node, self.people, "surname", "Smith", alice)
        self.batch.add_to_index(Node, self.people, "surname", "Smith", bob)
        nodes = self.batch.submit()
        assert nodes[0] != nodes[1]
        # check entries
        smiths = self.people.get("surname", "Smith")
        assert len(smiths) == 2
        assert alice in smiths
        assert bob in smiths
        # done
        self.graph.delete(alice, bob)

    def test_can_add_nodes_only_if_none_exist(self):
        alice, bob = self.graph.create(
            {"name": "Alice Smith"}, {"name": "Bob Smith"})
        self.batch.get_or_add_to_index(Node, self.people, "surname", "Smith", alice)
        self.batch.get_or_add_to_index(Node, self.people, "surname", "Smith", bob)
        nodes = self.batch.submit()
        assert nodes[0] == nodes[1]
        # check entries
        smiths = self.people.get("surname", "Smith")
        assert len(smiths) == 1
        assert alice in smiths
        # done
        self.graph.delete(alice, bob)


#class TestIndexedRelationshipCreation(object):
#
#    @pytest.fixture(autouse=True)
#    def setup(self, graph):
#        try:
#            graph.delete_index(Relationship, "friendships")
#        except LookupError:
#            pass
#        self.friendships = graph.get_or_create_index(Relationship, "Friendships")
#        self.alice, self.bob = graph.create({"name": "Alice"}, {"name": "Bob"})
#        self.batch = LegacyWriteBatch(graph)
#        self.graph = graph
#
#    def test_can_create_single_indexed_relationship(self, graph):
#        self.batch.get_or_create_indexed_relationship(
#            self.friendships, "friends", "alice_&_bob",
#            self.alice, "KNOWS", self.bob)
#        rels = self.batch.submit()
#        assert len(rels) == 1
#        assert isinstance(rels[0], Relationship)
#        assert rels[0].start_node == self.alice
#        assert rels[0].type == "KNOWS"
#        assert rels[0].end_node == self.bob
#        assert rels[0].properties == {}
#        graph.delete(rels)
#        graph.delete(self.alice, self.bob)
#
#    def test_can_get_or_create_uniquely_indexed_relationship(self, graph):
#        self.batch.get_or_create_indexed_relationship(
#            self.friendships, "friends", "alice_&_bob",
#            self.alice, "KNOWS", self.bob)
#        self.batch.get_or_create_indexed_relationship(
#            self.friendships, "friends", "alice_&_bob",
#            self.alice, "KNOWS", self.bob)
#        rels = self.batch.submit()
#        assert len(rels) == 2
#        assert isinstance(rels[0], Relationship)
#        assert isinstance(rels[1], Relationship)
#        assert rels[0] == rels[1]
#        graph.delete(rels)
#        graph.delete(self.alice, self.bob)


class TestIndexedRelationshipAddition(object):

    @pytest.fixture(autouse=True)
    def setup(self, graph):
        try:
            graph.legacy.delete_index(Relationship, "Friendships")
        except LookupError:
            pass
        self.friendships = graph.legacy.get_or_create_index(Relationship, "Friendships")
        self.batch = LegacyWriteBatch(graph)
        self.graph = graph

    def test_can_add_single_relationship(self, graph):
        alice, bob, ab = self.graph.create({"name": "Alice"}, {"name": "Bob"}, (0, "KNOWS", 1))
        self.batch.add_to_index(Relationship, self.friendships, "friends", "alice_&_bob", ab)
        self.batch.run()
        # check entries
        rels = self.friendships.get("friends", "alice_&_bob")
        assert len(rels) == 1
        assert ab in rels
        # done
        self.recycling = [ab, alice, bob]

    def test_can_add_two_similar_relationships(self, graph):
        alice, bob, ab1, ab2 = self.graph.create(
            {"name": "Alice"}, {"name": "Bob"},
            (0, "KNOWS", 1), (0, "KNOWS", 1))
        self.batch.add_to_index(Relationship, self.friendships, "friends", "alice_&_bob", ab1)
        self.batch.add_to_index(Relationship, self.friendships, "friends", "alice_&_bob", ab2)
        self.batch.run()
        # check entries
        entries = self.friendships.get("friends", "alice_&_bob")
        assert len(entries) == 2
        assert ab1 in entries
        assert ab2 in entries
        # done
        self.recycling = [ab1, ab2, alice, bob]

    def test_can_add_relationships_only_if_none_exist(self):
        alice, bob, ab1, ab2 = self.graph.create(
            {"name": "Alice"}, {"name": "Bob"},
            (0, "KNOWS", 1), (0, "KNOWS", 1))
        self.batch.get_or_add_to_index(Relationship, self.friendships,
                                       "friends", "alice_&_bob", ab1)
        self.batch.get_or_add_to_index(Relationship, self.friendships,
                                       "friends", "alice_&_bob", ab2)
        results = self.batch.submit()
        assert results[0] == results[1]
        # check entries
        entries = self.friendships.get("friends", "alice_&_bob")
        assert len(entries) == 1
        assert ab1 in entries
        # done
        self.recycling = [ab1, ab2, alice, bob]


class TestIndexedNodeRemoval(object):

    @pytest.fixture(autouse=True)
    def setup(self, graph):
        self.graph = graph
        try:
            graph.legacy.delete_index(Node, "node_removal_test_index")
        except LookupError:
            pass
        self.index = graph.legacy.get_or_create_index(Node, "node_removal_test_index")
        self.fred, self.wilma, = graph.create(
            {"name": "Fred Flintstone"}, {"name": "Wilma Flintstone"},
        )
        self.index.add("name", "Fred", self.fred)
        self.index.add("name", "Wilma", self.wilma)
        self.index.add("name", "Flintstone", self.fred)
        self.index.add("name", "Flintstone", self.wilma)
        self.index.add("flintstones", "%", self.fred)
        self.index.add("flintstones", "%", self.wilma)
        self.batch = LegacyWriteBatch(graph)

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
