#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2016, Nigel Small
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


from py2neo import Node
from py2neo.ext.batman import ManualIndex
from .util import IndexTestCase


class CreationAndDeletionTestCase(IndexTestCase):

    def test_can_create_index_object_with_colon_in_name(self):
        uri = 'http://localhost:7474/db/data/index/node/foo%3Abar/{key}/{value}'
        ManualIndex(Node, uri)

    def test_can_delete_create_and_delete_index(self):
        try:
            self.index_manager.delete_index(Node, "foo")
        except LookupError:
            pass
        foo = self.index_manager.get_index(Node, "foo")
        assert foo is None
        foo = self.index_manager.get_or_create_index(Node, "foo")
        assert foo is not None
        assert isinstance(foo, ManualIndex)
        assert foo.name == "foo"
        assert foo.content_type is Node
        self.index_manager.delete_index(Node, "foo")
        foo = self.index_manager.get_index(Node, "foo")
        assert foo is None

    def test_can_delete_create_and_delete_index_with_colon_in_name(self):
        try:
            self.index_manager.delete_index(Node, "foo:bar")
        except LookupError:
            pass
        foo = self.index_manager.get_index(Node, "foo:bar")
        assert foo is None
        foo = self.index_manager.get_or_create_index(Node, "foo:bar")
        assert foo is not None
        assert isinstance(foo, ManualIndex)
        assert foo.name == "foo:bar"
        assert foo.content_type is Node
        self.index_manager.delete_index(Node, "foo:bar")
        foo = self.index_manager.get_index(Node, "foo:bar")
        assert foo is None


class NodeIndexTestCase(IndexTestCase):

    def setUp(self):
        self.index = self.index_manager.get_or_create_index(Node, "node_test_index")

    def test_add_existing_node_to_index(self):
        alice = Node(name="Alice Smith")
        self.graph.create(alice)
        self.index.add("surname", "Smith", alice)
        entities = self.index.get("surname", "Smith")
        assert entities is not None
        assert isinstance(entities, list)
        assert len(entities) == 1
        assert entities[0] == alice
        self.graph.delete(alice)

    def test_add_existing_node_to_index_with_spaces_in_key_and_value(self):
        alice = Node(name="Alice von Schmidt")
        self.graph.create(alice)
        self.index.add("family name", "von Schmidt", alice)
        entities = self.index.get("family name", "von Schmidt")
        assert entities is not None
        assert isinstance(entities, list)
        assert len(entities) == 1
        assert entities[0] == alice
        self.graph.delete(alice)

    def test_add_existing_node_to_index_with_odd_chars_in_key_and_value(self):
        alice = Node(name="Alice Smith")
        self.graph.create(alice)
        self.index.add("@!%#", "!\"$%^&*()", alice)
        entities = self.index.get("@!%#", "!\"$%^&*()")
        assert entities is not None
        assert isinstance(entities, list)
        assert len(entities) == 1
        assert entities[0] == alice
        self.graph.delete(alice)

    def test_add_existing_node_to_index_with_slash_in_key(self):
        node = Node(foo="bar")
        self.graph.create(node)
        key = "foo/bar"
        value = "bar"
        self.index.add(key, value, node)
        entities = self.index.get(key, value)
        assert entities is not None
        assert isinstance(entities, list)
        assert len(entities) == 1
        assert entities[0] == node
        self.graph.delete(node)

    def test_add_existing_node_to_index_with_slash_in_value(self):
        node = Node(foo="bar")
        self.graph.create(node)
        key = "foo"
        value = "foo/bar"
        self.index.add(key, value, node)
        entities = self.index.get(key, value)
        assert entities is not None
        assert isinstance(entities, list)
        assert len(entities) == 1
        assert entities[0] == node
        self.graph.delete(node)

    def test_add_multiple_existing_nodes_to_index_under_same_key_and_value(self):
        alice = Node(name="Alice Smith")
        bob = Node(name="Bob Smith")
        carol = Node(name="Carol Smith")
        self.graph.create(alice | bob | carol)
        self.index.add("surname", "Smith", alice)
        self.index.add("surname", "Smith", bob)
        self.index.add("surname", "Smith", carol)
        entities = self.index.get("surname", "Smith")
        assert entities is not None
        assert isinstance(entities, list)
        assert len(entities) == 3
        for entity in entities:
            assert entity in (alice, bob, carol)
        self.graph.delete(alice | bob | carol)

    def test_create_node(self):
        alice = self.index.create("surname", "Smith", {"name": "Alice Smith"})
        assert alice is not None
        assert isinstance(alice, Node)
        assert alice["name"] == "Alice Smith"
        smiths = self.index.get("surname", "Smith")
        assert alice in smiths
        self.graph.delete(alice)

    def test_get_or_create_node(self):
        alice = self.index.get_or_create("surname", "Smith", {"name": "Alice Smith"})
        assert alice is not None
        assert isinstance(alice, Node)
        assert alice["name"] == "Alice Smith"
        alice_id = alice.remote._id
        for i in range(10):
            # subsequent calls return the same object as node already exists
            alice = self.index.get_or_create("surname", "Smith", {"name": "Alice Smith"})
            assert alice is not None
            assert isinstance(alice, Node)
            assert alice["name"] == "Alice Smith"
            assert alice_id == alice.remote._id
        self.graph.delete(alice)

    def test_create_if_none(self):
        alice = self.index.create_if_none("surname", "Smith", {"name": "Alice Smith"})
        assert alice is not None
        assert isinstance(alice, Node)
        assert alice["name"] == "Alice Smith"
        for i in range(10):
            # subsequent calls fail as node already exists
            alice = self.index.create_if_none("surname", "Smith", {"name": "Alice Smith"})
            assert alice is None

    def test_add_node_if_none(self):
        for node in self.index.get("surname", "Smith"):
            self.graph.delete(node)
        alice = Node(name="Alice Smith")
        bob = Node(name="Bob Smith")
        self.graph.create(alice | bob)
        # add Alice to the index - this should be successful
        result = self.index.add_if_none("surname", "Smith", alice)
        assert result == alice
        entities = self.index.get("surname", "Smith")
        assert entities is not None
        assert isinstance(entities, list)
        assert len(entities) == 1
        assert entities[0] == alice
        # add Bob to the index - this should fail as Alice is already there
        result = self.index.add_if_none("surname", "Smith", bob)
        assert result is None
        entities = self.index.get("surname", "Smith")
        assert entities is not None
        assert isinstance(entities, list)
        assert len(entities) == 1
        assert entities[0] == alice
        self.graph.delete(alice | bob)

    def test_node_index_query(self):
        red = Node()
        green = Node()
        blue = Node()
        self.graph.create(red | green | blue)
        self.index.add("colour", "red", red)
        self.index.add("colour", "green", green)
        self.index.add("colour", "blue", blue)
        colours_containing_r = self.index.query("colour:*r*")
        assert red in colours_containing_r
        assert green in colours_containing_r
        assert blue not in colours_containing_r
        self.graph.delete(red | green | blue)

    def test_node_index_query_utf8(self):
        red = Node()
        green = Node()
        blue = Node()
        self.graph.create(red | green | blue)
        self.index.add("colour", "красный", red)
        self.index.add("colour", "зеленый", green)
        self.index.add("colour", "синий", blue)
        colours_containing_r = self.index.query("colour:*ный*")
        assert red in colours_containing_r
        assert green in colours_containing_r
        assert blue not in colours_containing_r
        self.graph.delete(red | green | blue)


class RemovalTestCase(IndexTestCase):

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

    def check(self, key, value, *entities):
        e = self.index.get(key, value)
        assert len(entities) == len(e)
        for entity in entities:
            assert entity in e

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


class IndexedNodeTestCase(IndexTestCase):

    def test_get_or_create_indexed_node_with_int_property(self):
        fred = self.index_manager.get_or_create_indexed_node(
            index_name="person", key="name", value="Fred", properties={"level": 1})
        assert isinstance(fred, Node)
        assert fred["level"] == 1
        self.graph.delete(fred)
