#!/usr/bin/env python
# -*- encoding: utf-8 -*-

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


from py2neo import Node
from py2neo.batch.write import WriteBatch
from test.cases import DatabaseTestCase


class WriteBatchTestCase(DatabaseTestCase):
    
    def setUp(self):
        self.batch = WriteBatch(self.graph)
    
    def test_cannot_create_with_bad_type(self):
        try:
            self.batch.create("")
        except TypeError:
            assert True
        else:
            assert False
        
    def test_cannot_create_with_none(self):
        try:
            self.batch.create(None)
        except TypeError:
            assert True
        else:
            assert False
        
    def test_can_create_path_with_new_nodes(self):
        self.batch.create_path({"name": "Alice"}, "KNOWS", {"name": "Bob"})
        results = self.batch.submit()
        path = results[0]
        assert len(path) == 1
        assert path.nodes[0]["name"] == "Alice"
        assert path.relationships[0].type == "KNOWS"
        assert path.nodes[1]["name"] == "Bob"
        
    def test_can_create_path_with_existing_nodes(self):
        alice, bob = self.graph.create({"name": "Alice"}, {"name": "Bob"})
        self.batch.create_path(alice, "KNOWS", bob)
        results = self.batch.submit()
        path = results[0]
        assert len(path) == 1
        assert path.nodes[0] == alice
        assert path.relationships[0].type == "KNOWS"
        assert path.nodes[1] == bob
        
    def test_path_creation_is_not_idempotent(self):
        alice, = self.graph.create({"name": "Alice"})
        self.batch.create_path(alice, "KNOWS", {"name": "Bob"})
        results = self.batch.submit()
        path = results[0]
        bob = path.nodes[1]
        assert path.nodes[0] == alice
        assert bob["name"] == "Bob"
        self.batch = WriteBatch(self.graph)
        self.batch.create_path(alice, "KNOWS", {"name": "Bob"})
        results = self.batch.submit()
        path = results[0]
        assert path.nodes[0] == alice
        assert path.nodes[1] != bob
        
    def test_can_get_or_create_path_with_existing_nodes(self):
        alice, bob = self.graph.create({"name": "Alice"}, {"name": "Bob"})
        self.batch.get_or_create_path(alice, "KNOWS", bob)
        results = self.batch.submit()
        path = results[0]
        assert len(path) == 1
        assert path.nodes[0] == alice
        assert path.relationships[0].type == "KNOWS"
        assert path.nodes[1] == bob
        
    def test_path_merging_is_idempotent(self):
        alice, = self.graph.create({"name": "Alice"})
        self.batch.get_or_create_path(alice, "KNOWS", {"name": "Bob"})
        results = self.batch.submit()
        path = results[0]
        bob = path.nodes[1]
        assert path.nodes[0] == alice
        assert bob["name"] == "Bob"
        self.batch = WriteBatch(self.graph)
        self.batch.get_or_create_path(alice, "KNOWS", {"name": "Bob"})
        results = self.batch.submit()
        path = results[0]
        assert path.nodes[0] == alice
        assert path.nodes[1] == bob
        
    def test_can_set_property_on_preexisting_node(self):
        alice, = self.graph.create({"name": "Alice"})
        self.batch.set_property(alice, "age", 34)
        self.batch.run()
        alice.pull()
        assert alice["age"] == 34
        
    def test_can_set_property_on_node_in_same_batch(self):
        alice = self.batch.create({"name": "Alice"})
        self.batch.set_property(alice, "age", 34)
        results = self.batch.submit()
        alice = results[self.batch.find(alice)]
        alice.auto_sync_properties = True
        assert alice["age"] == 34
        
    def test_can_set_properties_on_preexisting_node(self):
        alice, = self.graph.create({})
        self.batch.set_properties(alice, {"name": "Alice", "age": 34})
        self.batch.run()
        alice.pull()
        assert alice["name"] == "Alice"
        assert alice["age"] == 34
        
    def test_can_set_properties_on_node_in_same_batch(self):
        alice = self.batch.create({})
        self.batch.set_properties(alice, {"name": "Alice", "age": 34})
        results = self.batch.submit()
        alice = results[self.batch.find(alice)]
        alice.auto_sync_properties = True
        assert alice["name"] == "Alice"
        assert alice["age"] == 34
        
    def test_can_delete_property_on_preexisting_node(self):
        alice, = self.graph.create({"name": "Alice", "age": 34})
        self.batch.delete_property(alice, "age")
        self.batch.run()
        alice.pull()
        assert alice["name"] == "Alice"
        assert alice["age"] is None
        
    def test_can_delete_property_on_node_in_same_batch(self):
        alice = self.batch.create({"name": "Alice", "age": 34})
        self.batch.delete_property(alice, "age")
        results = self.batch.submit()
        alice = results[self.batch.find(alice)]
        alice.auto_sync_properties = True
        assert alice["name"] == "Alice"
        assert alice["age"] is None    
    
    def test_can_delete_properties_on_preexisting_node(self):
        alice, = self.graph.create({"name": "Alice", "age": 34})
        self.batch.delete_properties(alice)
        self.batch.run()
        alice.pull()
        assert alice.properties == {}
        
    def test_can_delete_properties_on_node_in_same_batch(self):
        alice = self.batch.create({"name": "Alice", "age": 34})
        self.batch.delete_properties(alice)
        results = self.batch.submit()
        alice = results[self.batch.find(alice)]
        alice.pull()
        assert alice.properties == {}
        
    def test_can_add_labels_to_preexisting_node(self):
        alice, = self.graph.create({"name": "Alice"})
        self.batch.add_labels(alice, "human", "female")
        self.batch.run()
        alice.pull()
        assert alice.labels == {"human", "female"}
        
    def test_can_add_labels_to_node_in_same_batch(self):
        a = self.batch.create({"name": "Alice"})
        self.batch.add_labels(a, "human", "female")
        results = self.batch.submit()
        alice = results[self.batch.find(a)]
        alice.pull()
        assert alice.labels == {"human", "female"}
        
    def test_can_remove_labels_from_preexisting_node(self):
        alice, = self.graph.create(Node("human", "female", name="Alice"))
        self.batch.remove_label(alice, "human")
        self.batch.run()
        alice.pull()
        assert alice.labels == {"female"}    
    
    def test_can_add_and_remove_labels_on_node_in_same_batch(self):
        alice = self.batch.create({"name": "Alice"})
        self.batch.add_labels(alice, "human", "female")
        self.batch.remove_label(alice, "female")
        results = self.batch.submit()
        alice = results[self.batch.find(alice)]
        alice.pull()
        assert alice.labels == {"human"}
        
    def test_can_set_labels_on_preexisting_node(self):
        alice, = self.graph.create(Node("human", "female", name="Alice"))
        self.batch.set_labels(alice, "mystery", "badger")
        self.batch.run()
        alice.pull()
        assert alice.labels == {"mystery", "badger"}
        
    def test_can_set_labels_on_node_in_same_batch(self):
        self.batch.create({"name": "Alice"})
        self.batch.add_labels(0, "human", "female")
        self.batch.set_labels(0, "mystery", "badger")
        results = self.batch.submit()
        alice = results[0]
        alice.pull()
        assert alice.labels == {"mystery", "badger"}
