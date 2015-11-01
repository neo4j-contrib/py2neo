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


from py2neo import Node, NodePointer, JoinError
from py2neo.core import coalesce
from test.cases import DatabaseTestCase


class CastTestCase(DatabaseTestCase):

    def setUp(self):
        self.alice = Node(name="Alice")
        self.bob = Node(name="Bob")
        self.pointer_1 = NodePointer(1)
        self.pointer_2 = NodePointer(2)
        
    def test_can_coalesce_none_and_none(self):
        assert coalesce(None, None) is None
        
    def test_can_coalesce_none_and_node(self):
        assert coalesce(None, self.alice) is self.alice
        
    def test_can_coalesce_node_and_none(self):
        assert coalesce(self.alice, None) is self.alice
        
    def test_can_coalesce_same_nodes(self):
        assert coalesce(self.alice, self.alice) is self.alice
        
    def test_can_coalesce_similar_bound_nodes(self):
        self.alice.bind("http://localhost:7474/db/data/node/1")
        Node.cache.clear()
        alice_2 = Node(name="Alice")
        alice_2.bind(self.alice.uri)
        assert coalesce(self.alice, alice_2) == self.alice
    
    def test_cannot_coalesce_different_nodes(self):
        with self.assertRaises(JoinError):
            coalesce(self.alice, self.bob)
    
    def test_can_coalesce_none_and_pointer(self):
        assert coalesce(None, self.pointer_1) is self.pointer_1
        
    def test_can_coalesce_pointer_and_node(self):
        assert coalesce(self.pointer_1, None) is self.pointer_1
        
    def test_can_coalesce_same_pointers(self):
        assert coalesce(self.pointer_1, self.pointer_1) is self.pointer_1
        
    def test_can_coalesce_equal_pointers(self):
        assert coalesce(self.pointer_1, NodePointer(self.pointer_1.address)) == self.pointer_1
        
    def test_cannot_coalesce_different_pointers(self):
        with self.assertRaises(JoinError):
            coalesce(self.pointer_1, self.pointer_2)
        
    def test_cannot_coalesce_node_and_pointer(self):
        with self.assertRaises(JoinError):
            coalesce(self.alice, self.pointer_2)
        
    def test_cannot_coalesce_other_types(self):
        foo = "foo"
        with self.assertRaises(TypeError):
            coalesce(foo, foo)
        
    def test_cannot_coalesce_one_of_other_type(self):
        foo = "foo"
        with self.assertRaises(TypeError):
            coalesce(self.alice, foo)
