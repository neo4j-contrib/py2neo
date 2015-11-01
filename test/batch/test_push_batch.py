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


from py2neo import Node, Rel
from py2neo.batch import BatchError
from test.cases import DatabaseTestCase


class PushBatchTestCase(DatabaseTestCase):
        
    def test_can_push_node(self):
        alice = Node(name="Alice")
        self.graph.create(alice)
        alice.properties["age"] = 33
        self.graph.push(alice)
        node_id = alice._id
        Node.cache.clear()
        node = self.graph.node(node_id)
        assert node.properties["age"] == 33
        
    def test_cannot_push_empty_list_property(self):
        alice = Node(name="Alice")
        self.graph.create(alice)
        alice.properties["faults"] = []
        with self.assertRaises(BatchError):
            self.graph.push(alice)
        
    def test_can_push_rel(self):
        a, b, ab = self.graph.create({}, {}, (0, "KNOWS", 1))
        rel = ab.rel
        rel.properties["since"] = 1999
        self.graph.push(rel)
        rel_id = rel._id
        Rel.cache.clear()
        rel = self.graph.relationship(rel_id).rel
        assert rel.properties["since"] == 1999
        
    def test_cannot_push_none(self):
        with self.assertRaises(TypeError):
            self.graph.push(None)
