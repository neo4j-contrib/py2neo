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


from py2neo.core import Node, Relationship, Path
from py2neo.status import CypherError
from test.util import Py2neoTestCase


class DeleteTestCase(Py2neoTestCase):

    def test_can_delete_node(self):
        alice = Node("Person", name="Alice")
        self.graph.create(alice)
        assert self.graph.exists(alice)
        self.graph.delete(alice)
        assert not self.graph.exists(alice)
        
    def test_can_delete_nodes_and_relationship_rel_first(self):
        alice = Node("Person", name="Alice")
        bob = Node("Person", name="Bob")
        ab = Relationship(alice, "KNOWS", bob)
        self.graph.create(alice | bob | ab)
        assert self.graph.exists(alice, bob, ab)
        self.graph.delete(ab | alice | bob)
        assert not self.graph.exists(alice, bob, ab)

    def test_can_delete_nodes_and_relationship_nodes_first(self):
        alice = Node("Person", name="Alice")
        bob = Node("Person", name="Bob")
        ab = Relationship(alice, "KNOWS", bob)
        self.graph.create(alice | bob | ab)
        assert self.graph.exists(alice, bob, ab)
        self.graph.delete(alice | bob | ab)
        assert not self.graph.exists(alice, bob, ab)

    def test_cannot_delete_related_node(self):
        alice = Node("Person", name="Alice")
        bob = Node("Person", name="Bob")
        ab = Relationship(alice, "KNOWS", bob)
        self.graph.create(alice | bob | ab)
        assert self.graph.exists(alice, bob, ab)
        with self.assertRaises(CypherError):
            self.graph.delete(alice)
        self.graph.delete(alice | bob | ab)
        
    def test_can_delete_path(self):
        alice, bob, carol, dave = Node(), Node(), Node(), Node()
        path = Path(alice, "LOVES", bob, Relationship(carol, "HATES", bob), carol, "KNOWS", dave)
        self.graph.create(path)
        assert self.graph.exists(path)
        self.graph.delete(path)
        assert not self.graph.exists(path)
        
    def test_cannot_delete_other_types(self):
        with self.assertRaises(TypeError):
            self.graph.delete("not a node or a relationship")
