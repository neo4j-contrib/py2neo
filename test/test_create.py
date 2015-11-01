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


from py2neo import Node, Rev, Relationship, Path
from test.cases import DatabaseTestCase


class CreateTestCase(DatabaseTestCase):

    def test_creating_nothing_does_nothing(self):
        created = self.graph.create()
        assert created == ()

    def test_can_create_node_from_dict(self):
        # given
        properties = {"name": "Alice"}
        # when
        created, = self.graph.create(properties)
        # then
        assert isinstance(created, Node)
        assert created.properties == properties
        assert created.bound

    def test_can_create_node_from_object(self):
        # given
        labels = {"Person"}
        properties = {"name": "Alice"}
        node = Node(*labels, **properties)
        # when
        created, = self.graph.create(node)
        # then
        assert isinstance(created, Node)
        assert created.labels == labels
        assert created.properties == properties
        assert created.bound

    def test_created_node_is_same_object(self):
        # given
        labels = {"Person"}
        properties = {"name": "Alice"}
        node = Node(*labels, **properties)
        # when
        created, = self.graph.create(node)
        # then
        assert created is node

    def test_can_update_already_bound_node(self):
        # given
        node, = self.graph.create({})
        # when
        created, = self.graph.create(node)
        # then
        assert created is node

    def test_can_create_relationship_from_tuple(self):
        # given
        nodes = self.graph.create({}, {})
        type_ = "KNOWS"
        properties = {"since": 1999}
        # when
        created, = self.graph.create((nodes[0], (type_, properties), nodes[1]))
        # then
        assert isinstance(created, Relationship)
        assert created.type == type_
        assert created.properties == properties
        assert created.bound

    def test_can_create_unique_relationship_from_tuple(self):
        # given
        nodes = self.graph.create({}, {})
        type_ = "KNOWS"
        properties = {"since": 1999}
        # when
        created, = self.graph.create_unique((nodes[0], (type_, properties), nodes[1]))
        # then
        assert isinstance(created, Relationship)
        assert created.type == type_
        assert created.properties == properties
        assert created.bound

    def test_can_create_relationship_from_object(self):
        # given
        nodes = self.graph.create({}, {})
        type_ = "KNOWS"
        properties = {"since": 1999}
        relationship = Relationship(nodes[0], (type_, properties), nodes[1])
        # when
        created, = self.graph.create(relationship)
        # then
        assert isinstance(created, Relationship)
        assert created.type == type_
        assert created.properties == properties
        assert created.bound

    def test_created_relationship_is_same_object(self):
        # given
        nodes = self.graph.create({}, {})
        type_ = "KNOWS"
        properties = {"since": 1999}
        relationship = Relationship(nodes[0], (type_, properties), nodes[1])
        # when
        created, = self.graph.create(relationship)
        # then
        assert created is relationship

    def test_can_create_nodes_within_relationship(self):
        # when
        created, = self.graph.create(Relationship(Node(), "KNOWS", Node()))
        # then
        assert isinstance(created, Relationship)
        assert created.bound
        assert created.start_node.bound
        assert created.end_node.bound

    def test_can_create_nodes_plus_relationship(self):
        # when
        a, b, ab = self.graph.create(Node(), Node(), Relationship(0, "KNOWS", 1))
        # then
        assert ab.start_node is a
        assert ab.end_node is b

    def test_can_update_already_bound_rel(self):
        # given
        _, _, rel = self.graph.create({}, {}, (0, "KNOWS", 1))
        # when
        created, = self.graph.create(rel)
        # then
        assert created is rel

    def test_can_create_path_from_object(self):
        # given
        nodes = self.graph.create({}, {}, {}, {})
        path = Path(nodes[0], "LOVES", nodes[1], Rev("HATES"), nodes[2], "KNOWS", nodes[3])
        # when
        created, = self.graph.create(path)
        # then
        assert isinstance(created, Path)
        assert created.nodes == nodes
        assert created.rels[0].type == "LOVES"
        assert created.rels[1].type == "HATES"
        assert isinstance(created.rels[1], Rev)
        assert created.rels[2].type == "KNOWS"

    def test_cannot_create_entity_of_other_castable_type(self):
        with self.assertRaises(TypeError):
            self.graph.create(None)

    def test_node_pointer_must_point_to_a_node(self):
        with self.assertRaises(ValueError):
            self.graph.create({}, {}, (0, "KNOWS", 1), (0, "KNOWS", 2))
