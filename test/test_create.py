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


from py2neo import Node, Relationship, Path
from test.util import Py2neoTestCase


class CreateTestCase(Py2neoTestCase):

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
        assert dict(created) == properties
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
        assert created.labels() == labels
        assert dict(created) == properties
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
        assert created.type() == type_
        assert dict(created) == properties
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
        assert created.type() == type_
        assert dict(created) == properties
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
        assert created.type() == type_
        assert dict(created) == properties
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
        assert created.start_node().bound
        assert created.end_node().bound

    def test_can_create_nodes_plus_relationship(self):
        # when
        a, b, ab = self.graph.create(Node(), Node(), Relationship(0, "KNOWS", 1))
        # then
        assert ab.start_node() is a
        assert ab.end_node() is b

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
        path = Path(nodes[0], "LOVES", nodes[1], Relationship(nodes[2], "HATES", nodes[1]),
                    nodes[2], "KNOWS", nodes[3])
        # when
        created, = self.graph.create(path)
        # then
        assert isinstance(created, Path)
        assert created.nodes() == nodes
        assert created[0].type() == "LOVES"
        assert created[1].type() == "HATES"
        assert created[2].type() == "KNOWS"

    def test_cannot_create_entity_of_other_castable_type(self):
        with self.assertRaises(TypeError):
            self.graph.create(None)

    def test_node_pointer_must_point_to_a_node(self):
        with self.assertRaises(ValueError):
            self.graph.create({}, {}, (0, "KNOWS", 1), (0, "KNOWS", 2))

    def test_can_create_single_node(self):
        results = self.graph.create(
            {"name": "Alice"}
        )
        assert results is not None
        assert len(results) == 1
        assert isinstance(results[0], Node)
        assert "name" in results[0]
        assert results[0]["name"] == "Alice"

    def test_can_create_simple_graph(self):
        results = self.graph.create(
            {"name": "Alice"},
            {"name": "Bob"},
            (0, "KNOWS", 1)
        )
        assert results is not None
        assert len(results) == 3
        assert isinstance(results[0], Node)
        assert "name" in results[0]
        assert results[0]["name"] == "Alice"
        assert isinstance(results[1], Node)
        assert "name" in results[1]
        assert results[1]["name"] == "Bob"
        assert isinstance(results[2], Relationship)
        assert results[2].type() == "KNOWS"
        assert results[2].start_node() == results[0]
        assert results[2].end_node() == results[1]

    def test_can_create_simple_graph_with_rel_data(self):
        results = self.graph.create(
            {"name": "Alice"},
            {"name": "Bob"},
            (0, "KNOWS", 1, {"since": 1996})
        )
        assert results is not None
        assert len(results) == 3
        assert isinstance(results[0], Node)
        assert "name" in results[0]
        assert results[0]["name"] == "Alice"
        assert isinstance(results[1], Node)
        assert "name" in results[1]
        assert results[1]["name"] == "Bob"
        assert isinstance(results[2], Relationship)
        assert results[2].type() == "KNOWS"
        assert results[2].start_node() == results[0]
        assert results[2].end_node() == results[1]
        assert "since" in results[2]
        assert results[2]["since"] == 1996

    def test_can_create_graph_against_existing_node(self):
        ref_node, = self.graph.create({})
        results = self.graph.create(
            {"name": "Alice"},
            (ref_node, "PERSON", 0)
        )
        assert results is not None
        assert len(results) == 2
        assert isinstance(results[0], Node)
        assert "name" in results[0]
        assert results[0]["name"] == "Alice"
        assert isinstance(results[1], Relationship)
        assert results[1].type() == "PERSON"
        assert results[1].start_node() == ref_node
        assert results[1].end_node() == results[0]
        self.graph.delete(results[1], results[0], ref_node)

    def test_fails_on_bad_reference(self):
        with self.assertRaises(Exception):
            self.graph.create({"name": "Alice"}, (0, "KNOWS", 1))

    def test_can_create_big_graph(self):
        size = 40
        nodes = [
            {"number": i}
            for i in range(size)
        ]
        results = self.graph.create(*nodes)
        assert results is not None
        assert len(results) == size
        for i in range(size):
            assert isinstance(results[i], Node)
