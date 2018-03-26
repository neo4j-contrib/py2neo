#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2017, Nigel Small
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


from mock import patch

from py2neo.graph import Graph
from py2neo.json import JSONValueSystem
from py2neo.types import Node, Relationship, Path

from test.util import GraphTestCase


class NodeHydrationTestCase(GraphTestCase):

    def setUp(self):
        Node.cache.clear()
        self.graph = Graph()
        self.node = Node()
        self.hydrator = JSONValueSystem(self.graph, ["x"], {"x": self.node})

    def hydrate(self, dehydrated):
        hydrated, = self.hydrator.hydrate([dehydrated])
        return hydrated

    def test_minimal_node_hydrate(self):
        dehydrated = {
            "self": "http://localhost:7474/db/data/node/0",
        }
        hydrated = self.hydrate(dehydrated)
        self.assertIsInstance(hydrated, Node)
        self.assertEqual(hydrated.graph, self.graph)
        self.assertEqual(hydrated.identity, 0)

    def test_node_hydrate_with_properties(self):
        dehydrated = {
            "self": "http://localhost:7474/db/data/node/0",
            "data": {
                "name": "Alice",
                "age": 33,
            },
        }
        hydrated = self.hydrate(dehydrated)
        self.assertIsInstance(hydrated, Node)
        self.assertEqual(dict(hydrated), dehydrated["data"])
        self.assertEqual(hydrated.graph, self.graph)
        self.assertEqual(hydrated.identity, 0)

    def test_full_node_hydrate_without_labels(self):
        dehydrated = {
            "extensions": {
            },
            "paged_traverse": "http://localhost:7474/db/data/node/0/paged/traverse/{returnType}{?pageSize,leaseTime}",
            "labels": "http://localhost:7474/db/data/node/0/labels",
            "outgoing_relationships": "http://localhost:7474/db/data/node/0/relationships/out",
            "traverse": "http://localhost:7474/db/data/node/0/traverse/{returnType}",
            "all_typed_relationships": "http://localhost:7474/db/data/node/0/relationships/all/{-list|&|types}",
            "property": "http://localhost:7474/db/data/node/0/properties/{key}",
            "all_relationships": "http://localhost:7474/db/data/node/0/relationships/all",
            "self": "http://localhost:7474/db/data/node/0",
            "outgoing_typed_relationships": "http://localhost:7474/db/data/node/0/relationships/out/{-list|&|types}",
            "properties": "http://localhost:7474/db/data/node/0/properties",
            "incoming_relationships": "http://localhost:7474/db/data/node/0/relationships/in",
            "incoming_typed_relationships": "http://localhost:7474/db/data/node/0/relationships/in/{-list|&|types}",
            "create_relationship": "http://localhost:7474/db/data/node/0/relationships",
            "data": {
                "name": "Alice",
                "age": 33,
            },
        }
        hydrated = self.hydrate(dehydrated)
        self.assertIsInstance(hydrated, Node)
        self.assertEqual(dict(hydrated), dehydrated["data"])
        self.assertEqual(hydrated.graph, self.graph)
        self.assertEqual(hydrated.identity, 0)

    def test_full_node_hydrate_with_labels(self):
        dehydrated = {
            "extensions": {
            },
            "paged_traverse": "http://localhost:7474/db/data/node/0/paged/traverse/{returnType}{?pageSize,leaseTime}",
            "labels": "http://localhost:7474/db/data/node/0/labels",
            "outgoing_relationships": "http://localhost:7474/db/data/node/0/relationships/out",
            "traverse": "http://localhost:7474/db/data/node/0/traverse/{returnType}",
            "all_typed_relationships": "http://localhost:7474/db/data/node/0/relationships/all/{-list|&|types}",
            "property": "http://localhost:7474/db/data/node/0/properties/{key}",
            "all_relationships": "http://localhost:7474/db/data/node/0/relationships/all",
            "self": "http://localhost:7474/db/data/node/0",
            "outgoing_typed_relationships": "http://localhost:7474/db/data/node/0/relationships/out/{-list|&|types}",
            "properties": "http://localhost:7474/db/data/node/0/properties",
            "incoming_relationships": "http://localhost:7474/db/data/node/0/relationships/in",
            "incoming_typed_relationships": "http://localhost:7474/db/data/node/0/relationships/in/{-list|&|types}",
            "create_relationship": "http://localhost:7474/db/data/node/0/relationships",
            "data": {
                "name": "Alice",
                "age": 33,
            },
            "metadata": {
                "labels": ["Person", "Employee"],
            },
        }
        hydrated = self.hydrate(dehydrated)
        self.assertIsInstance(hydrated, Node)
        self.assertEqual(dict(hydrated), dehydrated["data"])
        self.assertEqual(set(hydrated.labels), set(dehydrated["metadata"]["labels"]))
        self.assertEqual(hydrated.graph, self.graph)
        self.assertEqual(hydrated.identity, 0)

    def test_node_hydration_with_issue_19542(self):
        dehydrated = {
            "extensions": {
            },
            "paged_traverse": "http://localhost:7474/db/data/node/0/paged/traverse/{returnType}{?pageSize,leaseTime}",
            "labels": "http://localhost:7474/db/data/node/0/labels",
            "outgoing_relationships": "http://localhost:7474/db/data/node/0/relationships/out",
            "traverse": "http://localhost:7474/db/data/node/0/traverse/{returnType}",
            "all_typed_relationships": "http://localhost:7474/db/data/node/0/relationships/all/{-list|&|types}",
            "property": "http://localhost:7474/db/data/node/0/properties/{key}",
            "all_relationships": "http://localhost:7474/db/data/node/0/relationships/all",
            "self": "http://localhost:7474/db/data/node/0",
            "outgoing_typed_relationships": "http://localhost:7474/db/data/node/0/relationships/out/{-list|&|types}",
            "properties": "http://localhost:7474/db/data/node/0/properties",
            "incoming_relationships": "http://localhost:7474/db/data/node/0/relationships/in",
            "incoming_typed_relationships": "http://localhost:7474/db/data/node/0/relationships/in/{-list|&|types}",
            "create_relationship": "http://localhost:7474/db/data/node/0/relationships",
            "data": {
                "name": "Alice",
                "age": 33,
            },
            "metadata": {
                "labels": ["Person", "Employee"],
            },
        }

        with patch("weakref.WeakValueDictionary.setdefault") as mocked:
            mocked.return_value = None
            hydrated = self.hydrate(dehydrated)
        self.assertIsInstance(hydrated, Node)
        self.assertEqual(dict(hydrated), dehydrated["data"])
        self.assertEqual(set(hydrated.labels), set(dehydrated["metadata"]["labels"]))
        self.assertEqual(hydrated.graph, self.graph)
        self.assertEqual(hydrated.identity, 0)


class RelationshipHydrationTestCase(GraphTestCase):

    def setUp(self):
        Relationship.cache.clear()
        self.graph = Graph()
        self.hydrator = JSONValueSystem(self.graph, ["x"])

    def hydrate(self, dehydrated):
        hydrated, = self.hydrator.hydrate([dehydrated])
        return hydrated

    def test_partial_relationship_hydration_with_inst(self):
        a = Node()
        b = Node()
        ab = Relationship(a, "TO", b)
        self.hydrator = JSONValueSystem(self.graph, ["x"], {"x": ab})
        self.graph.create(ab)
        dehydrated = {
            "extensions": {
            },
            "start": "http://localhost:7474/db/data/node/%d" % a.identity,
            "property": "http://localhost:7474/db/data/relationship/%d/properties/{key}" % ab.identity,
            "self": "http://localhost:7474/db/data/relationship/%d" % ab.identity,
            "properties": "http://localhost:7474/db/data/relationship/%d/properties" % ab.identity,
            "type": "KNOWS",
            "end": "http://localhost:7474/db/data/node/%d" % b.identity,
        }
        hydrated = self.hydrate(dehydrated)
        self.assertIsInstance(hydrated, Relationship)
        self.assertEqual(hydrated.start_node().graph, self.graph)
        self.assertEqual(hydrated.start_node().identity, a.identity)
        self.assertEqual(hydrated.end_node().graph, self.graph)
        self.assertEqual(hydrated.end_node().identity, b.identity)
        self.assertEqual(hydrated.type, dehydrated["type"])
        self.assertEqual(hydrated.graph, self.graph)
        self.assertEqual(hydrated.identity, ab.identity)

    def test_relationship_hydration_with_issue_19542(self):
        dehydrated = {
            "extensions": {
            },
            "start": "http://localhost:7474/db/data/node/23",
            "property": "http://localhost:7474/db/data/relationship/11/properties/{key}",
            "self": "http://localhost:7474/db/data/relationship/11",
            "properties": "http://localhost:7474/db/data/relationship/11/properties",
            "type": "KNOWS",
            "end": "http://localhost:7474/db/data/node/22",
            "data": {
                "since": 1999,
            },
        }
        with patch("weakref.WeakValueDictionary.setdefault") as mocked:
            mocked.return_value = None
            hydrated = self.hydrate(dehydrated)
        self.assertIsInstance(hydrated, Relationship)
        self.assertEqual(hydrated.start_node().graph, self.graph)
        self.assertEqual(hydrated.start_node().identity, 23)
        self.assertEqual(hydrated.end_node().graph, self.graph)
        self.assertEqual(hydrated.end_node().identity, 22)
        self.assertEqual(hydrated.type, dehydrated["type"])
        self.assertEqual(dict(hydrated), dehydrated["data"])
        self.assertEqual(hydrated.graph, self.graph)
        self.assertEqual(hydrated.identity, 11)

    def test_full_relationship_hydrate(self):
        dehydrated = {
            "extensions": {
            },
            "start": "http://localhost:7474/db/data/node/23",
            "property": "http://localhost:7474/db/data/relationship/11/properties/{key}",
            "self": "http://localhost:7474/db/data/relationship/11",
            "properties": "http://localhost:7474/db/data/relationship/11/properties",
            "type": "KNOWS",
            "end": "http://localhost:7474/db/data/node/22",
            "data": {
                "since": 1999,
            },
        }
        hydrated = self.hydrate(dehydrated)
        self.assertIsInstance(hydrated, Relationship)
        self.assertEqual(hydrated.start_node().graph, self.graph)
        self.assertEqual(hydrated.start_node().identity, 23)
        self.assertEqual(hydrated.end_node().graph, self.graph)
        self.assertEqual(hydrated.end_node().identity, 22)
        self.assertEqual(hydrated.type, dehydrated["type"])
        self.assertEqual(dict(hydrated), dehydrated["data"])
        self.assertEqual(hydrated.graph, self.graph)
        self.assertEqual(hydrated.identity, 11)

    def test_path_hydration_without_directions(self):
        a = Node()
        b = Node()
        c = Node()
        ab = Relationship(a, "KNOWS", b)
        cb = Relationship(c, "KNOWS", b)
        path = Path(a, ab, b, cb, c)
        self.graph.create(path)

        def uri(entity):
            return "http://localhost:7474/db/data/node/%d" % entity.identity

        dehydrated = {
            "nodes": [uri(a), uri(b), uri(c)],
            "relationships": [uri(ab), uri(cb)],
        }
        value_system = JSONValueSystem(self.graph, ["a"])
        hydrated = value_system.hydrate([dehydrated])
        assert isinstance(hydrated[0], Path)

    def test_list_hydration(self):
        dehydrated = [1, 2, 3]
        value_system = JSONValueSystem(self.graph, ["a"])
        hydrated = value_system.hydrate([dehydrated])
        assert hydrated[0] == [1, 2, 3]
