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


from mock import patch

from py2neo import Node, Relationship, Path, remote
from test.util import GraphTestCase


class NodeHydrationTestCase(GraphTestCase):

    def setUp(self):
        Node.cache.clear()

    def test_minimal_node_hydrate(self):
        dehydrated = {
            "self": "http://localhost:7474/db/data/node/0",
        }
        hydrated = Node.hydrate(dehydrated["self"], **dehydrated)
        assert isinstance(hydrated, Node)
        assert remote(hydrated)
        assert remote(hydrated).uri == dehydrated["self"]

    def test_node_hydrate_with_properties(self):
        dehydrated = {
            "self": "http://localhost:7474/db/data/node/0",
            "data": {
                "name": "Alice",
                "age": 33,
            },
        }
        hydrated = Node.hydrate(dehydrated["self"], **dehydrated)
        assert isinstance(hydrated, Node)
        assert dict(hydrated) == dehydrated["data"]
        assert remote(hydrated)
        assert remote(hydrated).uri == dehydrated["self"]

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
        hydrated = Node.hydrate(dehydrated["self"], **dehydrated)
        assert isinstance(hydrated, Node)
        assert dict(hydrated) == dehydrated["data"]
        assert remote(hydrated)
        assert remote(hydrated).uri == dehydrated["self"]

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
        hydrated = Node.hydrate(dehydrated["self"], **dehydrated)
        assert isinstance(hydrated, Node)
        assert dict(hydrated) == dehydrated["data"]
        assert set(hydrated.labels()) == set(dehydrated["metadata"]["labels"])
        assert remote(hydrated)
        assert remote(hydrated).uri == dehydrated["self"]

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
            hydrated = Node.hydrate(dehydrated["self"], **dehydrated)
        assert isinstance(hydrated, Node)
        assert dict(hydrated) == dehydrated["data"]
        assert set(hydrated.labels()) == set(dehydrated["metadata"]["labels"])
        assert remote(hydrated)
        assert remote(hydrated).uri == dehydrated["self"]


class RelationshipHydrationTestCase(GraphTestCase):

    def setUp(self):
        Relationship.cache.clear()

    def test_partial_relationship_hydration_with_inst(self):
        a = Node()
        b = Node()
        ab = Relationship(a, "TO", b)
        self.graph.create(ab)
        dehydrated = {
            "extensions": {
            },
            "start": "http://localhost:7474/db/data/node/%d" % remote(a)._id,
            "property": "http://localhost:7474/db/data/relationship/%d/properties/{key}" % remote(ab)._id,
            "self": "http://localhost:7474/db/data/relationship/%d" % remote(ab)._id,
            "properties": "http://localhost:7474/db/data/relationship/%d/properties" % remote(ab)._id,
            "type": "KNOWS",
            "end": "http://localhost:7474/db/data/node/%d" % remote(b)._id,
        }
        hydrated = Relationship.hydrate(dehydrated, inst=ab)
        assert isinstance(hydrated, Relationship)
        assert remote(hydrated.start_node())
        assert remote(hydrated.start_node()).uri == dehydrated["start"]
        assert remote(hydrated.end_node())
        assert remote(hydrated.end_node()).uri == dehydrated["end"]
        assert hydrated.type() == dehydrated["type"]
        assert remote(hydrated)
        assert remote(hydrated).uri == dehydrated["self"]

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
            hydrated = Relationship.hydrate(dehydrated)
        assert isinstance(hydrated, Relationship)
        assert remote(hydrated.start_node())
        assert remote(hydrated.start_node()).uri == dehydrated["start"]
        assert remote(hydrated.end_node())
        assert remote(hydrated.end_node()).uri == dehydrated["end"]
        assert hydrated.type() == dehydrated["type"]
        assert dict(hydrated) == dehydrated["data"]
        assert remote(hydrated)
        assert remote(hydrated).uri == dehydrated["self"]

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
        hydrated = Relationship.hydrate(dehydrated)
        assert isinstance(hydrated, Relationship)
        assert remote(hydrated.start_node())
        assert remote(hydrated.start_node()).uri == dehydrated["start"]
        assert remote(hydrated.end_node())
        assert remote(hydrated.end_node()).uri == dehydrated["end"]
        assert hydrated.type() == dehydrated["type"]
        assert dict(hydrated) == dehydrated["data"]
        assert remote(hydrated)
        assert remote(hydrated).uri == dehydrated["self"]

    def test_path_hydration_without_directions(self):
        a = Node()
        b = Node()
        c = Node()
        ab = Relationship(a, "KNOWS", b)
        cb = Relationship(c, "KNOWS", b)
        path = Path(a, ab, b, cb, c)
        self.graph.create(path)
        dehydrated = {
            "nodes": [remote(a).uri.string, remote(b).uri.string, remote(c).uri.string],
            "relationships": [remote(ab).uri.string, remote(cb).uri.string],
        }
        hydrated = self.graph._hydrate(dehydrated)
        assert isinstance(hydrated, Path)

    def test_list_hydration(self):
        dehydrated = [1, 2, 3]
        hydrated = self.graph._hydrate(dehydrated)
        assert hydrated == [1, 2, 3]
