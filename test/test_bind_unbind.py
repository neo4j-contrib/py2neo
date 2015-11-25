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


from py2neo.core import Resource, Node, Relationship, Bindable, Path
from py2neo.error import BindError
from test.util import Py2neoTestCase


class BindUnbindTestCase(Py2neoTestCase):
        
    def test_can_create_bindable_with_initial_uri(self):
        uri = "http://localhost:7474/db/data/node/1"
        bindable = Bindable()
        bindable.bind(uri)
        assert bindable.bound
        assert bindable.uri == uri

    def test_can_create_bindable_with_initial_uri_and_metadata(self):
        uri = "http://localhost:7474/db/data/node/1"
        metadata = {"foo": "bar"}
        bindable = Bindable()
        bindable.bind(uri, metadata)
        assert bindable.bound
        assert bindable.uri == uri
        assert bindable.resource.metadata == metadata

    def test_can_create_bindable_with_initial_uri_template(self):
        uri = "http://localhost:7474/db/data/node/{node_id}"
        bindable = Bindable()
        bindable.bind(uri)
        assert bindable.bound
        assert bindable.uri == uri

    def test_cannot_create_bindable_with_initial_uri_template_and_metadata(self):
        uri = "http://localhost:7474/db/data/node/{node_id}"
        metadata = {"foo": "bar"}
        service = Bindable()
        try:
            service.bind(uri, metadata)
        except ValueError:
            assert True
        else:
            assert False

    def test_default_state_for_node_is_unbound(self):
        node = Node()
        assert not node.bound
        with self.assertRaises(BindError):
            _ = node.resource

    def test_bound_path_is_bound(self):
        alice = Node(name="Alice")
        bob = Node(name="Bob")
        carol = Node(name="Carol")
        dave = Node(name="Dave")
        path = Path(alice, "LOVES", bob, Relationship(carol, "HATES", bob), carol, "KNOWS", dave)
        self.graph.create(path)
        assert path.bound

    def test_unbound_path_is_not_bound(self):
        alice = Node(name="Alice")
        bob = Node(name="Bob")
        carol = Node(name="Carol")
        dave = Node(name="Dave")
        path = Path(alice, "LOVES", bob, Relationship(carol, "HATES", bob), carol, "KNOWS", dave)
        assert not path.bound

    def test_can_bind_node_to_resource(self):
        uri = "http://localhost:7474/db/data/node/1"
        node = Node()
        node.bind(uri)
        assert node.bound
        assert isinstance(node.resource, Resource)
        assert node.resource.uri == uri
        node.unbind()
        assert not node.bound
        with self.assertRaises(BindError):
            _ = node.resource

    def test_can_bind_relationship_to_resource(self):
        uri = "http://localhost:7474/db/relationship/1"
        metadata = {
            "start": "http://localhost:7474/db/node/1",
            "end": "http://localhost:7474/db/node/2",
        }
        relationship = Relationship({}, "", {})
        # Pass in metadata to avoid callback to server
        relationship.bind(uri, metadata=metadata)
        assert relationship.bound
        assert isinstance(relationship.resource, Resource)
        assert relationship.resource.uri == uri
        relationship.unbind()
        assert not relationship.bound
        with self.assertRaises(BindError):
            _ = relationship.resource

    def test_can_unbind_node_if_not_cached(self):
        node, = self.graph.create({})
        Node.cache.clear()
        node.unbind()
        assert not node.bound

    def test_can_unbind_relationship_if_not_cached(self):
        a, b, ab = self.graph.create({}, {}, (0, "KNOWS", 1))
        Relationship.cache.clear()
        ab.unbind()
        assert not ab.bound

    def test_can_unbind_relationship_with_already_unbound_nodes(self):
        a, b, ab = self.graph.create({}, {}, (0, "KNOWS", 1))
        a.unbind()
        b.unbind()
        assert not a.bound
        assert not b.bound
        ab.unbind()
        assert not ab.bound

    def test_can_unbind_bound_path(self):
        alice = Node(name="Alice")
        bob = Node(name="Bob")
        carol = Node(name="Carol")
        dave = Node(name="Dave")
        path = Path(alice, "LOVES", bob, Relationship(carol, "HATES", bob), carol, "KNOWS", dave)
        self.graph.create(path)
        path.unbind()
        assert not path.bound

    def test_can_unbind_unbound_path_without_error(self):
        alice = Node(name="Alice")
        bob = Node(name="Bob")
        carol = Node(name="Carol")
        dave = Node(name="Dave")
        path = Path(alice, "LOVES", bob, Relationship(carol, "HATES", bob), carol, "KNOWS", dave)
        path.unbind()
        assert not path.bound
