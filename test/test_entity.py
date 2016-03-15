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


from py2neo import Resource, Node, Relationship, remote
from test.util import Py2neoTestCase


class EntityTestCase(Py2neoTestCase):
        
    def test_can_create_entity_with_initial_uri(self):
        uri = "http://localhost:7474/db/data/node/1"
        entity = Node()
        entity._set_remote(uri)
        assert remote(entity)
        assert remote(entity).uri == uri

    def test_can_create_entity_with_initial_uri_and_metadata(self):
        uri = "http://localhost:7474/db/data/node/1"
        metadata = {"foo": "bar"}
        entity = Node()
        entity._set_remote(uri, metadata)
        assert remote(entity)
        assert remote(entity).uri == uri
        assert remote(entity).metadata == metadata

    def test_default_state_for_node_is_unbound(self):
        node = Node()
        assert not remote(node)

    def test_can_bind_node_to_resource(self):
        uri = "http://localhost:7474/db/data/node/1"
        node = Node()
        node._set_remote(uri)
        assert remote(node)
        assert isinstance(remote(node), Resource)
        assert remote(node).uri == uri
        node._del_remote()
        assert not remote(node)

    def test_can_bind_relationship_to_resource(self):
        uri = "http://localhost:7474/db/relationship/1"
        metadata = {
            "start": "http://localhost:7474/db/node/1",
            "end": "http://localhost:7474/db/node/2",
        }
        relationship = Relationship({}, "", {})
        # Pass in metadata to avoid callback to server
        relationship._set_remote(uri, metadata=metadata)
        assert remote(relationship)
        assert isinstance(remote(relationship), Resource)
        assert remote(relationship).uri == uri
        relationship._del_remote()
        assert not remote(relationship)

    def test_can_unbind_node_if_not_cached(self):
        node = Node()
        self.graph.create(node)
        Node.cache.clear()
        node._del_remote()
        assert not remote(node)

    def test_can_unbind_relationship_if_not_cached(self):
        a = Node()
        b = Node()
        ab = Relationship(a, "KNOWS", b)
        self.graph.create(ab)
        Relationship.cache.clear()
        ab._del_remote()
        assert not remote(ab)

    def test_can_unbind_relationship_with_already_unbound_nodes(self):
        a = Node()
        b = Node()
        ab = Relationship(a, "KNOWS", b)
        self.graph.create(ab)
        a._del_remote()
        b._del_remote()
        assert not remote(a)
        assert not remote(b)
        ab._del_remote()
        assert not remote(ab)


class AutoNamingTestCase(Py2neoTestCase):

    def test_can_name_using_name_property(self):
        a = Node(name="Alice")
        assert a.__name__ == "alice"

    def test_can_name_using_magic_name_property(self):
        a = Node(__name__="Alice")
        assert a.__name__ == "Alice"
