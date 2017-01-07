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


from py2neo.http import WebResource, RemoteEntity, remote
from py2neo.types import Node, Relationship

from test.util import GraphTestCase


class EntityTestCase(GraphTestCase):
        
    def test_can_create_entity_with_initial_uri(self):
        uri = "http://localhost:7474/db/data/node/1"
        entity = Node()
        entity.__remote__ = RemoteEntity(uri)
        assert remote(entity)
        assert remote(entity).uri == uri

    def test_can_create_entity_with_initial_uri_and_metadata(self):
        uri = "http://localhost:7474/db/data/node/1"
        metadata = {"foo": "bar"}
        entity = Node()
        entity.__remote__  = RemoteEntity(uri, metadata)
        assert remote(entity)
        assert remote(entity).uri == uri
        remote_metadata = remote(entity).get_json(force=False)
        assert remote_metadata == metadata

    def test_default_state_for_node_is_unbound(self):
        node = Node()
        assert not remote(node)

    def test_can_bind_node_to_resource(self):
        uri = "http://localhost:7474/db/data/node/1"
        node = Node()
        node.__remote__ = RemoteEntity(uri)
        assert remote(node)
        assert isinstance(remote(node), WebResource)
        assert remote(node).uri == uri

    def test_can_bind_relationship_to_resource(self):
        uri = "http://localhost:7474/db/relationship/1"
        metadata = {
            "start": "http://localhost:7474/db/node/1",
            "end": "http://localhost:7474/db/node/2",
        }
        relationship = Relationship({}, "", {})
        # Pass in metadata to avoid callback to server
        relationship.__remote__ = RemoteEntity(uri, metadata=metadata)
        assert remote(relationship)
        assert isinstance(remote(relationship), WebResource)
        assert remote(relationship).uri == uri


class AutoNamingTestCase(GraphTestCase):

    def test_can_name_using_name_property(self):
        a = Node(name="Alice")
        assert a.__name__ == "alice"

    def test_can_name_using_magic_name_property(self):
        a = Node(__name__="Alice")
        assert a.__name__ == "Alice"
