#/usr/bin/env python
# -*- coding: utf-8 -*-

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


import pytest

from py2neo.core import Resource, Node, Rel, Rev, Relationship
from py2neo.error import BindError


def test_default_state_for_node_is_unbound():
    node = Node()
    assert not node.bound
    with pytest.raises(BindError):
        r = node.resource


def test_can_bind_node_to_resource():
    uri = "http://localhost:7474/db/data/node/1"
    node = Node()
    node.bind(uri)
    assert node.bound
    assert isinstance(node.resource, Resource)
    assert node.resource.uri == uri
    node.unbind()
    assert not node.bound
    with pytest.raises(BindError):
        r = node.resource


def test_can_bind_rel_to_resource():
    uri = "http://localhost:7474/db/relationship/1"
    rel = Rel()
    rel.bind(uri)
    assert rel.bound
    assert isinstance(rel.resource, Resource)
    assert rel.resource.uri == uri
    rel.unbind()
    assert not rel.bound
    with pytest.raises(BindError):
        r = rel.resource


def test_can_bind_rev_to_resource():
    uri = "http://localhost:7474/db/relationship/1"
    rel = Rev()
    rel.bind(uri)
    assert rel.bound
    assert isinstance(rel.resource, Resource)
    assert rel.resource.uri == uri
    rel.unbind()
    assert not rel.bound
    with pytest.raises(BindError):
        r = rel.resource


def test_can_bind_relationship_to_resource():
    uri = "http://localhost:7474/db/relationship/1"
    relationship = Relationship({}, "", {})
    relationship.bind(uri)
    assert relationship.bound
    assert isinstance(relationship.resource, Resource)
    assert relationship.resource.uri == uri
    relationship.unbind()
    assert not relationship.bound
    with pytest.raises(BindError):
        r = relationship.resource
