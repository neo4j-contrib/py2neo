#!/usr/bin/env python
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


from py2neo import Node, Relationship, Path


def test_graph_repr(graph):
    assert repr(graph) == "<Graph uri='http://localhost:7474/db/data/'>"


def test_node_repr(graph):
    node = Node("Person", name="Alice")
    assert repr(node) == "<Node labels={'Person'} properties={'name': 'Alice'}>"
    graph.create(node)
    assert repr(node) == ("<Node graph='http://localhost:7474/db/data/' ref='%s' "
                          "labels={'Person'} properties={'name': 'Alice'}>" % node.ref)


def test_relationship_repr(graph):
    alice = Node("Person", name="Alice")
    bob = Node("Person", name="Bob")
    relationship = Relationship(alice, "KNOWS", bob)
    assert repr(relationship) == "<Relationship type='KNOWS' properties={}>"
    graph.create(relationship)
    assert repr(relationship) == ("<Relationship graph='http://localhost:7474/db/data/' "
                                  "ref='%s' start='%s' end='%s' type='KNOWS' "
                                  "properties={}>" % (relationship.ref, alice.ref, bob.ref))


def test_rel_repr(graph):
    alice = Node("Person", name="Alice")
    bob = Node("Person", name="Bob")
    relationship = Relationship(alice, "KNOWS", bob)
    graph.create(relationship)
    rel = relationship.rel
    assert repr(rel) == ("<Rel graph='http://localhost:7474/db/data/' ref='%s' "
                         "type='KNOWS' properties={}>" % rel.ref)


def test_rev_repr(graph):
    alice = Node("Person", name="Alice")
    bob = Node("Person", name="Bob")
    relationship = Relationship(alice, "KNOWS", bob)
    graph.create(relationship)
    rev = -relationship.rel
    assert repr(rev) == ("<Rev graph='http://localhost:7474/db/data/' ref='%s' "
                         "type='KNOWS' properties={}>" % rev.ref)


def test_path_repr(graph):
    alice = Node("Person", name="Alice")
    bob = Node("Person", name="Bob")
    path = Path(alice, "KNOWS", bob)
    assert repr(path) == "<Path order=2 size=1>"
    graph.create(path)
    assert repr(path) == ("<Path graph='http://localhost:7474/db/data/' "
                          "start='%s' end='%s' order=2 size=1>" % (alice.ref, bob.ref))
