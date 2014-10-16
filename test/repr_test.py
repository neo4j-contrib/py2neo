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


from __future__ import unicode_literals

import sys

from py2neo import Node, NodePointer, Relationship, Path


PY2 = sys.version_info < (3,)


def test_graph_repr(graph):
    if PY2:
        assert repr(graph) == "<Graph uri=u'http://localhost:7474/db/data/'>"
    else:
        assert repr(graph) == "<Graph uri='http://localhost:7474/db/data/'>"


def test_node_repr(graph):
    node = Node("Person", name="Alice")
    if PY2:
        assert repr(node) == "<Node labels=set([u'Person']) properties={'name': u'Alice'}>"
    else:
        assert repr(node) == "<Node labels={'Person'} properties={'name': 'Alice'}>"
    graph.create(node)
    if PY2:
        assert repr(node) == ("<Node graph=u'http://localhost:7474/db/data/' ref=u'%s' "
                              "labels=set([u'Person']) properties={'name': u'Alice'}>" % node.ref)
    else:
        assert repr(node) == ("<Node graph='http://localhost:7474/db/data/' ref='%s' "
                              "labels={'Person'} properties={'name': 'Alice'}>" % node.ref)


def test_node_pointer_repr():
    pointer = NodePointer(3456)
    assert repr(pointer) == "<NodePointer address=3456>"


def test_relationship_repr(graph):
    alice = Node("Person", name="Alice")
    bob = Node("Person", name="Bob")
    relationship = Relationship(alice, "KNOWS", bob)
    if PY2:
        assert repr(relationship) == "<Relationship type=u'KNOWS' properties={}>"
    else:
        assert repr(relationship) == "<Relationship type='KNOWS' properties={}>"
    graph.create(relationship)
    if PY2:
        assert repr(relationship) == ("<Relationship graph=u'http://localhost:7474/db/data/' "
                                      "ref=u'%s' start=u'%s' end=u'%s' type=u'KNOWS' "
                                      "properties={}>" % (relationship.ref, alice.ref, bob.ref))
    else:
        assert repr(relationship) == ("<Relationship graph='http://localhost:7474/db/data/' "
                                      "ref='%s' start='%s' end='%s' type='KNOWS' "
                                      "properties={}>" % (relationship.ref, alice.ref, bob.ref))


def test_rel_repr(graph):
    alice = Node("Person", name="Alice")
    bob = Node("Person", name="Bob")
    relationship = Relationship(alice, "KNOWS", bob)
    graph.create(relationship)
    rel = relationship.rel
    if PY2:
        assert repr(rel) == ("<Rel graph=u'http://localhost:7474/db/data/' ref=u'%s' "
                             "type=u'KNOWS' properties={}>" % rel.ref)
    else:
        assert repr(rel) == ("<Rel graph='http://localhost:7474/db/data/' ref='%s' "
                             "type='KNOWS' properties={}>" % rel.ref)


def test_rev_repr(graph):
    alice = Node("Person", name="Alice")
    bob = Node("Person", name="Bob")
    relationship = Relationship(alice, "KNOWS", bob)
    graph.create(relationship)
    rev = -relationship.rel
    if PY2:
        assert repr(rev) == ("<Rev graph=u'http://localhost:7474/db/data/' ref=u'%s' "
                             "type=u'KNOWS' properties={}>" % rev.ref)
    else:
        assert repr(rev) == ("<Rev graph='http://localhost:7474/db/data/' ref='%s' "
                             "type='KNOWS' properties={}>" % rev.ref)


def test_path_repr(graph):
    alice = Node("Person", name="Alice")
    bob = Node("Person", name="Bob")
    path = Path(alice, "KNOWS", bob)
    assert repr(path) == "<Path order=2 size=1>"
    graph.create(path)
    if PY2:
        assert repr(path) == ("<Path graph=u'http://localhost:7474/db/data/' "
                              "start=u'%s' end=u'%s' order=2 size=1>" % (alice.ref, bob.ref))
    else:
        assert repr(path) == ("<Path graph='http://localhost:7474/db/data/' "
                              "start='%s' end='%s' order=2 size=1>" % (alice.ref, bob.ref))
