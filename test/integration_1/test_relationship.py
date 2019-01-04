#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2019, Nigel Small
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


from pytest import raises

from py2neo import Node, Relationship


KNOWS = Relationship.type("KNOWS")


def test_relationship_creation(graph):
    a = Node("Person", name="Alice")
    b = Node("Person", name="Bob")
    ab = KNOWS(a, b, since=1999)
    assert type(ab) is KNOWS
    assert ab["since"] == 1999
    assert ab.start_node is a
    assert ab.end_node is b
    graph.create(ab)
    assert isinstance(ab.identity, int)
    assert isinstance(a.identity, int)
    assert isinstance(b.identity, int)
    assert graph.exists(ab)
    assert graph.exists(a)
    assert graph.exists(b)


def test_can_get_relationship_by_id_when_cached(graph):
    a = Node()
    b = Node()
    r = Relationship(a, "TO", b)
    graph.create(r)
    got = graph.relationships.get(r.identity)
    assert got is r


def test_can_get_relationship_by_id_when_not_cached(graph):
    a = Node()
    b = Node()
    r = Relationship(a, "TO", b)
    graph.create(r)
    graph.relationship_cache.clear()
    got = graph.relationships.get(r.identity)
    assert got.identity == r.identity


def test_relationship_cache_is_thread_local(graph):
    import threading
    a = Node()
    b = Node()
    r = Relationship(a, "TO", b)
    graph.create(r)
    assert r.identity in graph.relationship_cache
    other_relationship_cache_keys = []

    def check_cache():
        other_relationship_cache_keys.extend(graph.relationship_cache.keys())

    thread = threading.Thread(target=check_cache)
    thread.start()
    thread.join()

    assert r.identity in graph.relationship_cache
    assert r.identity not in other_relationship_cache_keys


def test_cannot_get_relationship_by_id_when_id_does_not_exist(graph):
    a = Node()
    b = Node()
    r = Relationship(a, "TO", b)
    graph.create(r)
    rel_id = r.identity
    graph.delete(r)
    with raises(KeyError):
        _ = graph.relationships[rel_id]


def test_getting_no_relationships(graph):
    alice = Node(name="Alice")
    graph.create(alice)
    rels = list(graph.match(nodes=[alice]))
    assert rels is not None
    assert isinstance(rels, list)
    assert len(rels) == 0


def test_relationship_creation_2(graph):
    a = Node()
    b = Node()
    r = Relationship(a, "TO", b)
    graph.create(r)
    assert r.graph is graph
    assert r.identity is not None


def test_relationship_creation_on_existing_node(graph):
    a = Node()
    graph.create(a)
    b = Node()
    r = Relationship(a, "TO", b)
    graph.create(r)
    assert r.graph is a.graph is graph
    assert r.identity is not None


def test_only_one_relationship_in_a_relationship(graph):
    rel = Relationship({}, "KNOWS", {})
    assert len(rel.relationships) == 1


def test_relationship_equality_with_none(graph):
    rel = Relationship({}, "KNOWS", {})
    none = None
    assert rel != none


def test_relationship_equality_for_concrete(graph):
    a = Node()
    b = Node()
    r1 = Relationship(a, "KNOWS", b)
    r2 = Relationship(a, "KNOWS", b)
    graph.create(r1)
    graph.create(r2)
    assert r1 == r2


def test_cannot_delete_uncreated_relationship(graph):
    a = Node()
    b = Node()
    graph.create(a | b)
    r = Relationship(a, "TO", b)
    graph.delete(r)


def test_relationship_exists(graph):
    a = Node()
    b = Node()
    r = Relationship(a, "TO", b)
    graph.create(r)
    assert graph.exists(r)


def test_relationship_does_not_exist(graph):
    a = Node()
    b = Node()
    graph.create(a | b)
    r = Relationship(a, "TO", b)
    assert r.graph is not graph
    assert not graph.exists(r)


def test_blank_type_automatically_updates(graph):
    a = Node()
    b = Node()
    r = Relationship(a, "TO", b)
    graph.create(r)
    r._type = None
    assert r.graph is not None
    assert r.identity is not None
    assert r._type is None
    assert type(r).__name__ == "TO"


def test_cannot_cast_from_odd_object(graph):
    with raises(TypeError):
        _ = Relationship.cast(object())
