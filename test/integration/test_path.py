#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2020, Nigel Small
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


from pytest import fixture

from py2neo import Node, Relationship, Path


KNOWS = Relationship.type("KNOWS")


def test_can_create_path(graph):
    path = Path({"name": "Alice"}, "KNOWS", {"name": "Bob"})
    nodes = path.nodes
    assert dict(nodes[0]) == {"name": "Alice"}
    assert type(path[0]) is KNOWS
    assert dict(nodes[1]) == {"name": "Bob"}
    graph.create(path)
    assert isinstance(nodes[0], Node)
    assert nodes[0]["name"] == "Alice"
    assert isinstance(path[0], Relationship)
    assert type(path[0]) is KNOWS
    assert isinstance(nodes[1], Node)
    assert nodes[1]["name"] == "Bob"


def test_can_create_path_with_rel_properties(graph):
    path = Path({"name": "Alice"}, ("KNOWS", {"since": 1999}), {"name": "Bob"})
    nodes = path.nodes
    assert dict(nodes[0]) == {"name": "Alice"}
    assert type(path[0]) is KNOWS
    assert dict(path[0]) == {"since": 1999}
    assert dict(nodes[1]) == {"name": "Bob"}
    graph.create(path)
    assert isinstance(nodes[0], Node)
    assert nodes[0]["name"] == "Alice"
    assert isinstance(path[0], Relationship)
    assert type(path[0]) is KNOWS
    assert dict(path[0]) == {"since": 1999}
    assert isinstance(nodes[1], Node)
    assert nodes[1]["name"] == "Bob"


def test_can_construct_simple_path(graph):
    alice = Node(name="Alice")
    bob = Node(name="Bob")
    path = Path(alice, "KNOWS", bob)
    assert len(path.nodes) == 2
    assert len(path.relationships) == 1
    assert len(path) == 1


def test_can_construct_path_with_none_node(graph):
    alice = Node(name="Alice")
    path = Path(alice, "KNOWS", None)
    assert len(path.nodes) == 2
    assert len(path.relationships) == 1
    assert len(path) == 1


def test_can_create_path_2(graph):
    path = Path({"name": "Alice"}, "KNOWS", {"name": "Bob"})
    nodes = path.nodes
    assert len(path) == 1
    assert nodes[0]["name"] == "Alice"
    assert type(path[0]) is KNOWS
    assert nodes[-1]["name"] == "Bob"
    path = Path(path, "KNOWS", {"name": "Carol"})
    nodes = path.nodes
    assert len(path) == 2
    assert nodes[0]["name"] == "Alice"
    assert type(path[0]) is KNOWS
    assert nodes[1]["name"] == "Bob"
    path = Path({"name": "Zach"}, "KNOWS", path)
    nodes = path.nodes
    assert len(path) == 3
    assert nodes[0]["name"] == "Zach"
    assert type(path[0]) is KNOWS
    assert nodes[1]["name"] == "Alice"
    assert type(path[1]) is KNOWS
    assert nodes[2]["name"] == "Bob"


def test_can_slice_path(graph):
    a = Node(name="Alice")
    b = Node(name="Bob")
    c = Node(name="Carol")
    d = Node(name="Dave")
    e = Node(name="Eve")
    f = Node(name="Frank")
    path = Path(a, "KNOWS", b, "KNOWS", c, "KNOWS", d, "KNOWS", e, "KNOWS", f)
    assert len(path) == 5
    assert path[0] == Relationship(a, "KNOWS", b)
    assert path[1] == Relationship(b, "KNOWS", c)
    assert path[2] == Relationship(c, "KNOWS", d)
    assert path[-1] == Relationship(e, "KNOWS", f)
    assert path[0:2] == Path(a, "KNOWS", b, "KNOWS", c)
    assert path[3:5] == Path(d, "KNOWS", e, "KNOWS", f)
    assert path[:] == Path(a, "KNOWS", b, "KNOWS", c, "KNOWS", d, "KNOWS", e, "KNOWS", f)


def test_can_iterate_path(graph):
    a = Node(name="Alice")
    b = Node(name="Bob")
    c = Node(name="Carol")
    d = Node(name="Dave")
    e = Node(name="Eve")
    f = Node(name="Frank")
    path = Path(a, "KNOWS", b, "KNOWS", c, "KNOWS", d, "KNOWS", e, "KNOWS", f)
    assert list(path) == [
        Relationship(a, 'KNOWS', b),
        Relationship(b, 'KNOWS', c),
        Relationship(c, 'KNOWS', d),
        Relationship(d, 'KNOWS', e),
        Relationship(e, 'KNOWS', f),
    ]
    assert list(enumerate(path)) == [
        (0, Relationship(a, 'KNOWS', b)),
        (1, Relationship(b, 'KNOWS', c)),
        (2, Relationship(c, 'KNOWS', d)),
        (3, Relationship(d, 'KNOWS', e)),
        (4, Relationship(e, 'KNOWS', f))
    ]


def test_can_join_paths(graph):
    a = Node(name="Alice")
    b = Node(name="Bob")
    c = Node(name="Carol")
    d = Node(name="Dave")
    path1 = Path(a, "KNOWS", b)
    path2 = Path(c, "KNOWS", d)
    path = Path(path1, "KNOWS", path2)
    assert list(path) == [
        Relationship(a, 'KNOWS', b),
        Relationship(b, 'KNOWS', c),
        Relationship(c, 'KNOWS', d),
    ]


def test_path_equality(graph):
    alice = Node(name="Alice")
    bob = Node(name="Bob")
    carol = Node(name="Carol")
    dave = Node(name="Dave")
    path_1 = Path(alice, "LOVES", bob, Relationship(carol, "HATES", bob), carol, "KNOWS", dave)
    path_2 = Path(alice, "LOVES", bob, Relationship(carol, "HATES", bob), carol, "KNOWS", dave)
    assert path_1 == path_2


def test_path_inequality(graph):
    alice = Node(name="Alice")
    bob = Node(name="Bob")
    carol = Node(name="Carol")
    dave = Node(name="Dave")
    path_1 = Path(alice, "LOVES", bob, Relationship(carol, "HATES", bob), carol, "KNOWS", dave)
    path_2 = Path(alice, "KNOWS", carol, Relationship(dave, "KNOWS", carol), dave)
    assert path_1 != path_2
    assert path_1 != ""


def test_path_in_several_ways(graph):
    alice = Node(name="Alice")
    bob = Node(name="Bob")
    carol = Node(name="Carol")
    dave = Node(name="Dave")
    path = Path(alice, "LOVES", bob, Relationship(carol, "HATES", bob), carol, "KNOWS", dave)
    assert path.__bool__()
    assert path.__nonzero__()
    assert path[0] == Relationship(alice, "LOVES", bob)
    assert path[1] == Relationship(carol, "HATES", bob)
    assert path[2] == Relationship(carol, "KNOWS", dave)
    assert path[-1] == Relationship(carol, "KNOWS", dave)
    assert path[0:1] == Path(alice, "LOVES", bob)
    assert path[0:2] == Path(alice, "LOVES", bob, Relationship(carol, "HATES", bob), carol)
    try:
        _ = path[7]
    except IndexError:
        assert True
    else:
        assert False


def test_path_direction(graph):
    cypher = """\
    CREATE p=({name:'Alice'})-[:KNOWS]->({name:'Bob'})<-[:DISLIKES]-
             ({name:'Carol'})-[:MARRIED_TO]->({name:'Dave'})
    RETURN p
    """
    path = graph.evaluate(cypher)
    assert path[0].start_node["name"] == "Alice"
    assert path[0].end_node["name"] == "Bob"
    assert path[1].start_node["name"] == "Carol"
    assert path[1].end_node["name"] == "Bob"
    assert path[2].start_node["name"] == "Carol"
    assert path[2].end_node["name"] == "Dave"


@fixture()
def alice():
    return Node("Person", name="Alice", age=33)


@fixture()
def bob():
    return Node("Person", name="Bob", age=44)


@fixture()
def carol():
    return Node("Person", name="Carol", age=55)


@fixture()
def dave():
    return Node("Person", name="Dave", age=66)


def test_can_iterate_path_relationships(alice, bob, carol, dave):
    # given
    path = Path(alice, "LOVES", bob, Relationship(carol, "HATES", bob),
                carol, "KNOWS", dave)
    # when
    rels = list(path)
    # then
    assert rels == [
        Relationship(alice, "LOVES", bob),
        Relationship(carol, "HATES", bob),
        Relationship(carol, "KNOWS", dave),
    ]


def test_can_make_new_path_from_relationships(alice, bob, carol, dave):
    # given
    path = Path(alice, "LOVES", bob, Relationship(carol, "HATES", bob),
                carol, "KNOWS", dave)
    rels = list(path)
    # when
    new_path = Path(*rels)
    # then
    new_rels = list(new_path)
    assert new_rels == [
        Relationship(alice, "LOVES", bob),
        Relationship(carol, "HATES", bob),
        Relationship(carol, "KNOWS", dave),
    ]


def test_can_make_new_path_from_path(alice, bob, carol, dave):
    # given
    path = Path(alice, "LOVES", bob, Relationship(carol, "HATES", bob),
                carol, "KNOWS", dave)
    # when
    new_path = Path(path)
    # then
    new_rels = list(new_path)
    assert new_rels == [
        Relationship(alice, "LOVES", bob),
        Relationship(carol, "HATES", bob),
        Relationship(carol, "KNOWS", dave),
    ]


def test_can_reverse_iterate_path_relationships(alice, bob, carol, dave):
    # given
    path = Path(alice, "LOVES", bob, Relationship(carol, "HATES", bob),
                carol, "KNOWS", dave)
    # when
    rels = list(reversed(path))
    # then
    assert rels == [
        Relationship(carol, "KNOWS", dave),
        Relationship(carol, "HATES", bob),
        Relationship(alice, "LOVES", bob),
    ]
