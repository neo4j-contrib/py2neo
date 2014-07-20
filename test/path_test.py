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

from py2neo import neo4j, Node, Path, Rev, Relationship


class TestPathConstruction(object):

    def test_can_construct_simple_path(self):
        alice = Node(name="Alice")
        bob = Node(name="Bob")
        path = Path(alice, "KNOWS", bob)
        assert path.order == 2
        assert path.size == 1


class TestPath(object):

    def test_can_create_path(self):
        path = Path({"name": "Alice"}, "KNOWS", {"name": "Bob"})
        assert len(path) == 1
        assert path.nodes[0]["name"] == "Alice"
        assert path.rels[0].type == "KNOWS"
        assert path.nodes[-1]["name"] == "Bob"
        path = Path(path, "KNOWS", {"name": "Carol"})
        assert len(path) == 2
        assert path.nodes[0]["name"] == "Alice"
        assert path.relationships[0].type == "KNOWS"
        assert path.nodes[1]["name"] == "Bob"
        path = Path({"name": "Zach"}, "KNOWS", path)
        assert len(path) == 3
        assert path.nodes[0]["name"] == "Zach"
        assert path.relationships[0].type == "KNOWS"
        assert path.nodes[1]["name"] == "Alice"
        assert path.relationships[1].type == "KNOWS"
        assert path.nodes[2]["name"] == "Bob"

    def test_can_slice_path(self):
        path = Path({"name": "Alice"},
            "KNOWS", {"name": "Bob"},
            "KNOWS", {"name": "Carol"},
            "KNOWS", {"name": "Dave"},
            "KNOWS", {"name": "Eve"},
            "KNOWS", {"name": "Frank"},
        )
        assert len(path) == 5
        assert path[0] == Path({"name": "Alice"}, "KNOWS", {"name": "Bob"})
        assert path[1] == Path({"name": "Bob"}, "KNOWS", {"name": "Carol"})
        assert path[2] == Path({"name": "Carol"}, "KNOWS", {"name": "Dave"})
        assert path[-1] == Path({"name": "Eve"}, "KNOWS", {"name": "Frank"})
        assert path[0:2] == Path({"name": "Alice"}, "KNOWS", {"name": "Bob"}, "KNOWS", {"name": "Carol"})
        assert path[3:5] == Path({"name": "Dave"}, "KNOWS", {"name": "Eve"}, "KNOWS", {"name": "Frank"})
        assert path[:] == Path({"name": "Alice"}, "KNOWS", {"name": "Bob"}, "KNOWS", {"name": "Carol"}, "KNOWS", {"name": "Dave"}, "KNOWS", {"name": "Eve"}, "KNOWS", {"name": "Frank"})

    def test_can_iterate_path(self):
        path = Path({"name": "Alice"},
            "KNOWS", {"name": "Bob"},
            "KNOWS", {"name": "Carol"},
            "KNOWS", {"name": "Dave"},
            "KNOWS", {"name": "Eve"},
            "KNOWS", {"name": "Frank"},
        )
        assert list(iter(path)) == [
            Path({'name': 'Alice'}, 'KNOWS', {'name': 'Bob'}),
            Path({'name': 'Bob'}, 'KNOWS', {'name': 'Carol'}),
            Path({'name': 'Carol'}, 'KNOWS', {'name': 'Dave'}),
            Path({'name': 'Dave'}, 'KNOWS', {'name': 'Eve'}),
            Path({'name': 'Eve'}, 'KNOWS', {'name': 'Frank'}),
        ]
        assert list(enumerate(path)) == [
            (0, Path({'name': 'Alice'}, 'KNOWS', {'name': 'Bob'})),
            (1, Path({'name': 'Bob'}, 'KNOWS', {'name': 'Carol'})),
            (2, Path({'name': 'Carol'}, 'KNOWS', {'name': 'Dave'})),
            (3, Path({'name': 'Dave'}, 'KNOWS', {'name': 'Eve'})),
            (4, Path({'name': 'Eve'}, 'KNOWS', {'name': 'Frank'}))
        ]

    def test_can_join_paths(self):
        path1 = Path({"name": "Alice"}, "KNOWS", {"name": "Bob"})
        path2 = Path({"name": "Carol"}, "KNOWS", {"name": "Dave"})
        path = Path(path1, "KNOWS", path2)
        assert list(iter(path)) == [
            Path({'name': 'Alice'}, 'KNOWS', {'name': 'Bob'}),
            Path({'name': 'Bob'}, 'KNOWS', {'name': 'Carol'}),
            Path({'name': 'Carol'}, 'KNOWS', {'name': 'Dave'}),
        ]

    #def test_path_representation(self):
    #    path = Path({"name": "Alice"}, "KNOWS", {"name": "Bob"})
    #    #print(str(path))
    #    assert str(path) == '({"name":"Alice"})-[:"KNOWS"]->({"name":"Bob"})'
    #    #print(repr(path))
    #    assert repr(path) == (
    #        "Path(node({'name': 'Alice'}), "
    #        "('KNOWS', {}), "
    #        "node({'name': 'Bob'}))"
    #    )


class TestCreatePath(object):

    @pytest.fixture(autouse=True)
    def setup(self, graph):
        self.graph = graph
        neo4j.Graph.auto_sync_properties = True

    def test_can_create_path(self, graph):
        path = Path({"name": "Alice"}, "KNOWS", {"name": "Bob"})
        assert path.nodes[0] == {"name": "Alice"}
        assert path.rels[0].type == "KNOWS"
        assert path.nodes[1] == {"name": "Bob"}
        path = path.create(graph)
        assert isinstance(path.nodes[0], Node)
        assert path.nodes[0]["name"] == "Alice"
        assert isinstance(path.relationships[0], neo4j.Relationship)
        assert path.relationships[0].type == "KNOWS"
        assert isinstance(path.nodes[1], Node)
        assert path.nodes[1]["name"] == "Bob"

    def test_can_create_path_with_rel_properties(self):
        path = Path({"name": "Alice"}, ("KNOWS", {"since": 1999}), {"name": "Bob"})
        assert path.nodes[0] == {"name": "Alice"}
        assert path.rels[0].type == "KNOWS"
        assert path.rels[0].properties == {"since": 1999}
        assert path.nodes[1] == {"name": "Bob"}
        path = path.create(self.graph)
        assert isinstance(path.nodes[0], Node)
        assert path.nodes[0]["name"] == "Alice"
        assert isinstance(path.relationships[0], neo4j.Relationship)
        assert path.relationships[0].type == "KNOWS"
        assert path.relationships[0].properties == {"since": 1999}
        assert isinstance(path.nodes[1], Node)
        assert path.nodes[1]["name"] == "Bob"


class TestGetOrCreatePath(object):

    @pytest.fixture(autouse=True)
    def setup(self, graph):
        self.graph = graph

    def test_can_create_single_path(self):
        start_node, = self.graph.create({})
        p1 = start_node.get_or_create_path(
            "YEAR",  {"number": 2000},
            "MONTH", {"number": 12, "name": "December"},
            "DAY",   {"number": 25},
        )
        #print(p1)
        assert isinstance(p1, Path)
        assert len(p1) == 3
        assert p1.nodes[0] == start_node

    def test_can_create_overlapping_paths(self):
        start_node, = self.graph.create({})
        p1 = start_node.get_or_create_path(
            "YEAR",  {"number": 2000},
            "MONTH", {"number": 12, "name": "December"},
            "DAY",   {"number": 25, "name": "Christmas Day"},
        )
        assert isinstance(p1, Path)
        assert len(p1) == 3
        assert p1.nodes[0] == start_node
        #print(p1)
        p2 = start_node.get_or_create_path(
            "YEAR",  {"number": 2000},
            "MONTH", {"number": 12, "name": "December"},
            "DAY",   {"number": 24, "name": "Christmas Eve"},
        )
        assert isinstance(p2, Path)
        assert len(p2) == 3
        assert p1.nodes[0] == p2.nodes[0]
        assert p1.nodes[1] == p2.nodes[1]
        assert p1.nodes[2] == p2.nodes[2]
        assert p1.nodes[3] != p2.nodes[3]
        assert p1.relationships[0] == p2.relationships[0]
        assert p1.relationships[1] == p2.relationships[1]
        assert p1.relationships[2] != p2.relationships[2]
        #print(p2)
        p3 = start_node.get_or_create_path(
            "YEAR",  {"number": 2000},
            "MONTH", {"number": 11, "name": "November"},
            "DAY",   {"number": 5, "name": "Bonfire Night"},
        )
        assert isinstance(p3, Path)
        assert len(p3) == 3
        assert p2.nodes[0] == p3.nodes[0]
        assert p2.nodes[1] == p3.nodes[1]
        assert p2.nodes[2] != p3.nodes[2]
        assert p2.nodes[3] != p3.nodes[3]
        assert p2.relationships[0] == p3.relationships[0]
        assert p2.relationships[1] != p3.relationships[1]
        assert p2.relationships[2] != p3.relationships[2]
        #print(p3)

    def test_can_use_none_for_nodes(self):
        start_node, = self.graph.create({})
        p1 = start_node.get_or_create_path(
            "YEAR",  {"number": 2000},
            "MONTH", {"number": 12, "name": "December"},
            "DAY",   {"number": 25},
        )
        p2 = start_node.get_or_create_path(
            "YEAR",  {"number": 2000},
            "MONTH", None,
            "DAY",   {"number": 25},
        )
        assert isinstance(p2, Path)
        assert len(p2) == 3
        assert p1.nodes[0] == p2.nodes[0]
        assert p1.nodes[1] == p2.nodes[1]
        assert p1.nodes[2] == p2.nodes[2]
        assert p1.nodes[3] == p2.nodes[3]
        assert p1.relationships[0] == p2.relationships[0]
        assert p1.relationships[1] == p2.relationships[1]
        assert p1.relationships[2] == p2.relationships[2]

    def test_can_use_node_for_nodes(self):
        start_node, = self.graph.create({})
        p1 = start_node.get_or_create_path(
            "YEAR",  {"number": 2000},
            "MONTH", {"number": 12, "name": "December"},
            "DAY",   {"number": 25},
        )
        p2 = start_node.get_or_create_path(
            "YEAR",  {"number": 2000},
            "MONTH", p1.nodes[2],
            "DAY",   {"number": 25},
        )
        assert isinstance(p2, Path)
        assert len(p2) == 3
        assert p1.nodes[0] == p2.nodes[0]
        assert p1.nodes[1] == p2.nodes[1]
        assert p1.nodes[2] == p2.nodes[2]
        assert p1.nodes[3] == p2.nodes[3]
        assert p1.relationships[0] == p2.relationships[0]
        assert p1.relationships[1] == p2.relationships[1]
        assert p1.relationships[2] == p2.relationships[2]


class TestPathIterationAndReversal(object):

    @pytest.fixture
    def alice(self):
        return Node("Person", name="Alice", age=33)

    @pytest.fixture
    def bob(self):
        return Node("Person", name="Bob", age=44)

    @pytest.fixture
    def carol(self):
        return Node("Person", name="Carol", age=55)

    @pytest.fixture
    def dave(self):
        return Node("Person", name="Dave", age=66)

    def test_can_iterate_path_relationships(self, alice, bob, carol, dave):
        # given
        path = Path(alice, "LOVES", bob, Rev("HATES"), carol, "KNOWS", dave)
        # when
        rels = list(path)
        # then
        assert rels == [
            Relationship(alice, "LOVES", bob),
            Relationship(carol, "HATES", bob),
            Relationship(carol, "KNOWS", dave),
        ]

    def test_can_make_new_path_from_relationships(self, alice, bob, carol, dave):
        # given
        path = Path(alice, "LOVES", bob, Rev("HATES"), carol, "KNOWS", dave)
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

    def test_can_make_new_path_from_path(self, alice, bob, carol, dave):
        # given
        path = Path(alice, "LOVES", bob, Rev("HATES"), carol, "KNOWS", dave)
        # when
        new_path = Path(path)
        # then
        new_rels = list(new_path)
        assert new_rels == [
            Relationship(alice, "LOVES", bob),
            Relationship(carol, "HATES", bob),
            Relationship(carol, "KNOWS", dave),
        ]

    def test_can_reverse_iterate_path_relationships(self, alice, bob, carol, dave):
        # given
        path = Path(alice, "LOVES", bob, Rev("HATES"), carol, "KNOWS", dave)
        # when
        rels = list(reversed(path))
        # then
        assert rels == [
            Relationship(carol, "KNOWS", dave),
            Relationship(carol, "HATES", bob),
            Relationship(alice, "LOVES", bob),
        ]
