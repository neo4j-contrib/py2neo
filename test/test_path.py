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


from py2neo import Node, Path, Rev, Relationship, JoinError, Rel, ServiceRoot, BindError, Graph
from test.util import Py2neoTestCase
from test.compat import assert_repr


class PathTestCase(Py2neoTestCase):

    def test_can_construct_simple_path(self):
        alice = Node(name="Alice")
        bob = Node(name="Bob")
        path = Path(alice, "KNOWS", bob)
        assert path.order() == 2
        assert path.size() == 1

    def test_can_create_path(self):
        path = Path({"name": "Alice"}, "KNOWS", {"name": "Bob"})
        nodes = path.nodes()
        assert len(path) == 1
        assert nodes[0]["name"] == "Alice"
        assert path[0].type() == "KNOWS"
        assert nodes[-1]["name"] == "Bob"
        path = Path(path, "KNOWS", {"name": "Carol"})
        nodes = path.nodes()
        assert len(path) == 2
        assert nodes[0]["name"] == "Alice"
        assert path[0].type() == "KNOWS"
        assert nodes[1]["name"] == "Bob"
        path = Path({"name": "Zach"}, "KNOWS", path)
        nodes = path.nodes()
        assert len(path) == 3
        assert nodes[0]["name"] == "Zach"
        assert path[0].type() == "KNOWS"
        assert nodes[1]["name"] == "Alice"
        assert path[1].type() == "KNOWS"
        assert nodes[2]["name"] == "Bob"

    def test_can_slice_path(self):
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

    def test_can_iterate_path(self):
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

    def test_can_join_paths(self):
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

    def test_path_repr(self):
        alice = Node("Person", name="Alice")
        bob = Node("Person", name="Bob")
        path = Path(alice, "KNOWS", bob)
        assert repr(path) == "<Path order=2 size=1>"
        self.graph.create(path)
        assert repr(path).startswith("<Path")

    def test_path_str(self):
        alice = Node("Person", name="Alice")
        bob = Node("Person", name="Bob")
        path = Path(alice, "KNOWS", bob)
        assert str(path) == '(:Person {name:"Alice"})-[:KNOWS]->(:Person {name:"Bob"})'
        self.graph.create(path)
        assert str(path) == '(:Person {name:"Alice"})-[:KNOWS]->(:Person {name:"Bob"})'

    def test_can_hydrate_path(self):
        dehydrated = self.cypher.post("CREATE p=()-[:KNOWS]->() RETURN p")["data"][0][0]
        if "directions" not in dehydrated:
            dehydrated["directions"] = ["->"]
        hydrated = Path.hydrate(dehydrated)
        assert isinstance(hydrated, Path)
        assert hydrated.order() == 2
        assert hydrated.size() == 1
        assert hydrated[0].type() == "KNOWS"
        
    def test_can_hydrate_path_into_existing_instance(self):
        alice = Node("Person", name="Alice", age=33)
        bob = Node("Person", name="Bob", age=44)
        dehydrated = self.cypher.post("CREATE p=()-[:KNOWS]->() RETURN p")["data"][0][0]
        path = Path(alice, "KNOWS", bob)
        if "directions" not in dehydrated:
            dehydrated["directions"] = ["->"]
        hydrated = Path.hydrate(dehydrated, inst=path)
        assert hydrated is path
        
    def test_can_hydrate_path_without_directions(self):
        statement = "CREATE p=()-[:LIKES]->()<-[:DISLIKES]-() RETURN p"
        dehydrated = self.cypher.post(statement)["data"][0][0]
        try:
            del dehydrated["directions"]
        except KeyError:
            pass
        hydrated = self.graph.hydrate(dehydrated)
        assert isinstance(hydrated, Path)
        assert hydrated.order() == 3
        assert hydrated.size() == 2
        assert hydrated[0].type() == "LIKES"
        assert hydrated[1].type() == "DISLIKES"

    def test_cannot_build_path_with_two_consecutive_rels(self):
        with self.assertRaises(TypeError):
            _ = Path(Node(name="Alice"), Rel("KNOWS"), Rel("KNOWS"), Node(name="Bob"))

    def test_path_equality(self):
        alice = Node(name="Alice")
        bob = Node(name="Bob")
        carol = Node(name="Carol")
        dave = Node(name="Dave")
        path_1 = Path(alice, "LOVES", bob, Rev("HATES"), carol, "KNOWS", dave)
        path_2 = Path(alice, "LOVES", bob, Rev("HATES"), carol, "KNOWS", dave)
        assert path_1 == path_2

    def test_path_inequality(self):
        alice = Node(name="Alice")
        bob = Node(name="Bob")
        carol = Node(name="Carol")
        dave = Node(name="Dave")
        path_1 = Path(alice, "LOVES", bob, Rev("HATES"), carol, "KNOWS", dave)
        path_2 = Path(alice, "KNOWS", carol, Rev("KNOWS"), dave)
        assert path_1 != path_2
        assert path_1 != ""

    def test_path_in_several_ways(self):
        alice = Node(name="Alice")
        bob = Node(name="Bob")
        carol = Node(name="Carol")
        dave = Node(name="Dave")
        path = Path(alice, "LOVES", bob, Rev("HATES"), carol, "KNOWS", dave)
        assert path.__bool__()
        assert path.__nonzero__()
        assert path[0] == Relationship(alice, "LOVES", bob)
        assert path[1] == Relationship(carol, "HATES", bob)
        assert path[2] == Relationship(carol, "KNOWS", dave)
        assert path[-1] == Relationship(carol, "KNOWS", dave)
        assert path[0:1] == Path(alice, "LOVES", bob)
        assert path[0:2] == Path(alice, "LOVES", bob, Rev("HATES"), carol)
        try:
            _ = path[7]
        except IndexError:
            assert True
        else:
            assert False

    def test_service_root_on_bound_path(self):
        alice = Node(name="Alice")
        bob = Node(name="Bob")
        carol = Node(name="Carol")
        dave = Node(name="Dave")
        path = Path(alice, "LOVES", bob, Rev("HATES"), carol, "KNOWS", dave)
        self.graph.create(path)
        assert path.service_root == ServiceRoot("http://localhost:7474/")
        path[0].unbind()
        assert path.service_root == ServiceRoot("http://localhost:7474/")

    def test_service_root_on_unbound_path(self):
        alice = Node(name="Alice")
        bob = Node(name="Bob")
        carol = Node(name="Carol")
        dave = Node(name="Dave")
        path = Path(alice, "LOVES", bob, Rev("HATES"), carol, "KNOWS", dave)
        try:
            assert path.service_root == ServiceRoot("http://localhost:7474/")
        except BindError:
            assert True
        else:
            assert False

    def test_graph(self):
        alice = Node(name="Alice")
        bob = Node(name="Bob")
        carol = Node(name="Carol")
        dave = Node(name="Dave")
        path, = self.graph.create(Path(alice, "LOVES", bob, Rev("HATES"), carol, "KNOWS", dave))
        assert path.graph == Graph("http://localhost:7474/db/data/")

    def test_path_direction(self):
        cypher = """\
        CREATE p=({name:'Alice'})-[:KNOWS]->({name:'Bob'})<-[:DISLIKES]-
                 ({name:'Carol'})-[:MARRIED_TO]->({name:'Dave'})
        RETURN p
        """
        path = self.cypher.evaluate(cypher)
        assert path[0].start_node()["name"] == "Alice"
        assert path[0].end_node()["name"] == "Bob"
        assert path[1].start_node()["name"] == "Carol"
        assert path[1].end_node()["name"] == "Bob"
        assert path[2].start_node()["name"] == "Carol"
        assert path[2].end_node()["name"] == "Dave"


class CreatePathTestCase(Py2neoTestCase):

    def setUp(self):
        Graph.auto_sync_properties = True

    def test_can_create_path(self):
        path = Path({"name": "Alice"}, "KNOWS", {"name": "Bob"})
        nodes = path.nodes()
        assert dict(nodes[0]) == {"name": "Alice"}
        assert path[0].type() == "KNOWS"
        assert dict(nodes[1]) == {"name": "Bob"}
        path, = self.graph.create(path)
        assert isinstance(nodes[0], Node)
        assert nodes[0]["name"] == "Alice"
        assert isinstance(path[0], Relationship)
        assert path[0].type() == "KNOWS"
        assert isinstance(nodes[1], Node)
        assert nodes[1]["name"] == "Bob"

    def test_can_create_path_with_rel_properties(self):
        path = Path({"name": "Alice"}, ("KNOWS", {"since": 1999}), {"name": "Bob"})
        nodes = path.nodes()
        assert dict(nodes[0]) == {"name": "Alice"}
        assert path[0].type() == "KNOWS"
        assert dict(path[0]) == {"since": 1999}
        assert dict(nodes[1]) == {"name": "Bob"}
        path, = self.graph.create(path)
        assert isinstance(nodes[0], Node)
        assert nodes[0]["name"] == "Alice"
        assert isinstance(path[0], Relationship)
        assert path[0].type() == "KNOWS"
        assert dict(path[0]) == {"since": 1999}
        assert isinstance(nodes[1], Node)
        assert nodes[1]["name"] == "Bob"


class CreateUniquePathTestCase(Py2neoTestCase):

    def test_can_create_single_path(self):
        start_node, = self.graph.create({})
        p1, = self.graph.create_unique(Path(
            start_node,
            "YEAR",  {"number": 2000},
            "MONTH", {"number": 12, "name": "December"},
            "DAY",   {"number": 25},
        ))
        n1 = p1.nodes()
        assert isinstance(p1, Path)
        assert len(p1) == 3
        assert n1[0] == start_node

    def test_can_create_overlapping_paths(self):
        start_node, = self.graph.create({})
        p1, = self.graph.create_unique(Path(
            start_node,
            "YEAR",  {"number": 2000},
            "MONTH", {"number": 12, "name": "December"},
            "DAY",   {"number": 25, "name": "Christmas Day"},
        ))
        n1 = p1.nodes()
        assert isinstance(p1, Path)
        assert len(p1) == 3
        assert n1[0] == start_node
        p2, = self.graph.create_unique(Path(
            start_node,
            "YEAR",  {"number": 2000},
            "MONTH", {"number": 12, "name": "December"},
            "DAY",   {"number": 24, "name": "Christmas Eve"},
        ))
        n2 = p2.nodes()
        assert isinstance(p2, Path)
        assert len(p2) == 3
        assert n1[0] == n2[0]
        assert n1[1] == n2[1]
        assert n1[2] == n2[2]
        assert n1[3] != n2[3]
        assert p1[0] == p2[0]
        assert p1[1] == p2[1]
        assert p1[2] != p2[2]
        p3, = self.graph.create_unique(Path(
            start_node,
            "YEAR",  {"number": 2000},
            "MONTH", {"number": 11, "name": "November"},
            "DAY",   {"number": 5, "name": "Bonfire Night"},
        ))
        n3 = p3.nodes()
        assert isinstance(p3, Path)
        assert len(p3) == 3
        assert n2[0] == n3[0]
        assert n2[1] == n3[1]
        assert n2[2] != n3[2]
        assert n2[3] != n3[3]
        assert p2[0] == p3[0]
        assert p2[1] != p3[1]
        assert p2[2] != p3[2]

    def test_can_use_node_for_nodes(self):
        start_node, = self.graph.create({})
        p1, = self.graph.create_unique(Path(
            start_node,
            "YEAR",  {"number": 2000},
            "MONTH", {"number": 12, "name": "December"},
            "DAY",   {"number": 25},
        ))
        n1 = p1.nodes()
        p2, = self.graph.create_unique(Path(
            start_node,
            "YEAR",  {"number": 2000},
            "MONTH", n1[2],
            "DAY",   {"number": 25},
        ))
        n2 = p2.nodes()
        assert isinstance(p2, Path)
        assert len(p2) == 3
        assert n1[0] == n2[0]
        assert n1[1] == n2[1]
        assert n1[2] == n2[2]
        assert n1[3] == n2[3]
        assert p1[0] == p2[0]
        assert p1[1] == p2[1]
        assert p1[2] == p2[2]


class PathIterationAndReversalTestCase(Py2neoTestCase):

    def setUp(self):
        self.alice = Node("Person", name="Alice", age=33)
        self.bob = Node("Person", name="Bob", age=44)
        self.carol = Node("Person", name="Carol", age=55)
        self.dave = Node("Person", name="Dave", age=66)

    def test_can_iterate_path_relationships(self):
        # given
        path = Path(self.alice, "LOVES", self.bob, Rev("HATES"), self.carol, "KNOWS", self.dave)
        # when
        rels = list(path)
        # then
        assert rels == [
            Relationship(self.alice, "LOVES", self.bob),
            Relationship(self.carol, "HATES", self.bob),
            Relationship(self.carol, "KNOWS", self.dave),
        ]

    def test_can_make_new_path_from_relationships(self):
        # given
        path = Path(self.alice, "LOVES", self.bob, Rev("HATES"), self.carol, "KNOWS", self.dave)
        rels = list(path)
        # when
        new_path = Path(*rels)
        # then
        new_rels = list(new_path)
        assert new_rels == [
            Relationship(self.alice, "LOVES", self.bob),
            Relationship(self.carol, "HATES", self.bob),
            Relationship(self.carol, "KNOWS", self.dave),
        ]

    def test_can_make_new_path_from_path(self):
        # given
        path = Path(self.alice, "LOVES", self.bob, Rev("HATES"), self.carol, "KNOWS", self.dave)
        # when
        new_path = Path(path)
        # then
        new_rels = list(new_path)
        assert new_rels == [
            Relationship(self.alice, "LOVES", self.bob),
            Relationship(self.carol, "HATES", self.bob),
            Relationship(self.carol, "KNOWS", self.dave),
        ]

    def test_can_reverse_iterate_path_relationships(self):
        # given
        path = Path(self.alice, "LOVES", self.bob, Rev("HATES"), self.carol, "KNOWS", self.dave)
        # when
        rels = list(reversed(path))
        # then
        assert rels == [
            Relationship(self.carol, "KNOWS", self.dave),
            Relationship(self.carol, "HATES", self.bob),
            Relationship(self.alice, "LOVES", self.bob),
        ]
