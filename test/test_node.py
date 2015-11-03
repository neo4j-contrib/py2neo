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


from py2neo import Node, NodePointer, GraphError, BindError, Path
from test.compat import patch, long, assert_repr
from test.util import Py2neoTestCase
from py2neo.packages.httpstream import ClientError, Resource as _Resource


class DodgyClientError(ClientError):
    status_code = 499


class NodeTestCase(Py2neoTestCase):

    def test_can_create_node(self):
        a = Node("Person", name="Alice", age=33)
        assert a.labels == {"Person"}
        assert a.properties == {"name": "Alice", "age": 33}

    def test_bound_node_equals_unbound_node_with_same_properties(self):
        alice_1 = Node(name="Alice")
        alice_1.bind("http://localhost:7474/db/data/node/1")
        alice_2 = Node(name="Alice")
        assert alice_1 == alice_2

    def test_bound_node_equality(self):
        alice_1 = Node(name="Alice")
        alice_1.bind("http://localhost:7474/db/data/node/1")
        Node.cache.clear()
        alice_2 = Node(name="Alice")
        alice_2.bind(alice_1.uri)
        assert alice_1 == alice_2

    def test_unbound_node_equality(self):
        alice_1 = Node("Person", name="Alice")
        alice_2 = Node("Person", name="Alice")
        assert alice_1 == alice_2

    def test_node_exists_will_raise_non_404_errors(self):
        with patch.object(_Resource, "get") as mocked:
            error = GraphError("bad stuff happened")
            error.response = DodgyClientError()
            mocked.side_effect = error
            alice = Node(name="Alice Smith")
            alice.bind("http://localhost:7474/db/data/node/1")
            with self.assertRaises(GraphError):
                _ = alice.exists

    def test_node_degree(self):
        alice = Node("Person", name="Alice")
        bob = Node("Person", name="Bob")
        carol = Node("Person", name="Carol")
        with self.assertRaises(BindError):
            _ = alice.degree
        self.graph.create(alice)
        assert alice.degree == 0
        self.graph.create(Path(alice, "KNOWS", bob))
        assert alice.degree == 1
        self.graph.create(Path(alice, "KNOWS", carol))
        assert alice.degree == 2
        self.graph.create(Path(carol, "KNOWS", alice))
        assert alice.degree == 3

    def test_can_merge_unsaved_changes_when_querying_node(self):
        alice, _, _ = self.graph.create(Node("Person", name="Alice"), {}, (0, "KNOWS", 1))
        assert alice.properties == {"name": "Alice"}
        alice["age"] = 33
        assert alice.properties == {"name": "Alice", "age": 33}
        _ = list(alice.match_outgoing("KNOWS"))
        assert alice.properties == {"name": "Alice", "age": 33}

    def test_will_error_when_refreshing_deleted_node(self):
        node = Node()
        self.graph.create(node)
        self.graph.delete(node)
        with self.assertRaises(GraphError):
            node.refresh()


class AbstractNodeTestCase(Py2neoTestCase):

    def test_can_create_unbound_node(self):
        alice = Node(name="Alice", age=34)
        assert isinstance(alice, Node)
        assert not alice.bound
        assert alice["name"] == "Alice"
        assert alice["age"] == 34

    def test_node_equality(self):
        alice_1 = Node(name="Alice", age=34)
        alice_2 = Node(name="Alice", age=34)
        assert alice_1 == alice_2

    def test_node_inequality(self):
        alice = Node(name="Alice", age=34)
        bob = Node(name="Bob", age=56)
        assert alice != bob

    def test_node_is_never_equal_to_none(self):
        alice = Node(name="Alice", age=34)
        assert alice is not None


class ConcreteNodeTestCase(Py2neoTestCase):

    def test_can_create_concrete_node(self):
        alice, = self.graph.create({"name": "Alice", "age": 34})
        assert isinstance(alice, Node)
        assert alice.bound
        assert alice["name"] == "Alice"
        assert alice["age"] == 34

    def test_all_property_types(self):
        data = {
            "nun": None,
            "yes": True,
            "no": False,
            "int": 42,
            "float": 3.141592653589,
            "long": long("9223372036854775807"),
            "str": "hello, world",
            "unicode": u"hello, world",
            "boolean_list": [True, False, True, True, False],
            "int_list": [1, 1, 2, 3, 5, 8, 13, 21, 35],
            "str_list": ["red", "orange", "yellow", "green", "blue", "indigo", "violet"]
        }
        foo, = self.graph.create(data)
        for key, value in data.items():
            self.assertEqual(foo[key], value)

    def test_cannot_assign_oversized_long(self):
        foo, = self.graph.create({})
        with self.assertRaises(ValueError):
            foo["long"] = long("9223372036854775808")

    def test_cannot_assign_mixed_list(self):
        foo, = self.graph.create({})
        with self.assertRaises(TypeError):
            foo["mixed_list"] = [42, "life", "universe", "everything"]

    def test_cannot_assign_dict(self):
        foo, = self.graph.create({})
        with self.assertRaises(TypeError):
            foo["dict"] = {"foo": 3, "bar": 4, "baz": 5}

    def test_relative_uri_of_bound_node(self):
        node, = self.graph.create({})
        relative_uri_string = node.ref
        assert node.uri.string.endswith(relative_uri_string)
        assert relative_uri_string.startswith("node/")

    def test_relative_uri_of_unbound_node(self):
        node = Node()
        with self.assertRaises(BindError):
            _ = node.ref

    def test_node_repr(self):
        node = Node("Person", name="Alice")
        assert_repr(node, "<Node labels={'Person'} properties={'name': 'Alice'}>",
                          "<Node labels=set([u'Person']) properties={'name': u'Alice'}>")
        self.graph.create(node)
        assert_repr(node, "<Node graph='http://localhost:7474/db/data/' ref='%s' "
                          "labels={'Person'} properties={'name': 'Alice'}>" % node.ref,
                          "<Node graph=u'http://localhost:7474/db/data/' ref=u'%s' "
                          "labels=set([u'Person']) properties={'name': u'Alice'}>" % node.ref)

    def test_node_hashes(self):
        assert hash(Node()) == hash(Node())
        assert hash(Node(name="Alice")) == hash(Node(name="Alice"))
        assert hash(Node(name="Alice", age=33)) == hash(Node(age=33, name="Alice"))
        assert (hash(Node("Person", name="Alice", age=33)) ==
                hash(Node("Person", age=33, name="Alice")))
        node_1 = Node("Person", name="Alice")
        self.graph.create(node_1)
        node_2 = Node("Person", name="Alice")
        node_2.bind(node_1.uri)
        assert node_1 is not node_2
        assert hash(node_1) == hash(node_2)

    def test_stale_node_repr(self):
        node = Node.hydrate({"self": "http://localhost:7474/db/data/node/0"})
        assert_repr(node, "<Node graph='http://localhost:7474/db/data/' ref='%s' "
                          "labels=? properties=?>" % node.ref,
                          "<Node graph=u'http://localhost:7474/db/data/' ref=u'%s' "
                          "labels=? properties=?>" % node.ref)

    def test_node_str(self):
        node = Node("Person", name="Alice")
        assert str(node) == '(:Person {name:"Alice"})'
        self.graph.create(node)
        assert str(node) == '(n%s:Person {name:"Alice"})' % node._id


class NodeMatchTestCase(Py2neoTestCase):
        
    def setUp(self):
        a, b, c, d, e = self.graph.create(
            {"name": "Alice"},
            {"name": "Bob"},
            {"name": "Carol"},
            {"name": "Dave"},
            {"name": "Eve"},
        )
        rels = self.graph.create(
            (a, "LOVES", b),
            (b, "LOVES", a),
            (b, "KNOWS", c),
            (b, "KNOWS", d),
            (d, "LOVES", e),
        )
        self.sample_graph = a, b, c, d, e, rels

    def test_can_match_zero_outgoing(self):
        a, b, c, d, e, rels = self.sample_graph
        matches = list(e.match_outgoing())
        assert len(matches) == 0

    def test_can_match_one_outgoing(self):
        a, b, c, d, e, rels = self.sample_graph
        matches = list(a.match_outgoing())
        assert len(matches) == 1
        assert rels[0] in matches

    def test_can_match_many_outgoing(self):
        a, b, c, d, e, rels = self.sample_graph
        matches = list(b.match_outgoing())
        assert len(matches) == 3
        assert rels[1] in matches
        assert rels[2] in matches
        assert rels[3] in matches

    def test_can_match_many_outgoing_with_limit(self):
        a, b, c, d, e, rels = self.sample_graph
        matches = list(b.match_outgoing(limit=2))
        assert len(matches) == 2
        for match in matches:
            assert match in (rels[1], rels[2], rels[3])

    def test_can_match_many_outgoing_by_type(self):
        a, b, c, d, e, rels = self.sample_graph
        matches = list(b.match_outgoing("KNOWS"))
        assert len(matches) == 2
        assert rels[2] in matches
        assert rels[3] in matches

    def test_can_match_many_outgoing_by_multiple_types(self):
        a, b, c, d, e, rels = self.sample_graph
        matches = list(b.match_outgoing(("KNOWS", "LOVES")))
        assert len(matches) == 3
        assert rels[1] in matches
        assert rels[2] in matches
        assert rels[3] in matches

    def test_can_match_many_in_both_directions(self):
        a, b, c, d, e, rels = self.sample_graph
        matches = list(b.match())
        assert len(matches) == 4
        assert rels[0] in matches
        assert rels[1] in matches
        assert rels[2] in matches
        assert rels[3] in matches

    def test_can_match_many_in_both_directions_with_limit(self):
        a, b, c, d, e, rels = self.sample_graph
        matches = list(b.match(limit=2))
        assert len(matches) == 2
        for match in matches:
            assert match in (rels[0], rels[1], rels[2], rels[3])

    def test_can_match_many_by_type_in_both_directions(self):
        a, b, c, d, e, rels = self.sample_graph
        matches = list(b.match("LOVES"))
        assert len(matches) == 2
        assert rels[0] in matches
        assert rels[1] in matches


class NodePointerTestCase(Py2neoTestCase):

    def test_node_pointer_equality(self):
        p1 = NodePointer(42)
        p2 = NodePointer(42)
        assert p1 == p2

    def test_node_pointer_inequality(self):
        p1 = NodePointer(42)
        p2 = NodePointer(69)
        assert p1 != p2

    def test_node_pointer_hashes(self):
        assert hash(NodePointer(42)) == hash(NodePointer(42))

    def test_node_pointer_repr(self):
        pointer = NodePointer(3456)
        assert_repr(pointer, "<NodePointer address=3456>")

    def test_node_pointer_str(self):
        pointer = NodePointer(3456)
        assert str(pointer) == "{3456}"
