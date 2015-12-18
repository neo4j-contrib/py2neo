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


from py2neo import Node, BindError, Relationship, Path, node
from test.compat import long
from test.util import Py2neoTestCase
from py2neo.packages.httpstream import ClientError


class DodgyClientError(ClientError):
    status_code = 499


class NodeTestCase(Py2neoTestCase):

    def test_can_create_local_node(self):
        a = Node("Person", name="Alice", age=33)
        assert a.labels() == {"Person"}
        assert dict(a) == {"name": "Alice", "age": 33}

    def test_can_create_remote_node(self):
        a = Node("Person", name="Alice", age=33)
        self.graph.create(a)
        assert a.labels() == {"Person"}
        assert dict(a) == {"name": "Alice", "age": 33}
        assert a.ref.startswith("node/")

    def test_bound_node_equals_unbound_node_with_same_properties(self):
        alice_1 = Node(name="Alice")
        alice_1.bind("http://localhost:7474/db/data/node/1")
        alice_2 = Node(name="Alice")
        assert alice_1.labels() == alice_2.labels()
        assert dict(alice_1) == dict(alice_2)

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
        assert alice_1.labels() == alice_2.labels()
        assert dict(alice_1) == dict(alice_2)

    def test_node_degree(self):
        alice = Node("Person", name="Alice")
        bob = Node("Person", name="Bob")
        carol = Node("Person", name="Carol")
        with self.assertRaises(BindError):
            _ = alice.degree()
        self.graph.create(alice)
        assert alice.degree() == 0
        self.graph.create(Path(alice, "KNOWS", bob))
        assert alice.degree() == 1
        self.graph.create(Path(alice, "KNOWS", carol))
        assert alice.degree() == 2
        self.graph.create(Path(carol, "KNOWS", alice))
        assert alice.degree() == 3

    def test_can_merge_unsaved_changes_when_querying_node(self):
        a = Node("Person", name="Alice")
        b = Node()
        self.graph.create(a | b | Relationship(a, "KNOWS", b))
        assert dict(a) == {"name": "Alice"}
        a["age"] = 33
        assert dict(a) == {"name": "Alice", "age": 33}
        _ = list(self.graph.match(a, "KNOWS"))
        assert dict(a) == {"name": "Alice", "age": 33}

    def test_will_error_when_refreshing_deleted_node(self):
        a = Node()
        self.graph.create(a)
        self.graph.delete(a)
        with self.assertRaises(BindError):
            a.refresh()


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
        assert alice_1.labels() == alice_2.labels()
        assert dict(alice_1) == dict(alice_2)

    def test_node_inequality(self):
        alice = Node(name="Alice", age=34)
        bob = Node(name="Bob", age=56)
        assert alice != bob

    def test_node_is_never_equal_to_none(self):
        alice = Node(name="Alice", age=34)
        assert alice is not None


class ConcreteNodeTestCase(Py2neoTestCase):

    def test_can_create_concrete_node(self):
        alice = node({"name": "Alice", "age": 34})
        self.graph.create(alice)
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
        foo = node(data)
        self.graph.create(foo)
        for key, value in data.items():
            self.assertEqual(foo[key], value)

    def test_cannot_assign_oversized_long(self):
        foo = Node()
        self.graph.create(foo)
        with self.assertRaises(ValueError):
            foo["long"] = long("9223372036854775808")

    def test_cannot_assign_mixed_list(self):
        foo = Node()
        self.graph.create(foo)
        with self.assertRaises(TypeError):
            foo["mixed_list"] = [42, "life", "universe", "everything"]

    def test_cannot_assign_dict(self):
        foo = Node()
        self.graph.create(foo)
        with self.assertRaises(TypeError):
            foo["dict"] = {"foo": 3, "bar": 4, "baz": 5}

    def test_relative_uri_of_bound_node(self):
        a = Node()
        self.graph.create(a)
        relative_uri_string = a.ref
        assert a.uri.string.endswith(relative_uri_string)
        assert relative_uri_string.startswith("node/")

    def test_relative_uri_of_unbound_node(self):
        a = Node()
        with self.assertRaises(BindError):
            _ = a.ref

    def test_node_hashes(self):
        node_1 = Node("Person", name="Alice")
        self.graph.create(node_1)
        node_2 = Node("Person", name="Alice")
        node_2.bind(node_1.uri)
        assert node_1 is not node_2
        assert hash(node_1) == hash(node_2)

    def test_node_str(self):
        a = Node("Person", name="Alice")
        assert str(a) == '(:Person {name:"Alice"})'
        self.graph.create(a)
        assert str(a) == '(n%s:Person {name:"Alice"})' % a._id
