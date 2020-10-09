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


from py2neo import Node, Relationship
from py2neo.compat import long


def test_single_node_creation(graph):
    a = Node("Person", name="Alice")
    assert a.labels == {"Person"}
    assert a["name"] == "Alice"
    graph.create(a)
    assert isinstance(a.identity, int)
    assert graph.exists(a)


def test_can_create_local_node(graph):
    a = Node("Person", name="Alice", age=33)
    assert set(a.labels) == {"Person"}
    assert dict(a) == {"name": "Alice", "age": 33}


def test_can_create_remote_node(graph):
    a = Node("Person", name="Alice", age=33)
    graph.create(a)
    assert set(a.labels) == {"Person"}
    assert dict(a) == {"name": "Alice", "age": 33}


def test_bound_node_equals_unbound_node_with_same_properties(graph):
    alice_1 = Node(name="Alice")
    alice_1.graph = graph
    alice_1.identity = 999
    alice_2 = Node(name="Alice")
    assert set(alice_1.labels) == set(alice_2.labels)
    assert dict(alice_1) == dict(alice_2)


def test_bound_node_equality(graph):
    alice_1 = Node(name="Alice")
    alice_1.graph = graph
    alice_1.identity = 999
    alice_2 = Node(name="Alice")
    alice_2.graph = alice_1.graph
    alice_2.identity = alice_1.identity
    assert alice_1 == alice_2


def test_unbound_node_equality(graph):
    alice_1 = Node("Person", name="Alice")
    alice_2 = Node("Person", name="Alice")
    assert set(alice_1.labels) == set(alice_2.labels)
    assert dict(alice_1) == dict(alice_2)


def test_can_merge_unsaved_changes_when_querying_node(graph):
    a = Node("Person", name="Alice")
    b = Node()
    graph.create(a | b | Relationship(a, "KNOWS", b))
    assert dict(a) == {"name": "Alice"}
    a["age"] = 33
    assert dict(a) == {"name": "Alice", "age": 33}
    _ = list(graph.match((a, None), "KNOWS"))
    assert dict(a) == {"name": "Alice", "age": 33}


def test_pull_node_labels_if_stale(graph):
    a = Node("Thing")
    graph.create(a)
    a.remove_label("Thing")
    a._stale.add("labels")
    labels = a.labels
    assert set(labels) == {"Thing"}


def test_pull_node_property_if_stale(graph):
    a = Node(foo="bar")
    graph.create(a)
    a["foo"] = None
    a._stale.add("properties")
    assert a["foo"] == "bar"


def test_can_create_concrete_node(graph):
    alice = Node.cast({"name": "Alice", "age": 34})
    graph.create(alice)
    assert isinstance(alice, Node)
    assert alice["name"] == "Alice"
    assert alice["age"] == 34


def test_all_property_types(graph):
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
    foo = Node.cast(data)
    graph.create(foo)
    for key, value in data.items():
        assert foo[key] == value


def test_node_hashes(graph):
    node_1 = Node("Person", name="Alice")
    node_1.graph = graph
    node_1.identity = 999
    node_2 = Node("Person", name="Alice")
    node_2.graph = node_1.graph
    node_2.identity = node_1.identity
    assert node_1 is not node_2
    assert hash(node_1) == hash(node_2)


def test_cannot_delete_uncreated_node(graph):
    a = Node()
    graph.delete(a)


def test_node_exists(graph):
    a = Node()
    graph.create(a)
    assert graph.exists(a)


def test_node_does_not_exist(graph):
    a = Node()
    assert not graph.exists(a)


def test_can_name_using_name_property(graph):
    a = Node(name="Alice")
    assert a.__name__ == "Alice"


def test_can_name_using_magic_name_property(graph):
    a = Node(__name__="Alice")
    assert a.__name__ == "Alice"
