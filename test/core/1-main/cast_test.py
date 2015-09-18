#!/usr/bin/env python
# -*- encoding: utf-8 -*-

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


from py2neo.http import Node, NodePointer, Rel, Relationship, cast, cast_to_node, cast_to_rel


def test_graph_cast():
    assert cast(None) is None


def test_node_cast():
    alice = Node("Person", "Employee", name="Alice", age=33)
    assert cast_to_node() == Node()
    assert cast_to_node(None) is None
    assert cast_to_node(alice) is alice
    assert cast_to_node("Person") == Node("Person")
    assert cast_to_node(name="Alice") == Node(name="Alice")
    assert cast_to_node("Person", "Employee", name="Alice", age=33) == alice
    assert cast_to_node({"name": "Alice"}) == Node(name="Alice")
    assert cast_to_node(("Person", "Employee", {"name": "Alice", "age": 33})) == alice
    assert cast_to_node(42) == NodePointer(42)
    assert cast_to_node(NodePointer(42)) == NodePointer(42)
    try:
        cast_to_node(3.14)
    except TypeError:
        assert True
    else:
        assert False


def test_rel_cast():
    knows = Rel("KNOWS", since=1999)
    assert cast_to_rel() == Rel()
    assert cast_to_rel(None) is None
    assert cast_to_rel(knows) is knows
    assert cast_to_rel("KNOWS") == Rel("KNOWS")
    assert cast_to_rel(since=1999) == Rel(since=1999)
    assert cast_to_rel("KNOWS", since=1999) == Rel("KNOWS", since=1999)
    assert cast_to_rel({"since": 1999}) == Rel(since=1999)
    assert cast_to_rel(("KNOWS", {"since": 1999})) == knows
    assert cast_to_rel(Relationship({}, "KNOWS", {})) == Rel("KNOWS")
