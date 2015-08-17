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


from py2neo import Node, NodePointer
from py2neo.cypher.task import CypherTask, CreateNode, MergeNode, CreateRelationship


def test_bare_task():
    statement = "spam"
    parameters = {"foo": "bar"}
    snip = CypherTask(statement, parameters)
    assert snip.__repr__() == "<CypherTask statement=%r parameters=%r>" % (statement, parameters)
    assert snip.__str__() == statement
    assert snip.__unicode__() == statement
    assert snip.statement == statement
    assert snip.parameters == parameters


def test_create_empty_node():
    snip = CreateNode()
    assert snip.statement == "CREATE (a)"
    assert snip.parameters == {}


def test_create_empty_node_with_return():
    snip = CreateNode().with_return()
    assert snip.statement == "CREATE (a) RETURN a"
    assert snip.parameters == {}


def test_create_node_with_label():
    snip = CreateNode("Person")
    assert snip.statement == "CREATE (a:Person)"
    assert snip.parameters == {}


def test_create_node_with_label_and_return():
    snip = CreateNode("Person").with_return()
    assert snip.statement == "CREATE (a:Person) RETURN a"
    assert snip.parameters == {}


def test_create_node_with_labels():
    snip = CreateNode("Homo Sapiens", "Female")
    assert snip.labels == {"Homo Sapiens", "Female"}
    assert snip.statement == "CREATE (a:Female:`Homo Sapiens`)"
    assert snip.parameters == {}


def test_create_node_with_labels_and_return():
    snip = CreateNode("Homo Sapiens", "Female").with_return()
    assert snip.statement == "CREATE (a:Female:`Homo Sapiens`) RETURN a"
    assert snip.parameters == {}


def test_create_node_with_labels_and_properties():
    snip = CreateNode("Homo Sapiens", "Female", name="Alice", age=33, active=True)
    assert snip.statement == "CREATE (a:Female:`Homo Sapiens` {A})"
    assert snip.parameters == {"A": {"name": "Alice", "age": 33, "active": True}}


def test_create_node_with_labels_and_properties_and_return():
    snip = CreateNode("Homo Sapiens", "Female", name="Alice", age=33, active=True).with_return()
    assert snip.statement == "CREATE (a:Female:`Homo Sapiens` {A}) RETURN a"
    assert snip.parameters == {"A": {"name": "Alice", "age": 33, "active": True}}


def test_create_node_with_set():
    snip = CreateNode().set("Person", name="Alice")
    assert snip.statement == "CREATE (a:Person {A})"
    assert snip.parameters == {"A": {"name": "Alice"}}


def test_create_node_with_set_and_return():
    snip = CreateNode().set("Person", name="Alice").with_return()
    assert snip.statement == "CREATE (a:Person {A}) RETURN a"
    assert snip.parameters == {"A": {"name": "Alice"}}


def test_merge_node():
    snip = MergeNode("Person", "name", "Alice")
    assert snip.statement == "MERGE (a:Person {name:{A1}})"
    assert snip.parameters == {"A1": "Alice"}


def test_merge_node_with_return():
    snip = MergeNode("Person", "name", "Alice").with_return()
    assert snip.statement == "MERGE (a:Person {name:{A1}}) RETURN a"
    assert snip.parameters == {"A1": "Alice"}


def test_merge_node_without_property():
    snip = MergeNode("Person")
    assert snip.primary_label == "Person"
    assert snip.primary_key is None
    assert snip.primary_value is None
    assert snip.statement == "MERGE (a:Person)"
    assert snip.parameters == {}


def test_merge_node_without_property_with_return():
    snip = MergeNode("Person").with_return()
    assert snip.primary_label == "Person"
    assert snip.primary_key is None
    assert snip.primary_value is None
    assert snip.statement == "MERGE (a:Person) RETURN a"
    assert snip.parameters == {}


def test_merge_node_with_extra_values():
    snip = MergeNode("Person", "name", "Alice").set("Employee", employee_id=1234)
    assert snip.labels == {"Person", "Employee"}
    assert snip.statement == "MERGE (a:Person {name:{A1}}) SET a:Employee SET a={A}"
    assert snip.parameters == {"A1": "Alice", "A": {"employee_id": 1234, "name": "Alice"}}


def test_merge_node_with_extra_values_and_return():
    snip = MergeNode("Person", "name", "Alice").set("Employee", employee_id=1234).with_return()
    assert snip.statement == "MERGE (a:Person {name:{A1}}) SET a:Employee SET a={A} RETURN a"
    assert snip.parameters == {"A1": "Alice", "A": {"employee_id": 1234, "name": "Alice"}}


def test_create_relationship_and_both_nodes():
    t = CreateRelationship(Node("Person", name="Alice"), "KNOWS", Node("Person", name="Bob"))
    assert t.statement == "CREATE (a:Person {A}) CREATE (b:Person {B}) CREATE (a)-[r:KNOWS]->(b)"
    assert t.parameters == {"A": {"name": "Alice"}, "B": {"name": "Bob"}}


def test_create_relationship_with_properties_and_both_nodes():
    t = CreateRelationship(Node("Person", name="Alice"), "KNOWS", Node("Person", name="Bob"),
                           since=1999)
    assert t.statement == "CREATE (a:Person {A}) " \
                          "CREATE (b:Person {B}) " \
                          "CREATE (a)-[r:KNOWS {R}]->(b)"
    assert t.parameters == {"A": {"name": "Alice"}, "B": {"name": "Bob"}, "R": {"since": 1999}}


def test_create_relationship_and_start_node():
    alice = Node("Person", name="Alice")
    alice.bind("http://localhost:7474/db/data/node/1")
    t = CreateRelationship(alice, "KNOWS", Node("Person", name="Bob"))
    assert t.statement == "MATCH (a) WHERE id(a)={A} " \
                          "CREATE (b:Person {B}) " \
                          "CREATE (a)-[r:KNOWS]->(b)"
    assert t.parameters == {"A": 1, "B": {"name": "Bob"}}


def test_create_relationship_and_end_node():
    bob = Node("Person", name="Bob")
    bob.bind("http://localhost:7474/db/data/node/2")
    t = CreateRelationship(Node("Person", name="Alice"), "KNOWS", bob)
    assert t.statement == "CREATE (a:Person {A}) " \
                          "MATCH (b) WHERE id(b)={B} " \
                          "CREATE (a)-[r:KNOWS]->(b)"
    assert t.parameters == {"A": {"name": "Alice"}, "B": 2}


def test_create_relationship_only():
    alice = Node("Person", name="Alice")
    alice.bind("http://localhost:7474/db/data/node/1")
    bob = Node("Person", name="Bob")
    bob.bind("http://localhost:7474/db/data/node/2")
    t = CreateRelationship(alice, "KNOWS", bob)
    assert t.statement == "MATCH (a) WHERE id(a)={A} " \
                          "MATCH (b) WHERE id(b)={B} " \
                          "CREATE (a)-[r:KNOWS]->(b)"
    assert t.parameters == {"A": 1, "B": 2}
