#/usr/bin/env python
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


from py2neo.cypher.task import CypherTask, CreateNode, MergeNode


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
    assert snip.statement == "CREATE ()"
    assert snip.parameters == {}


def test_create_empty_node_with_return():
    snip = CreateNode().with_return()
    assert snip.statement == "CREATE (a) RETURN a"
    assert snip.parameters == {}


def test_create_node_with_label():
    snip = CreateNode("Person")
    assert snip.statement == "CREATE (:Person)"
    assert snip.parameters == {}


def test_create_node_with_label_and_return():
    snip = CreateNode("Person").with_return()
    assert snip.statement == "CREATE (a:Person) RETURN a"
    assert snip.parameters == {}


def test_create_node_with_labels():
    snip = CreateNode("Homo Sapiens", "Female")
    assert snip.labels == {"Homo Sapiens", "Female"}
    assert snip.statement == "CREATE (:Female:`Homo Sapiens`)"
    assert snip.parameters == {}


def test_create_node_with_labels_and_return():
    snip = CreateNode("Homo Sapiens", "Female").with_return()
    assert snip.statement == "CREATE (a:Female:`Homo Sapiens`) RETURN a"
    assert snip.parameters == {}


def test_create_node_with_labels_and_properties():
    snip = CreateNode("Homo Sapiens", "Female", name="Alice", age=33, active=True)
    assert snip.statement == "CREATE (:Female:`Homo Sapiens` {P})"
    assert snip.parameters == {"P": {"name": "Alice", "age": 33, "active": True}}


def test_create_node_with_labels_and_properties_and_return():
    snip = CreateNode("Homo Sapiens", "Female", name="Alice", age=33, active=True).with_return()
    assert snip.statement == "CREATE (a:Female:`Homo Sapiens` {P}) RETURN a"
    assert snip.parameters == {"P": {"name": "Alice", "age": 33, "active": True}}


def test_create_node_with_set():
    snip = CreateNode().set("Person", name="Alice")
    assert snip.statement == "CREATE (:Person {P})"
    assert snip.parameters == {"P": {"name": "Alice"}}


def test_create_node_with_set_and_return():
    snip = CreateNode().set("Person", name="Alice").with_return()
    assert snip.statement == "CREATE (a:Person {P}) RETURN a"
    assert snip.parameters == {"P": {"name": "Alice"}}


def test_merge_node():
    snip = MergeNode("Person", "name", "Alice")
    assert snip.statement == "MERGE (:Person {name:{V}})"
    assert snip.parameters == {"V": "Alice"}


def test_merge_node_with_return():
    snip = MergeNode("Person", "name", "Alice").with_return()
    assert snip.statement == "MERGE (a:Person {name:{V}}) RETURN a"
    assert snip.parameters == {"V": "Alice"}


def test_merge_node_without_property():
    snip = MergeNode("Person")
    assert snip.primary_label == "Person"
    assert snip.primary_key is None
    assert snip.primary_value is None
    assert snip.statement == "MERGE (:Person)"
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
    assert snip.statement == "MERGE (a:Person {name:{V}}) SET a:Employee SET a={P}"
    assert snip.parameters == {"V": "Alice", "P": {"employee_id": 1234, "name": "Alice"}}


def test_merge_node_with_extra_values_and_return():
    snip = MergeNode("Person", "name", "Alice").set("Employee", employee_id=1234).with_return()
    assert snip.statement == "MERGE (a:Person {name:{V}}) SET a:Employee SET a={P} RETURN a"
    assert snip.parameters == {"V": "Alice", "P": {"employee_id": 1234, "name": "Alice"}}
