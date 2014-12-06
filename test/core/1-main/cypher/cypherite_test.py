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


from py2neo.cypher.cypherite import Cypherite, MergeNode


def test_base_cypherite():
    statement = "spam"
    parameters = {"foo": "bar"}
    snip = Cypherite(statement, parameters)
    assert snip.__repr__() == "<Cypherite statement=%r parameters=%r>" % (statement, parameters)
    assert snip.__str__() == statement
    assert snip.__unicode__() == statement
    assert snip.statement == statement
    assert snip.parameters == parameters


def test_can_build_simple_merge_node():
    snip = MergeNode("Person", "name", "Alice")
    assert snip.statement == "MERGE (a:Person {name:{V}})"
    assert snip.parameters == {"V": "Alice"}


def test_can_build_simple_merge_node_with_return():
    snip = MergeNode("Person", "name", "Alice").with_return()
    assert snip.statement == "MERGE (a:Person {name:{V}})\nRETURN a"
    assert snip.parameters == {"V": "Alice"}


def test_can_build_merge_node_without_property():
    snip = MergeNode("Person")
    assert snip.primary_label == "Person"
    assert snip.primary_key is None
    assert snip.primary_value is None
    assert snip.statement == "MERGE (a:Person)"
    assert snip.parameters == {}


def test_can_build_merge_node_without_property_with_return():
    snip = MergeNode("Person").with_return()
    assert snip.primary_label == "Person"
    assert snip.primary_key is None
    assert snip.primary_value is None
    assert snip.statement == "MERGE (a:Person)\nRETURN a"
    assert snip.parameters == {}


def test_can_build_merge_node_with_extra_values():
    snip = MergeNode("Person", "name", "Alice").set("Employee", employee_id=1234)
    assert snip.statement == "MERGE (a:Person {name:{V}})\nSET a:Employee\nSET a={P}"
    assert snip.parameters == {"V": "Alice", "P": {"employee_id": 1234, "name": "Alice"}}


def test_can_build_merge_node_with_extra_values_and_return():
    snip = MergeNode("Person", "name", "Alice").set("Employee", employee_id=1234).with_return()
    assert snip.statement == "MERGE (a:Person {name:{V}})\nSET a:Employee\nSET a={P}\nRETURN a"
    assert snip.parameters == {"V": "Alice", "P": {"employee_id": 1234, "name": "Alice"}}
