#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2021, Nigel Small
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


from pytest import raises

from py2neo import Node, NodeMatcher, Neo4jError


def test_can_get_all_node_labels(graph):
    types = graph.schema.node_labels
    assert isinstance(types, frozenset)


def test_can_get_all_relationship_types(graph):
    types = graph.schema.relationship_types
    assert isinstance(types, frozenset)


def test_simple_uniqueness_constraint(graph, make_unique_id):
    label = make_unique_id()
    a1 = Node(label, name="Alice")
    graph.create(a1)
    graph.schema.create_uniqueness_constraint(label, "name")
    constraints = graph.schema.get_uniqueness_constraints(label)
    assert constraints == ["name"]


def test_unique_constraint_creation_failure(graph, make_unique_id):
    label = make_unique_id()
    a1 = Node(label, name="Alice")
    a2 = Node(label, name="Alice")
    graph.create(a1 | a2)
    with raises(Neo4jError) as e:
        graph.schema.create_uniqueness_constraint(label, "name")
    assert e.value.code == "Neo.DatabaseError.Schema.ConstraintCreationFailed"


def test_schema_index(graph, make_unique_id):
    node_matcher = NodeMatcher(graph)
    label_1 = make_unique_id()
    label_2 = make_unique_id()
    munich = Node(name="München", key="09162000")
    graph.create(munich)
    munich.clear_labels()
    munich.update_labels({label_1, label_2})
    graph.schema.create_index(label_1, "name")
    graph.schema.create_index(label_1, "key")
    graph.schema.create_index(label_2, "name")
    graph.schema.create_index(label_2, "key")
    found_borough_via_name = node_matcher.match(label_1, name="München")
    found_borough_via_key = node_matcher.match(label_1, key="09162000")
    found_county_via_name = node_matcher.match(label_2, name="München")
    found_county_via_key = node_matcher.match(label_2, key="09162000")
    assert list(found_borough_via_name) == list(found_borough_via_key)
    assert list(found_county_via_name) == list(found_county_via_key)
    assert list(found_borough_via_name) == list(found_county_via_name)
    keys = graph.schema.get_indexes(label_1)
    assert (u"name",) in keys
    assert (u"key",) in keys
    graph.schema.drop_index(label_1, "name")
    graph.schema.drop_index(label_1, "key")
    graph.schema.drop_index(label_2, "name")
    graph.schema.drop_index(label_2, "key")
    with raises(Neo4jError) as e:
        graph.schema.drop_index(label_2, "key")
    assert e.value.code == "Neo.DatabaseError.Schema.IndexDropFailed"
    graph.delete(munich)


def test_labels_constraints(graph, make_unique_id):
    label_1 = make_unique_id()
    a = Node(label_1, name="Alice")
    b = Node(label_1, name="Alice")
    graph.create(a | b)
    with raises(Neo4jError) as e:
        graph.schema.create_uniqueness_constraint(label_1, "name")
    assert e.value.code == "Neo.DatabaseError.Schema.ConstraintCreationFailed"
    b.remove_label(label_1)
    graph.push(b)
    graph.schema.create_uniqueness_constraint(label_1, "name")
    a.remove_label(label_1)
    graph.push(a)
    b.add_label(label_1)
    graph.push(b)
    b.remove_label(label_1)
    graph.push(b)
    graph.schema.drop_uniqueness_constraint(label_1, "name")
    with raises(Neo4jError) as e:
        graph.schema.drop_uniqueness_constraint(label_1, "name")
    assert e.value.code == "Neo.DatabaseError.Schema.ConstraintDropFailed"
    graph.delete(a | b)
