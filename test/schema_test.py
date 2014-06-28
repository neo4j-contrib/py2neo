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


from __future__ import unicode_literals

from uuid import uuid4

import pytest

from py2neo import neo4j, node, Graph
from py2neo.packages.httpstream.http import ServerError


def get_clean_database():
    # Constraints have to be removed before the indexed property keys can be removed.
    graph = neo4j.Graph()
    for label in graph.node_labels:
        for key in graph.schema.get_unique_constraints(label):
            graph.schema.remove_unique_constraint(label, key)
        for key in graph.schema.get_indexed_property_keys(label):
            graph.schema.drop_index(label, key)
    return graph


@pytest.skip(not Graph().supports_node_labels)
def test_schema_index():
    graph_db = get_clean_database()
    label_1 = uuid4().hex
    label_2 = uuid4().hex
    munich, = graph_db.create({'name': "München", 'key': "09162000"})
    munich.add_labels(label_1, label_2)
    graph_db.schema.create_index(label_1, "name")
    graph_db.schema.create_index(label_1, "key")
    graph_db.schema.create_index(label_2, "name")
    graph_db.schema.create_index(label_2, "key")
    found_borough_via_name = graph_db.find(label_1, "name", "München")
    found_borough_via_key = graph_db.find(label_1, "key", "09162000")
    found_county_via_name = graph_db.find(label_2, "name", "München")
    found_county_via_key = graph_db.find(label_2, "key", "09162000")
    assert list(found_borough_via_name) == list(found_borough_via_key)
    assert list(found_county_via_name) == list(found_county_via_key)
    assert list(found_borough_via_name) == list(found_county_via_name)
    keys = graph_db.schema.get_indexed_property_keys(label_1)
    assert "name" in keys
    assert "key" in keys
    graph_db.schema.drop_index(label_1, "name")
    graph_db.schema.drop_index(label_1, "key")
    graph_db.schema.drop_index(label_2, "name")
    graph_db.schema.drop_index(label_2, "key")
    with pytest.raises(LookupError):
        graph_db.schema.drop_index(label_2, "key")
    graph_db.delete(munich)


@pytest.skip(not Graph().supports_node_labels)
def test_unique_constraint():
    graph_db = get_clean_database()
    label_1 = uuid4().hex
    borough, = graph_db.create(node(name="Taufkirchen"))
    borough.add_labels(label_1)
    graph_db.schema.add_unique_constraint(label_1, "name")
    constraints = graph_db.schema.get_unique_constraints(label_1)
    assert "name" in constraints
    borough_2, = graph_db.create(node(name="Taufkirchen"))
    with pytest.raises(ValueError):
        borough_2.add_labels(label_1)
    graph_db.delete(borough, borough_2)


@pytest.skip(not Graph().supports_node_labels)
def test_labels_constraints():
    graph_db = get_clean_database()
    label_1 = uuid4().hex
    a, b = graph_db.create({"name": "Alice"}, {"name": "Alice"})
    a.add_labels(label_1)
    b.add_labels(label_1)
    with pytest.raises(ValueError):
        graph_db.schema.add_unique_constraint(label_1, "name")
    b.remove_labels(label_1)
    graph_db.schema.add_unique_constraint(label_1, "name")
    a.remove_labels(label_1)
    b.add_labels(label_1)
    with pytest.raises(ServerError):
        graph_db.schema.drop_index(label_1, "name")
    b.remove_labels(label_1)
    graph_db.schema.remove_unique_constraint(label_1, "name")
    with pytest.raises(LookupError):
        graph_db.schema.drop_index(label_1, "name")
    graph_db.delete(a, b)
