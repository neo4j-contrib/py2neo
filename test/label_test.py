#/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright 2011-2013, Nigel Small
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


from py2neo import neo4j, node


def test_can_get_defined_node_labels_if_supported():
    graph_db = neo4j.GraphDatabaseService()
    try:
        labels = graph_db.node_labels
    except NotImplementedError:
        assert True
    else:
        assert isinstance(labels, set)


def test_can_add_labels_to_node():
    graph_db = neo4j.GraphDatabaseService()
    alice, = graph_db.create(node(name="Alice"))
    try:
        alice.labels.add("human", "female")
    except NotImplementedError:
        assert True
    else:
        labels = alice.labels
        assert isinstance(labels, neo4j.LabelSet)
        assert len(labels) == 2
        assert "human" in labels
        assert "female" in labels


def test_can_remove_labels_from_node():
    graph_db = neo4j.GraphDatabaseService()
    alice, = graph_db.create(node(name="Alice"))
    try:
        alice.labels.add("human", "female")
    except NotImplementedError:
        assert True
    else:
        labels = alice.labels
        assert len(labels) == 2
        assert "human" in labels
        assert "female" in labels
        alice.labels.remove("human")
        labels = alice.labels
        assert len(labels) == 1
        assert "human" not in labels
        assert "female" in labels
        alice.labels.remove("female")
        labels = alice.labels
        assert len(labels) == 0
        assert "human" not in labels
        assert "female" not in labels


def test_can_replace_labels_on_node():
    graph_db = neo4j.GraphDatabaseService()
    alice, = graph_db.create(node(name="Alice"))
    try:
        alice.labels.add("human", "female")
    except NotImplementedError:
        assert True
    else:
        labels = alice.labels
        assert isinstance(labels, neo4j.LabelSet)
        assert len(labels) == 2
        assert "human" in labels
        assert "female" in labels
        alice.labels.replace("mystery", "badger")
        labels = alice.labels
        assert len(labels) == 2
        assert "human" not in labels
        assert "female" not in labels
        assert "mystery" in labels
        assert "badger" in labels
