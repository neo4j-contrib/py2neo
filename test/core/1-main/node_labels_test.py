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


try:
    from unittest.mock import Mock, patch
except ImportError:
    from mock import Mock, patch

from py2neo import Node
from py2neo.http import LabelSet


def test_label_set_equality():
    label_set_1 = LabelSet({"foo"})
    label_set_2 = LabelSet({"foo"})
    assert label_set_1 == label_set_2


def test_label_set_inequality():
    label_set_1 = LabelSet({"foo"})
    label_set_2 = LabelSet({"bar"})
    assert label_set_1 != label_set_2


def test_can_get_all_node_labels(graph):
    labels = graph.node_labels
    assert isinstance(labels, frozenset)


def test_can_create_node_with_labels(graph):
    alice = Node("Person", name="Alice")
    assert alice.labels == {"Person"}


def test_can_add_labels_to_existing_node(graph):
    alice = Node(name="Alice")
    alice.labels.add("Person")
    assert alice.labels == {"Person"}


def test_can_remove_labels_from_existing_node(graph):
    alice = Node("Person", name="Alice")
    alice.labels.remove("Person")
    assert alice.labels == set()


def test_can_replace_labels_on_existing_node(graph):
    alice = Node("Person", name="Alice")
    alice.labels.replace({"Employee"})
    assert alice.labels == {"Employee"}
