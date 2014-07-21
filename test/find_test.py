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
from py2neo import GraphError


try:
    from unittest.mock import patch, Mock
except ImportError:
    from mock import patch
from uuid import uuid4

from py2neo.packages.httpstream import Resource as _Resource


def test_will_find_no_nodes_with_non_existent_label(graph):
    if not graph.supports_node_labels:
        return
    nodes = list(graph.find(uuid4().hex))
    assert nodes == []


def test_can_find_nodes_with_label(graph):
    if not graph.supports_node_labels:
        return
    alice, = graph.create({"name": "Alice"})
    alice.add_labels("Person")
    nodes = list(graph.find("Person"))
    assert alice in nodes


def test_can_find_nodes_with_label_and_property(graph):
    if not graph.supports_node_labels:
        return
    alice, = graph.create({"name": "Alice"})
    alice.add_labels("Person")
    nodes = list(graph.find("Person", "name", "Alice"))
    assert alice in nodes


def test_can_handle_not_found_error_as_no_nodes(graph):
    with patch.object(_Resource, "get") as mocked:
        error = GraphError("")
        error.response = Mock()
        error.response.status_code = 404
        mocked.side_effect = error
        nodes = list(graph.find(""))
        assert nodes == []


def test_will_raise_on_other_error(graph):
    with patch.object(_Resource, "get") as mocked:
        error = GraphError("")
        error.response = Mock()
        error.response.status_code = 500
        mocked.side_effect = error
        try:
            nodes = list(graph.find(""))
        except GraphError:
            assert True
        else:
            assert False
