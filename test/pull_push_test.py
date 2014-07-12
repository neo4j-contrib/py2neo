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


import pytest

from py2neo.core import Graph, Node


def test_can_pull_node(graph):
    if not graph.supports_node_labels:
        return
    local = Node()
    remote = Node("Person", name="Alice")
    graph.create(remote)
    assert local.labels == set()
    assert local.properties == {}
    local.bind(remote.uri)
    local.pull()
    assert local.labels == remote.labels
    assert local.properties == remote.properties


def test_can_push_node(graph):
    if not graph.supports_node_labels:
        return
    local = Node("Person", name="Alice")
    remote = Node()
    graph.create(remote)
    assert remote.labels == set()
    assert remote.properties == {}
    local.bind(remote.uri)
    local.push()
    assert remote.labels == remote.labels
    assert remote.properties == remote.properties
