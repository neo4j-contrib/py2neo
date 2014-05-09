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


from py2neo import neo4j


def test_can_create_path_with_new_nodes(graph):
    batch = neo4j.WriteBatch(graph)
    batch.create_path({"name": "Alice"}, "KNOWS", {"name": "Bob"})
    results = batch.submit()
    path = results[0]
    assert len(path) == 1
    assert path.nodes[0]["name"] == "Alice"
    assert path.relationships[0].type == "KNOWS"
    assert path.nodes[1]["name"] == "Bob"


def test_can_create_path_with_existing_nodes(graph):
    alice, bob = graph.create({"name": "Alice"}, {"name": "Bob"})
    batch = neo4j.WriteBatch(graph)
    batch.create_path(alice, "KNOWS", bob)
    results = batch.submit()
    path = results[0]
    assert len(path) == 1
    assert path.nodes[0] == alice
    assert path.relationships[0].type == "KNOWS"
    assert path.nodes[1] == bob


def test_is_not_idempotent():
    graph = neo4j.Graph()
    alice, = graph.create({"name": "Alice"})
    batch = neo4j.WriteBatch(graph)
    batch.create_path(alice, "KNOWS", {"name": "Bob"})
    results = batch.submit()
    path = results[0]
    bob = path.nodes[1]
    assert path.nodes[0] == alice
    assert bob["name"] == "Bob"
    batch = neo4j.WriteBatch(graph)
    batch.create_path(alice, "KNOWS", {"name": "Bob"})
    results = batch.submit()
    path = results[0]
    assert path.nodes[0] == alice
    assert path.nodes[1] != bob
