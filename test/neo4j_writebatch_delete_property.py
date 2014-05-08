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


def test_can_delete_property_on_preexisting_node(graph):
    alice, = graph.create({"name": "Alice", "age": 34})
    batch = neo4j.WriteBatch(graph)
    batch.delete_property(alice, "age")
    batch.run()
    assert alice["name"] == "Alice"
    assert alice["age"] is None


def test_can_delete_property_on_node_in_same_batch(graph):
    batch = neo4j.WriteBatch(graph)
    alice = batch.create({"name": "Alice", "age": 34})
    batch.delete_property(alice, "age")
    results = batch.submit()
    alice = results[batch.find(alice)]
    assert alice["name"] == "Alice"
    assert alice["age"] is None
