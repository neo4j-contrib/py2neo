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


from py2neo import neo4j


def test_can_delete_properties_on_preexisting_node():
    graph_db = neo4j.GraphDatabaseService()
    alice, = graph_db.create({"name": "Alice", "age": 34})
    batch = neo4j.WriteBatch(graph_db)
    batch.delete_properties(alice)
    batch.execute().close()
    props = alice.get_properties()
    assert props == {}


def test_can_delete_properties_on_node_in_same_batch():
    graph_db = neo4j.GraphDatabaseService()
    batch = neo4j.WriteBatch(graph_db)
    alice = batch.create({"name": "Alice", "age": 34})
    batch.delete_properties(alice)
    results = list(batch.execute())
    alice = results[batch.find(alice)]
    props = alice.get_properties()
    assert props == {}
