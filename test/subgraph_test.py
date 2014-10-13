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


from py2neo import Subgraph, Node


def test_empty_subgraph():
    s = Subgraph()
    assert len(s) == 0
    assert s.order == 0
    assert s.size == 0


def test_subgraph_with_single_node():
    s = Subgraph(Node("Person", name="Alice"))
    assert len(s) == 0
    assert s.order == 1
    assert s.size == 0


def test_subgraph_with_single_relationship():
    s = Subgraph(({"name": "Alice"}, "KNOWS", {"name": "Bob"}))
    assert len(s) == 1
    assert s.order == 2
    assert s.size == 1


def test_converting_cypher_results_to_subgraph(graph):
    r = graph.cypher.execute(
        "CREATE (a:Person {name:'Alice'})-[ab:KNOWS]->(b:Person {name:'Bob'}) RETURN a, ab, b")
    a, ab, b = r[0]
    s = r.to_subgraph()
    assert len(s) == 1
    assert s.order == 2
    assert s.size == 1
    assert a in s
    assert ab in s
    assert b in s
