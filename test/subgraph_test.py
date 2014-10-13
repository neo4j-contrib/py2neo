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
    assert len(s.nodes) == 0
    assert len(s.relationships) == 0


def test_subgraph_with_single_node():
    s = Subgraph(Node("Person", name="Alice"))
    assert len(s) == 0
    assert len(s.nodes) == 1
    assert len(s.relationships) == 0


def test_subgraph_with_single_relationship():
    s = Subgraph(({"name": "Alice"}, "KNOWS", {"name": "Bob"}))
    assert len(s) == 1
    assert len(s.nodes) == 2
    assert len(s.relationships) == 1
