#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2015, Nigel Small
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


from py2neo.core import Node, Relationship, Subgraph

from test.util import Py2neoTestCase


class SubgraphTestCase(Py2neoTestCase):

    def test_can_build_subgraph_from_single_node(self):
        a = Node()
        s = Subgraph(a)
        assert s.order() == 1
        assert s.size() == 0
        assert set(s.nodes()) == {a}
        assert set(s.relationships()) == set()

    def test_can_build_subgraph_from_nodes_and_relationships(self):
        a = Node()
        b = Node()
        r = Relationship(a, "TO", b)
        s = Subgraph(a, b, r)
        assert s.order() == 2
        assert s.size() == 1
        assert set(s.nodes()) == {a, b}
        assert set(s.relationships()) == {r}

    def test_can_build_subgraph_from_single_relationship(self):
        a = Node()
        b = Node()
        r = Relationship(a, "TO", b)
        s = Subgraph(r)
        assert s.order() == 2
        assert s.size() == 1
        assert set(s.nodes()) == {a, b}
        assert set(s.relationships()) == {r}

    def test_can_build_subgraph_from_castable_values(self):
        s = Subgraph({"name": "Alice"}, {"name": "Bob"}, (0, "KNOWS", 1))
        assert s.order() == 2
        assert s.size() == 1
