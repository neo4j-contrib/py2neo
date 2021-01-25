#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2021, Nigel Small
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


from pytest import raises

from py2neo import Node, Relationship


def test_can_delete_relationship_by_separating(graph):
    a = Node()
    b = Node()
    r = Relationship(a, "TO", b)
    graph.create(r)
    assert graph.exists(r)
    with graph.begin() as tx:
        tx.separate(r)
    assert not graph.exists(r)
    assert graph.exists(a)
    assert graph.exists(b)


def test_cannot_separate_non_graphy_thing(graph):
    with raises(TypeError):
        graph.separate("this string is definitely not graphy")
