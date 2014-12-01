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


from py2neo import *


def test_zero_nodes_are_familiar():
    assert familiar()


def test_one_node_is_familiar(graph):
    a, = graph.create({})
    assert familiar(a)


def test_two_nodes_are_familiar(graph):
    a, b = graph.create({}, {})
    assert familiar(a, b)


def test_three_nodes_are_familiar(graph):
    a, b, c = graph.create({}, {}, {})
    assert familiar(a, b, c)


def test_unbound_node_cannot_be_familiar():
    try:
        assert familiar(Node())
    except ValueError:
        assert True
    else:
        assert False


def test_nodes_from_different_graphs_are_unfamiliar():
    from py2neo.packages.httpstream.http import NetworkAddressError
    a = Node()
    try:
        a.bind("http://foo:7474/db/data/node/1")
    except NetworkAddressError:
        pass
    b = Node()
    try:
        b.bind("http://bar:7474/db/data/node/2")
    except NetworkAddressError:
        pass
    assert not familiar(a, b)
