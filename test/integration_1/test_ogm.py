#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2019, Nigel Small
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


from os.path import dirname, join as path_join

from pytest import fixture

from py2neo.ogm import RelatedObjects, Property, RelatedTo, RelatedFrom, OUTGOING, GraphObject, Label
from py2neo.matching import NodeMatcher


class SimpleThing(GraphObject):
    pass


def test_simple_create(graph):
    thing = SimpleThing()
    graph.create(thing)
    assert thing.__node__.graph is graph
    assert thing.__node__.identity is not None


def test_simple_merge(graph):
    thing = SimpleThing()
    graph.merge(thing)
    assert thing.__node__.graph is graph
    assert thing.__node__.identity is not None


def test_simple_push(graph):
    thing = SimpleThing()
    graph.push(thing)
    assert thing.__node__.graph is graph
    assert thing.__node__.identity is not None


class A(GraphObject):

    b = RelatedTo("B")


class B(GraphObject):

    a = RelatedFrom("A")


def test_crossover(graph):
    a = A()
    b = B()
    a.b.add(b)
    b.a.add(a)
    graph.create(a)
    graph.create(b)
