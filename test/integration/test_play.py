#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2020, Nigel Small
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


from pytest import mark, raises


def test_can_play_simple_function(graph):
    collected = []

    def work(tx):
        collected.append(tx.evaluate("RETURN 1"))

    graph.play(work)
    assert collected == [1]


def test_can_play_parameterised_function(graph):
    collected = []

    def work(tx, x):
        collected.append(tx.evaluate("RETURN $x", x=x))

    graph.play(work, args=[1])
    assert collected == [1]


@mark.skip("Chaining functionality not yet implemented")
def test_can_play_chained_functions(graph):
    collected = []

    def work(tx, x):
        collected.append(tx.evaluate("RETURN $x", x=x))

    first = graph.play(work, args=[1])
    if graph.service.connector.profile.protocol == "bolt":
        graph.play(work, args=[2], after=first)
        assert collected == [1, 2]
    else:
        with raises(TypeError):
            graph.play(work, args=[2], after=first)
        assert collected == [1]


@mark.skip("Chaining functionality not yet implemented")
def test_can_play_multi_chained_functions(graph):
    collected = []

    def work(tx, x):
        collected.append(tx.evaluate("RETURN $x", x=x))

    first = graph.play(work, args=[1])
    second = graph.play(work, args=[2])
    if graph.service.connector.profile.protocol == "bolt":
        graph.play(work, args=[3], after=(first, second))
        assert collected == [1, 2, 3]
    else:
        with raises(TypeError):
            graph.play(work, args=[3], after=(first, second))
        assert collected == [1, 2]
