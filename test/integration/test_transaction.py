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


def test_run_in_auto_tx(graph):
    tx = graph.auto()
    for record in tx.run("RETURN 1"):
        assert record[0] == 1


def test_run_in_explicit_tx(graph):
    tx = graph.begin()
    for record in tx.run("RETURN 1"):
        assert record[0] == 1
    tx.commit()


def test_evaluate_in_auto_tx(graph):
    tx = graph.auto()
    assert tx.evaluate("RETURN 1") == 1


def test_evaluate_in_explicit_tx(graph):
    tx = graph.begin()
    assert tx.evaluate("RETURN 1") == 1
    tx.commit()


def test_update_in_auto_tx(graph):
    tx = graph.auto()
    tx.update("CREATE ()")


def test_update_in_explicit_tx(graph):
    tx = graph.begin()
    tx.update("CREATE ()")
    tx.commit()
