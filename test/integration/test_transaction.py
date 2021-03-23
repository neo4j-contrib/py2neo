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


class TestAutoCommitTransaction(object):

    def test_run(self, graph):
        for record in graph.run("RETURN 1"):
            assert record[0] == 1

    def test_evaluate(self, graph):
        assert graph.evaluate("RETURN 1") == 1

    def test_update(self, graph):
        graph.update("CREATE ()")


class TestExplicitTransaction(object):

    def test_run(self, graph):
        tx = graph.begin()
        for record in tx.run("RETURN 1"):
            assert record[0] == 1
        graph.commit(tx)

    def test_evaluate(self, graph):
        tx = graph.begin()
        assert tx.evaluate("RETURN 1") == 1
        graph.commit(tx)

    def test_update(self, graph):
        tx = graph.begin()
        tx.update("CREATE ()")
        graph.commit(tx)
