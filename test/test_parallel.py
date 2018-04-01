#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2018, Nigel Small
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


from test.util import GraphTestCase


class ParallelTestCase(GraphTestCase):

    def test_can_run_cypher_while_in_transaction(self):
        tx = self.graph.begin()
        outer_result = tx.run("UNWIND range(1, 10) AS n RETURN n")
        inner_result = self.graph.run("CREATE (a) RETURN a")
        outer_result_list = list(map(tuple, outer_result))
        tx.rollback()
        record = inner_result.next()
        created = record[0]
        assert outer_result_list == [(1,), (2,), (3,), (4,), (5,), (6,), (7,), (8,), (9,), (10,)]
        assert self.graph.exists(created)
