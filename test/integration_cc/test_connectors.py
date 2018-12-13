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


from __future__ import absolute_import

from py2neo.database import CypherStats, CypherPlan
from py2neo.testing import ClusterIntegrationTestCase, for_each_connector


class ConnectorTestCase(ClusterIntegrationTestCase):

    @for_each_connector
    def test_server_agent(self, connector):
        expected = "Neo4j/{}".format(self.server_version)
        actual = connector.server_agent
        self.assertEqual(expected, actual)

    @for_each_connector
    def test_keys(self, connector):
        cursor = connector.run("RETURN 'Alice' AS name, 33 AS age")
        expected = ["name", "age"]
        actual = cursor.keys()
        self.assertSequenceEqual(expected, actual)

    @for_each_connector
    def test_records(self, connector):
        cursor = connector.run("UNWIND range(1, $x) AS n RETURN n, n * n AS n_sq", {"x": 3})
        expected = [[1, 1], [2, 4], [3, 9]]
        actual = list(cursor)
        self.assertSequenceEqual(expected, actual)

    @for_each_connector
    def test_stats(self, connector):
        cursor = connector.run("CREATE ()", {})
        expected = CypherStats(nodes_created=1)
        actual = cursor.stats()
        self.assertEqual(expected, actual)

    @for_each_connector
    def test_explain_plan(self, connector):
        cursor = connector.run("EXPLAIN RETURN $x", {"x": 1})
        expected = CypherPlan(
            operator_type='ProduceResults',
            identifiers=['$x'],
            children=[
                CypherPlan(
                    operator_type='Projection',
                    identifiers=['$x'],
                    children=[],
                    args={
                        'estimated_rows': 1.0,
                        'expressions': '{$x : $x}',
                    },
                ),
            ],
            args={
                'estimated_rows': 1.0,
                'planner': 'COST',
                'planner_impl': 'IDP',
                'planner_version': '3.4',
                'runtime': 'COMPILED',
                'runtime_impl': 'COMPILED',
                'runtime_version': '3.4',
                'version': 'CYPHER 3.4',
            },
        )
        actual = cursor.plan()
        self.assertEqual(expected, actual)

    @for_each_connector
    def test_profile_plan(self, connector):
        cursor = connector.run("PROFILE RETURN $x", {"x": 1})
        actual = cursor.plan()
        expected = CypherPlan(
            operator_type='ProduceResults',
            identifiers=['$x'],
            children=[
                CypherPlan(
                    operator_type='Projection',
                    identifiers=['$x'],
                    children=[],
                    args={
                        'db_hits': 0,
                        'estimated_rows': 1.0,
                        'expressions': '{$x : $x}',
                        'page_cache_hit_ratio': 0.0,
                        'page_cache_hits': 0,
                        'page_cache_misses': 0,
                        'rows': 1,
                        'time': actual.children[0].args["time"],
                    },
                ),
            ],
            args={
                'db_hits': 0,
                'estimated_rows': 1.0,
                'page_cache_hit_ratio': 0.0,
                'page_cache_hits': 0,
                'page_cache_misses': 0,
                'planner': 'COST',
                'planner_impl': 'IDP',
                'planner_version': '3.4',
                'rows': 1,
                'runtime': 'COMPILED',
                'runtime_impl': 'COMPILED',
                'runtime_version': '3.4',
                'time': actual.args["time"],
                'version': 'CYPHER 3.4',
            },
        )
        self.assertEqual(expected, actual)
