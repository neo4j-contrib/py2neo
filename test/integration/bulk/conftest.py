#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
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


import pytest


@pytest.fixture
def clear_graph(graph):
    graph.run("MATCH (n) DETACH DELETE n")

    # remove indexes
    result = list(
        graph.run("CALL db.indexes()")
    )

    for row in result:
        # the result of the db.indexes() procedure is different for Neo4j 3.5 and 4
        # this should also be synced with differences in py2neo versions
        labels = []
        if 'tokenNames' in row:
            labels = row['tokenNames']
        elif 'labelsOrTypes' in row:
            labels = row['labelsOrTypes']

        properties = row['properties']

        # multiple labels possible?
        for label in labels:
            q = "DROP INDEX ON :{}({})".format(label, ', '.join(properties))

    return graph
