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


from py2neo.graphio.objects.helper import create_single_index, create_composite_index


def test_create_single_index(clear_graph):
    test_label = 'Foo'
    test_prop = 'bar'

    create_single_index(clear_graph, test_label, test_prop)

    result = list(
        clear_graph.run("CALL db.indexes()")
    )
    row = result[0]

    # the result of the db.indexes() procedure is different for Neo4j 3.5 and 4
    # this should also be synced with differences in py2neo versions
    if 'tokenNames' in row:
        assert row['tokenNames'] == [test_label]
        assert row['properties'] == [test_prop]

    elif 'labelsOrTypes' in row:
        assert row['labelsOrTypes'] == [test_label]
        assert row['properties'] == [test_prop]


def test_create_composite_index(clear_graph):
    test_label = 'Foo'
    test_properties = ['bar', 'keks']

    create_composite_index(clear_graph, test_label, test_properties)

    result = list(
        clear_graph.run("CALL db.indexes()")
    )

    row = result[0]

    # the result of the db.indexes() procedure is different for Neo4j 3.5 and 4
    # this should also be synced with differences in py2neo versions
    if 'tokenNames' in row:
        assert row['tokenNames'] == [test_label]
        # cast to set in case lists have different order
        assert set(row['properties']) == set(test_properties)

    elif 'labelsOrTypes' in row:
        assert row['labelsOrTypes'] == [test_label]
        # cast to set in case lists have different order
        assert set(row['properties']) == set(test_properties)
