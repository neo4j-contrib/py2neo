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


from py2neo.bulk.queries import nodes_merge_unwind


class TestNodesMergeUnwind:

    def test_query_single_label(self):
        test_label = ['Foo']

        query = nodes_merge_unwind(test_label, ['sid'])

        expected_query = """UNWIND $props AS properties
MERGE (n:Foo { sid: properties.sid } )
ON CREATE SET n = properties
ON MATCH SET n += properties"""

        assert query == expected_query

    def test_query_multiple_labels(self):
        test_labels = ['Foo', 'Bar']

        query = nodes_merge_unwind(test_labels, ['sid'])

        expected_query = """UNWIND $props AS properties
MERGE (n:Foo:Bar { sid: properties.sid } )
ON CREATE SET n = properties
ON MATCH SET n += properties"""

        assert query == expected_query

    def test_query_multiple_merge_properties(self):
        test_labels = ['Foo', 'Bar']
        merge_props = ['sid', 'other']

        query = nodes_merge_unwind(test_labels, merge_props)

        expected_query = """UNWIND $props AS properties
MERGE (n:Foo:Bar { sid: properties.sid, other: properties.other } )
ON CREATE SET n = properties
ON MATCH SET n += properties"""

        assert query == expected_query

    def test_own_param_name(self):
        test_labels = ['Foo', 'Bar']
        merge_props = ['sid', 'other']

        query = nodes_merge_unwind(test_labels, merge_props, property_parameter='nodes')

        expected_query = """UNWIND $nodes AS properties
MERGE (n:Foo:Bar { sid: properties.sid, other: properties.other } )
ON CREATE SET n = properties
ON MATCH SET n += properties"""

        assert query == expected_query

    def test_nodes_are_created(self, clear_graph, make_unique_id):
        label = make_unique_id()
        query = nodes_merge_unwind([label], ['testid'])

        clear_graph.run(query, props=[{'testid': 1, 'key': 'newvalue'}])

        results = list(clear_graph.run("MATCH (n:%s) RETURN n" % label))

        first_row = results[0]
        first_row_first_element = first_row[0]

        assert len(results) == 1
        assert first_row_first_element['testid'] == 1
        assert first_row_first_element['key'] == 'newvalue'

    def test_nodes_are_merged(self, clear_graph, make_unique_id):
        label = make_unique_id()
        clear_graph.run("CREATE (n:%s) SET n.testid = 1, n.key = 'value', n.other = 'other_value'" % label)

        query = nodes_merge_unwind([label], ['testid'])

        clear_graph.run(query, props=[{'testid': 1, 'key': 'newvalue'}])

        results = list(clear_graph.run("MATCH (n:%s) RETURN n" % label))

        first_row = results[0]
        first_row_first_element = first_row[0]

        assert len(results) == 1
        assert first_row_first_element['testid'] == 1
        assert first_row_first_element['key'] == 'newvalue'
        # assert other value did not change
        assert first_row_first_element['other'] == 'other_value'
