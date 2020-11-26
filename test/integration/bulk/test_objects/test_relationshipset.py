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


# note: integration tests for creating relationships needs nodes in the database
# we create the nodes with bulk, this could mean that issues are difficult to resolve
# however, NodeSets are also tested separately

import pytest
from py2neo.bulk import NodeSet, RelationshipSet


@pytest.fixture
def small_relationshipset(make_unique_id):
    rs = RelationshipSet('TEST', ['Test'], ['Foo'], ['uuid'], ['uuid'])

    for i in range(100):
        rs.add_relationship(
            {'uuid': i}, {'uuid': i}, {}
        )

    return rs


@pytest.fixture(scope='function')
def create_nodes_test(clear_graph):
    ns1 = NodeSet(['Test'], merge_keys=['uuid'])
    ns2 = NodeSet(['Foo'], merge_keys=['uuid'])

    for i in range(100):
        ns1.add_node({'uuid': i})
        ns2.add_node({'uuid': i})

    ns1.create(clear_graph)
    ns2.create(clear_graph)

    return ns1, ns2


class TestRelationshipSetSet:
    """
    Test basic function such as adding rels.
    """

    def test_item_iterator(self, small_relationshipset):
        for i in small_relationshipset.item_iterator():
            assert i['start_node_properties']
            assert i['end_node_properties']


class TestRelationshipSetCreate:

    def test_relationshipset_create_number(self, graph, create_nodes_test, small_relationshipset):

        small_relationshipset.create(graph)

        result = list(
            graph.run(
                "MATCH (t:Test)-[r:TEST]->(f:Foo) RETURN count(r)"
            )
        )
        print(result)
        print(result[0])
        assert result[0][0] == 100

    def test_relationship_create_single_index(self, clear_graph, small_relationshipset):

        small_relationshipset.create_index(clear_graph)

        result = list(
            clear_graph.run("CALL db.indexes()")
        )

        for row in result:
            # the result of the db.indexes() procedure is different for Neo4j 3.5 and 4
            # this should also be synced with differences in py2neo versions
            if 'tokenNames' in row:
                assert row['tokenNames'] == ['Test'] and row['properties'] == ['uuid'] \
                       or row['tokenNames'] == ['Test'] and row['properties'] == ['uuid']

            elif 'labelsOrTypes' in row:
                assert row['labelsOrTypes'] == ['Test'] and row['properties'] == ['uuid'] \
                       or row['labelsOrTypes'] == ['Test'] and row['properties'] == ['uuid']


class TestRelationshipSetMerge:

    def test_relationshipset_merge(self, graph, create_nodes_test, small_relationshipset):

        small_relationshipset.merge(graph)

        result = list(
            graph.run(
                "MATCH (t:Test)-[r:TEST]->(f:Foo) RETURN count(r)"
            )
        )
        print(result)
        print(result[0])
        assert result[0][0] == 100

    def test_to_dict(self, small_relationshipset):
        d = small_relationshipset.to_dict()
        assert isinstance(d, dict)
        assert len(d["relationships"]) == 100

    def test_from_dict(self, small_relationshipset):
        d = small_relationshipset.to_dict()
        rs2 = RelationshipSet.from_dict(d)
        assert rs2.rel_type == small_relationshipset.rel_type
        assert rs2.start_node_labels == small_relationshipset.start_node_labels
        assert rs2.end_node_labels == small_relationshipset.end_node_labels
        assert rs2.start_node_properties == small_relationshipset.start_node_properties
        assert rs2.end_node_properties == small_relationshipset.end_node_properties
