# note: integration tests for creating relationships needs nodes in the database
# we create the nodes with graphio, this could mean that issues are difficult to resolve
# however, NodeSets are also tested separately

import pytest
from py2neo.graphio.objects.nodeset import NodeSet, RelationshipSet


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
