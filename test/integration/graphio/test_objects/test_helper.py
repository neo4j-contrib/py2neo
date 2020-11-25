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
