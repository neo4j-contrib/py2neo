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
