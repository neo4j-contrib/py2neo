import pytest

from py2neo import neo4j


@pytest.fixture(scope="session")
def graph_db(request):
    db = neo4j.GraphDatabaseService("http://localhost:7474/db/data/")

    def clear():
        db.clear()

    request.addfinalizer(clear)
    return db
