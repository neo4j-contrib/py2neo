import pytest

from py2neo import cypher, neo4j


@pytest.fixture(scope="session")
def graph_db(request):
    db = neo4j.GraphDatabaseService("http://localhost:7474/db/data/")

    def clear():
        db.clear()

    request.addfinalizer(clear)
    return db


@pytest.fixture
def session(request, graph_db):
    session = cypher.Session()
    return session
