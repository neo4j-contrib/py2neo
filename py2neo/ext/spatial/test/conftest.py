import pytest

from py2neo.core import Graph


DEFAULT_DB = "http://localhost:7474/db/data/"


def pytest_report_header(config):
    return "py2neo Spatial test run"


def pytest_configure(config):
    db = Graph(DEFAULT_DB)
    rels = next(db.match(), None)
    if rels:
        print(
            'Test Runner will only operate on an empty graph.    '
            'Either clear the DB with `neotool clear` or provide '
            'a differnt DB URI ')

        pytest.exit(1)


def pytest_unconfigure(config):
    """ Final tear-down behaviour of test environment
    """
    db = Graph(DEFAULT_DB)
    db.delete_all()


@pytest.fixture
def graph(request):
    graph = Graph(DEFAULT_DB)
    return graph
