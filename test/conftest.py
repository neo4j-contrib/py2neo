import logging

import logutils
import pytest
from logutils.colorize import ColorizingStreamHandler

from py2neo import cypher, neo4j

# ToDo: possible environment constants
DEFAULT_DB = "http://localhost:7474/db/data/"
PY2NEO_LOGGING_LEVEL_COLOUR_MAP = {
    10: (None, 'blue', True),
    20: (None, 'blue', True),
    30: (None, 'yellow', True),
    40: ('red', 'white', True),
    50: ('red', 'white', True),
}

logger = logging.getLogger('pytest_configure')
logger.setLevel(logging.INFO)


def pytest_configure(config):
    db = neo4j.GraphDatabaseService(DEFAULT_DB)
    rels = next(db.match(), None)
    if rels:
        logging.warning(
            'Test Runner will only operate on an empty graph.    '
            'Either clear the DB with `neotool clear` or provide '
            'a differnt DB URI ')

        pytest.exit(1)

    handler = ColorizingStreamHandler()
    handler.level_map = PY2NEO_LOGGING_LEVEL_COLOUR_MAP
    logger.addHandler(handler)

    logger.info('- all logging captured at >= DEUBG and piped '
                'to stdout for test failures.                 ')
    logger.info('- tests running on Neo4J %s', db.neo4j_version)


@pytest.fixture
def graph_db(request):
    db = neo4j.GraphDatabaseService("http://localhost:7474/db/data/")
    db.clear()
    return db

# ToDo: varsion marker

