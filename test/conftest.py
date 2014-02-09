from decimal import Decimal
import logging

import logutils
import pytest
from logutils.colorize import ColorizingStreamHandler

from py2neo import cypher, neo4j


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


def pytest_unconfigure(config):
    db = neo4j.GraphDatabaseService(DEFAULT_DB)
    db.clear()
    logger.info('cleared db after tests completed')


def pytest_runtest_setup(item):
    # looking for markers like '@pytest.mark.neoversion("X.XX")'
    version_markers = item.get_marker("neoversion")
    if version_markers:
        logger.info('minimum neo version required')
        db = neo4j.GraphDatabaseService(DEFAULT_DB)

        required_version = Decimal(version_markers.args[0])

        version_tuple = db.neo4j_version[:-1]  # ignore meta
        actual_version = Decimal(
            '{0}.{1}'.format(
                version_tuple[0], ''.join(str(n) for n in version_tuple[1:])
            )
        )

        if required_version > actual_version:
            logger.warning(
                '{test_name} requires neo > {version}'.format(
                test_name=item.name,
                version=required_version)
            )

            pytest.skip()


@pytest.fixture
def graph_db(request):
    db = neo4j.GraphDatabaseService(DEFAULT_DB)
    db.clear()
    return db


@pytest.fixture
def session(request):
    session = cypher.Session()
    return session
