import sys

from py2neo import Graph, GraphError
from py2neo.ext.spatial import Spatial


def get_spatial_api(server_url):
    graph = Graph(server_url)

    try:
        graph.schema
    except GraphError as exc:
        if '401' in exc.message:
            print('username/password invalid')
            sys.exit(1)

        if '403' in exc.message:
            print('Neo4j requires you to change the default password')
            sys.exit(1)

        raise

    try:
        spatial = Spatial(graph)
    except KeyError as exc:
        if 'extensions' in exc.message:
            print('No server extensions found')
            sys.exit(1)

        raise

    return spatial
