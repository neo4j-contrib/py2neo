from __future__ import print_function

import sys
sys.path.insert(1, '../py2neo/')

import argparse
import os

from py2neo.ext.spatial.scripts.helpers import get_spatial_api
from py2neo.ext.spatial import Spatial


NEO_URL_TEMPLATE = "http://{username}:{password}@localhost:{port}/db/data/"
DATA_HOME = 'examples/data'


def load(server_url, geometry_name, wkt_string, layer_name):
    spatial = get_spatial_api(server_url)

    spatial.create_layer(layer_name)
    spatial.create_geometry(geometry_name, wkt_string, layer_name)
    print('done')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Load the example data into map layers')

    parser.add_argument(
        '--username', type=str, help='a neo4j user. defaults to "neo4j"')

    parser.add_argument(
        '--password', type=str,
        help='the password of the user. defaults to "neo4j"')

    parser.add_argument(
        '--data', type=str, help='the name of the data file to load')

    parser.add_argument(
        '--layer', dest='layer_name', action='store',
        help="""The layer to add the data to.
        This will be created if it does not already exist.""")

    parser.add_argument(
        '--port', dest='port_address', action='store',
        help='The port Neo is running on. defaults to "7474"')

    args = parser.parse_args()

    port = args.port_address or 7474
    username = args.username or "neo4j"
    password = args.password or "neo4j"

    neo_url = NEO_URL_TEMPLATE.format(
        username=username, password=password, port=port)

    geometry_name = args.data
    layer_name = args.layer_name

    if not geometry_name.endswith('wkt'):
        geometry_name += '.wkt'

    working_directory = os.path.dirname(os.path.realpath(__file__))
    ext_root = working_directory.strip('scripts')
    data_uri = os.path.join(ext_root, DATA_HOME, geometry_name)

    with open(data_uri, 'r') as fh:
        wkt_string = fh.read()

    load(
        server_url=neo_url, geometry_name=geometry_name,
        wkt_string=wkt_string, layer_name=layer_name
    )
