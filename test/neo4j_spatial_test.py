from xxsubtype import spamdict
from shapely.geometry import Point
from py2neo.neo4j import Node
from py2neo.packages.urimagic import URI
from py2neo.util import ustr

__author__ = 'meier_ul'

from py2neo import neo4j


def test_spatial_base():
    graph_db = neo4j.GraphDatabaseService()
    if not graph_db.has_spatial_extension:
        return
    graph_db.clear()

    spatial = graph_db.spatial
    spatial.add_simple_point_layer("test")
    spatial.add_simple_point_layer("test2", "y", "x")
    spatial.add_editable_layer("layer1", "wkt")

    test1 = spatial.get_layer('test')
    test2 = spatial.get_layer('test2')
    test3 = spatial.get_layer('layer1')


    point = Point(15.2, 60.1)
    point2 = Point(16.2, 61.1)
    circle = point2.buffer(0.5)
    point_node, = graph_db.create({'name': 'point', 'lat': point.y, 'lon': point.x})
    point_node_2, = graph_db.create({'name': 'point2', 'x': point.x, 'y': point.y})
    wkt_node, = graph_db.create({'name': 'wkt-point', 'wkt': point.wkt})
    spatial.add_node_to_layer('test', point_node)
    spatial.add_node_to_layer('test2', point_node_2)
    spatial.add_node_to_layer('layer1', wkt_node)
    spatial.update_geometry_from_wkt('test', point2.wkt, point_node)
    spatial.update_geometry_from_wkt('test2', point2.wkt, point_node_2)
    spatial.update_geometry_from_wkt('layer1', point2.wkt, wkt_node)
    spatial.update_geometry_from_wkt('test', circle.wkt, point_node)
    spatial.update_geometry_from_wkt('test2', circle.wkt, point_node_2)
    spatial.update_geometry_from_wkt('layer1', circle.wkt, wkt_node)




