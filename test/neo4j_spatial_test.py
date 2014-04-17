__author__ = 'meier_ul'

from py2neo import neo4j


def test_spatial_base():
    graph_db = neo4j.GraphDatabaseService()
    if not graph_db.has_spatial_extension:
        return
    graph_db.clear()

    spatial = graph_db.spatial
    spatial.add_simple_point_layer("test")
    spatial.add_simple_point_layer("test2", "x", "y")
    spatial.add_editable_layer("layer1", "WKT", "wkt")
    spatial.add_editable_layer("layer2", "WKB", "wkb")

