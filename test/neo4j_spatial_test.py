__author__ = 'meier_ul'

from py2neo import neo4j_spatial


def test_spatial_base():
    try:
        graph_db = neo4j_spatial.SpatialGraphDatabaseService()
    except NotImplementedError:
        return
    graph_db.clear()
    assert graph_db.layers is None


def test_spatial_layers():
    try:
        graph_db = neo4j_spatial.SpatialGraphDatabaseService()
    except NotImplementedError:
        return
    graph_db.clear()

    point_layer = graph_db.add_layer('test_point', 'point')
    wkt_layer = graph_db.add_layer('test_wkt', 'wkt', property_name='wkt')
    override_layer_1 = graph_db.add_layer('test_override', 'point', lat='y', lon='x')
    override_layer_2 = graph_db.add_layer('test_override', 'wkt', property_name='wkt')

    assert point_layer.name == 'test_point'
    assert point_layer.type == 'POINT'
    assert point_layer.config == 'lon:lat'
    assert wkt_layer.name == 'test_wkt'
    assert wkt_layer.type == 'WKT'
    assert wkt_layer.config == 'wkt'

    assert override_layer_1 == override_layer_2
    assert override_layer_1.type == 'POINT'
    assert override_layer_2.type == 'POINT'
    assert override_layer_1.config == 'x:y'

    layers = graph_db.layers
    assert len(layers) == 3

    found_layer = graph_db.find_layer('test_point')
    assert found_layer == point_layer
    testnode = graph_db.create({'bla': 'blubb'})
    point_layer.add_node(testnode)





