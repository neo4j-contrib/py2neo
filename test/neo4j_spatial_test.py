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


def test_spatial_nodes():
    try:
        graph_db = neo4j_spatial.SpatialGraphDatabaseService()
    except NotImplementedError:
        return
    graph_db.clear()

    point_layer = graph_db.add_layer('test', 'point', lat='lat', lon='lon')
    taufkirchen, = graph_db.create({'name': 'Testpoint', 'lon': 11.616667, 'lat': 48.05})
    point_layer.add_node(taufkirchen)
    assert taufkirchen['bbox'] == [11.616667, 48.05, 11.616667, 48.05]
    point_layer.update_geometry(taufkirchen, 'POINT(11.61 48.065833)')
    assert taufkirchen['lon'] == 11.61
    assert taufkirchen['lat'] == 48.065833


def test_find_spatial_nodes():
    try:
        graph_db = neo4j_spatial.SpatialGraphDatabaseService()
    except NotImplementedError:
        return
    graph_db.clear()

    gemeindelayer = graph_db.add_layer('gemeinden', 'point')
    taufkirchen, = graph_db.create({'name': 'Taufkirchen', 'lon': 11.616667, 'lat': 48.05})
    gemeindelayer.add_node(taufkirchen)
    unterhaching, = graph_db.create({'name': 'Unterhaching', 'lon': 11.61, 'lat': 48.065833})
    gemeindelayer.add_node(unterhaching)
    oberhaching, = graph_db.create({'name': 'Oberhaching', 'lon': 11.583333, 'lat': 48.016667})
    gemeindelayer.add_node(oberhaching)
    baltrum, = graph_db.create({'name': 'Baltrum', 'lon': 7.368333, 'lat': 53.728889})
    gemeindelayer.add_node(baltrum)
    nodes = gemeindelayer.find_geometries_in_bbox(minx=11, maxx=12, miny=48, maxy=49)
    assert len(nodes) == 3
    assert taufkirchen in nodes
    assert unterhaching in nodes
    assert oberhaching in nodes
    assert baltrum not in nodes
    nodes = gemeindelayer.find_geometries_within_distance(x=11.6, y=48, distance=10)
    assert len(nodes) == 3
    assert taufkirchen in nodes
    assert unterhaching in nodes
    assert oberhaching in nodes
    assert baltrum not in nodes