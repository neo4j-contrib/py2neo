from py2neo.ext.spatial.util import parse_lat_long
from .basetest import TestBase


class TestBasic(TestBase):
    def test_create_and_fetch_point(self, spatial):
        geometry_name = 'basic_test'
        layer_name = 'basic_layer'

        spatial.create_layer(layer_name)

        point = (5.5, -4.5)

        shape = parse_lat_long(point)
        assert shape.type == 'Point'

        spatial.create_geometry(
            geometry_name=geometry_name, wkt_string=shape.wkt,
            layer_name=layer_name)

        application_node = self.get_application_node(spatial, geometry_name)

        node_properties = application_node.properties
        assert node_properties['_py2neo_geometry_name'] == geometry_name

        geometry_node = self.get_geometry_node(spatial, geometry_name)

        node_properties = geometry_node.properties
        assert node_properties['wkt'] == 'POINT (5.5 -4.5)'
        assert node_properties['bbox'] == [5.5, -4.5, 5.5, -4.5]

    def test_precision(self, graph, spatial, layer):
        x, y = 51.513845, -0.098351
        shape = parse_lat_long((x, y))

        expected_wkt_string = 'POINT ({x} {y})'.format(x=x, y=y)

        assert shape.x == x
        assert shape.y == y
        assert shape.wkt == 'POINT (51.513845 -0.09835099999999999)'

        spatial.create_geometry(
            geometry_name='tricky', wkt_string=shape.wkt,
            layer_name=layer)

        application_node = self.get_application_node(spatial, 'tricky')

        assert application_node

        # get the geometry node and inspect the WKT string
        query = (
            "MATCH (l { layer:{layer_name} })<-[r_layer:LAYER]-"
            "(root { name:'spatial_root' }), "
            "(bbox)-[r_root:RTREE_ROOT]-(l), "
            "(geometry_node)-[r_ref:RTREE_REFERENCE]-(bbox) "
            "RETURN geometry_node"
        )

        params = {
            'layer_name': layer,
        }

        result = graph.cypher.execute(query, params)

        record = result[0]
        geometry_node = record[0]
        properties = geometry_node.properties

        wkt = properties['wkt']
        assert wkt == expected_wkt_string
