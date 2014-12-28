import pytest

from py2neo.ext.spatial.util import parse_lat_long
from .basetest import TestBase


class TestBasic(TestBase):
    def test_create_and_fetch_point(self, spatial):
        graph = spatial.graph
        geometry_name = 'basic_test'
        layer_name = 'basic_layer'
        basic_point = 'basic_point'

        spatial.create_layer(layer_name)

        point = (5.5, -4.5)

        shape = parse_lat_long(point)
        assert shape.type == 'Point'

        spatial.create_geometry(
            geometry_name=geometry_name, wkt_string=shape.wkt,
            layer_name=layer_name)

        application_node = self.get_application_node(spatial, geometry_name)

        assert application_node

    def test_precision(self, graph, spatial, layer, hard_lat_long):
        x = hard_lat_long.coords[0]
        y = hard_lat_long.coords[1]

        shape = parse_lat_long(hard_lat_long.coords)

        assert shape.x == x
        assert shape.y == y

        wkt_string = spatial._get_wkt_from_shape(shape)

        spatial.create_geometry(
            geometry_name='tricky', wkt_string=wkt_string,
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

        assert wkt == wkt_string
