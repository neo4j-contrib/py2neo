from unittest import skipUnless
from py2neo.ext.spatial.util import parse_lat_long
from .basetest import SpatialTestCase, spatial_available


class BasicTestCase(SpatialTestCase):

    @skipUnless(spatial_available, "no spatial plugin available")
    def test_create_and_fetch_point(self):
        geometry_name = 'basic_test'
        layer_name = 'basic_layer'

        self.spatial.create_layer(layer_name)

        point = (5.5, -4.5)

        shape = parse_lat_long(point)
        assert shape.type == 'Point'

        self.spatial.create_geometry(
            geometry_name=geometry_name, wkt_string=shape.wkt,
            layer_name=layer_name)

        application_node = self.get_application_node(self.spatial, geometry_name)

        assert application_node['_py2neo_geometry_name'] == geometry_name

        geometry_node = self.get_geometry_node(self.spatial, geometry_name)

        assert geometry_node['wkt'] == 'POINT (5.5 -4.5)'
        assert geometry_node['bbox'] == [5.5, -4.5, 5.5, -4.5]

    @skipUnless(spatial_available, "no spatial plugin available")
    def test_precision(self):
        x, y = 51.513845, -0.098351
        shape = parse_lat_long((x, y))

        expected_wkt_string = 'POINT ({x} {y})'.format(x=x, y=y)

        assert shape.x == x
        assert shape.y == y
        assert shape.wkt == 'POINT (51.513845 -0.09835099999999999)'

        self.spatial.create_geometry(
            geometry_name='tricky', wkt_string=shape.wkt,
            layer_name=self.layer)

        application_node = self.get_application_node(self.spatial, 'tricky')

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
            'layer_name': self.layer,
        }

        geometry_node = self.graph.evaluate(query, params)
        wkt = geometry_node['wkt']
        assert wkt == expected_wkt_string
