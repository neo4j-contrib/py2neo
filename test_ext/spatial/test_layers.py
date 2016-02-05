from unittest import skipUnless
from py2neo.ext.spatial.exceptions import LayerNotFoundError, InvalidWKTError
from .basetest import SpatialTestCase, spatial_available


LAYER_NAME = "geometry_layer"


class LayersTestCase(SpatialTestCase):

    @skipUnless(spatial_available, "no spatial plugin available")
    def test_create_layer(self):
        self.spatial.create_layer(LAYER_NAME)
        assert self._layer_exists(self.spatial.graph, LAYER_NAME)

    @skipUnless(spatial_available, "no spatial plugin available")
    def test_layer_uniqueness(self):
        graph = self.spatial.graph

        def count(layer_name):
            count = 0
            results = graph.cypher.execute(
                "MATCH (r { name:'spatial_root' }), (r)-[:LAYER]->(n) \
RETURN n")

            for record in results:
                node = record[0]
                if node.properties['layer'] == layer_name:
                    count += 1
            return count

        assert count(LAYER_NAME) == 0

        self.spatial.create_layer(LAYER_NAME)
        assert count(LAYER_NAME) == 1

        self.spatial.create_layer(LAYER_NAME)
        assert count(LAYER_NAME) == 1

    @skipUnless(spatial_available, "no spatial plugin available")
    def test_cannot_create_geometry_if_layer_does_not_exist(self):
        with self.assertRaises(LayerNotFoundError):
            self.spatial.create_geometry(
                geometry_name="spatial", wkt_string='POINT (1,1)',
                layer_name="missing")

    @skipUnless(spatial_available, "no spatial plugin available")
    def test_handle_bad_wkt(self):
        geometry_name = "shape"
        bad_geometry = 'isle of wight'

        self.spatial.create_layer(LAYER_NAME)

        with self.assertRaises(InvalidWKTError):
            self.spatial.create_geometry(
                geometry_name=geometry_name, wkt_string=bad_geometry,
                layer_name=LAYER_NAME)

    @skipUnless(spatial_available, "no spatial plugin available")
    def test_get_layer(self):
        self.spatial.create_layer("this")
        assert self._layer_exists(self.spatial.graph, "this")
        assert self.spatial.get_layer("this")

    @skipUnless(spatial_available, "no spatial plugin available")
    def test_delete_layer(self):
        graph = self.spatial.graph
        self.spatial.create_layer("mylayer")
        self.spatial.create_geometry(
            geometry_name="shape_a", wkt_string=self.cornwall_wkt,
            layer_name="mylayer")
        self.spatial.create_geometry(
            geometry_name="shape_b", wkt_string=self.devon_wkt,
            layer_name="mylayer")

        assert self._geometry_exists(graph, "shape_a", "mylayer")
        assert self._geometry_exists(graph, "shape_b", "mylayer")

        self.spatial.delete_layer("mylayer")

        assert not self._geometry_exists(graph, "shape_a", "mylayer")
        assert not self._geometry_exists(graph, "shape_b", "mylayer")

        assert not self._layer_exists(graph, "mylayer")
