import pytest

from .. import Spatial
from .. exceptions import (
    GeometryExistsError, LayerNotFoundError, InvalidWKTError)


GEOMETRY_A = 'MULTIPOLYGON (((-1.253002 50.80102, -1.03137 50.69296,\
 -1.114103 50.61067, -1.307651 50.50555, -1.659074 50.62415,\
 -1.518657 50.72036, -1.253002 50.80102)))'

GEOMETRY_B = 'MULTIPOLYGON(((0 0,10 20,30 40,0 0),(1 1,2 2,3 3,1 1)),\
 ((100 100,110 110,120 120,100 100)))'


class TestLayers(object):
    @pytest.fixture(autouse=True)
    def spatial_api(self, graph):
        graph.delete_all()

        indexes = graph.legacy._indexes
        for key, _ in indexes.copy().items():
            indexes[key] = {}

        spatial = Spatial(graph)
        self.spatial = spatial

    @staticmethod
    def _layer_exists(graph, layer_name):
        # layers created by the server extension are not labelled.
        results = graph.cypher.execute("MATCH (n) RETURN n")
        for result in results:
            node = result.values[0]
            if node.properties.get('layer') == layer_name:
                return True
        return False

    @staticmethod
    def _geometry_exists(graph, geometry_name, layer_name):
        resp = graph.find(
            label=layer_name, property_key="name",
            property_value=geometry_name)
        results = [r for r in resp]

        return len(results) == 1

    def test_create_layer(self, graph):
        self.spatial.create_layer("test_layer")
        assert self._layer_exists(graph, "test_layer")

    def test_layer_uniqueness(self, graph):
        spatial = self.spatial
        layer_name = 'test_layer'

        def count(layer_name):
            # layers created by the server extension are not labelled.
            count = 0
            results = graph.cypher.execute("MATCH (n) RETURN n")
            for result in results:
                node = result.values[0]
                if node.properties.get('layer') == layer_name:
                    count += 1
            return count

        assert count(layer_name) == 0

        spatial.create_layer("test_layer")
        assert count(layer_name) == 1

        spatial.create_layer("test_layer")
        assert count(layer_name) == 1

    def test_cannot_create_geometry_if_layer_does_not_exist(self):
        with pytest.raises(LayerNotFoundError):
            self.spatial.create(
                geometry_name="test", wkt_string=GEOMETRY_A,
                layer_name="missing")

    def test_create_geometry(self, graph):
        geometry_name = "shape"
        layer_name = "test_layer"

        spatial = self.spatial
        spatial.create_layer(layer_name)
        spatial.create(
            geometry_name=geometry_name, wkt_string=GEOMETRY_A,
            layer_name=layer_name)

        assert self._geometry_exists(graph, geometry_name, layer_name)

    def test_geometry_uniqueness(self):
        geometry_name = "shape"
        layer_name = "test_layer"

        spatial = self.spatial
        spatial.create_layer(layer_name)
        spatial.create(
            geometry_name=geometry_name, wkt_string=GEOMETRY_A,
            layer_name=layer_name)

        with pytest.raises(GeometryExistsError):
            spatial.create(
                geometry_name=geometry_name, wkt_string=GEOMETRY_A,
                layer_name=layer_name)

    def test_handle_bad_wkt(self):
        geometry_name = "shape"
        layer_name = "test_layer"
        bad_geometry = 'isle of wight'

        spatial = self.spatial
        spatial.create_layer(layer_name)

        with pytest.raises(InvalidWKTError):
            spatial.create(
                geometry_name=geometry_name, wkt_string=bad_geometry,
                layer_name=layer_name)

    def test_destroy_geometry(self, graph):
        geometry_name = "shape"
        layer_name = "test_layer"

        spatial = self.spatial
        spatial.create_layer(layer_name)
        spatial.create(
            geometry_name=geometry_name, wkt_string=GEOMETRY_A,
            layer_name=layer_name)

        assert self._geometry_exists(graph, geometry_name, layer_name)

        spatial.destroy(geometry_name, GEOMETRY_A, layer_name)

        assert not self._geometry_exists(graph, geometry_name, layer_name)

    def test_destroy_layer(self, graph):
        layer_name = "test_layer"

        spatial = self.spatial
        spatial.create_layer(layer_name)
        spatial.create(
            geometry_name="shape_a", wkt_string=GEOMETRY_A,
            layer_name=layer_name)
        spatial.create(
            geometry_name="shape_b", wkt_string=GEOMETRY_B,
            layer_name=layer_name)

        assert self._geometry_exists(graph, "shape_a", layer_name)
        assert self._geometry_exists(graph, "shape_b", layer_name)

        spatial.destroy_layer(layer_name, force=True)

        assert not self._geometry_exists(graph, "shape_a", layer_name)
        assert not self._geometry_exists(graph, "shape_b", layer_name)

        assert not self._layer_exists(graph, layer_name)
