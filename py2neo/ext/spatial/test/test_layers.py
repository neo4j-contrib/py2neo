import pytest

from .. import Spatial, Node
from .. exceptions import (
    GeometryExistsError, LayerNotFoundError, InvalidWKTError)


LAYER_NAME = "geometry_layer"

GEOMETRY_A = 'MULTIPOLYGON (((-1.253002 50.80102, -1.03137 50.69296,\
 -1.114103 50.61067, -1.307651 50.50555, -1.659074 50.62415,\
 -1.518657 50.72036, -1.253002 50.80102)))'

GEOMETRY_B = 'MULTIPOLYGON(((0 0,10 20,30 40,0 0),(1 1,2 2,3 3,1 1)),\
 ((100 100,110 110,120 120,100 100)))'


class TestLayers(object):
    @pytest.fixture(autouse=True)
    def spatial_api(self, graph):
        graph.delete_all()
        graph.legacy.delete_index(Node, LAYER_NAME)
        spatial = Spatial(graph)
        self.spatial = spatial

    @staticmethod
    def _layer_exists(graph, layer_name=LAYER_NAME):
        # layers created by the server extension are not labelled.
        results = graph.cypher.execute("MATCH (n) RETURN n")
        for result in results:
            node = result.values[0]
            if node.properties.get('layer') == LAYER_NAME:
                return True
        return False

    @staticmethod
    def _geometry_exists(graph, geometry_name, LAYER_NAME):
        resp = graph.find(
            label=LAYER_NAME, property_key="name",
            property_value=geometry_name)
        results = [r for r in resp]

        return len(results) == 1

    def test_create_layer(self, graph):
        self.spatial.create_layer(LAYER_NAME)
        assert self._layer_exists(graph, LAYER_NAME)

    def test_layer_uniqueness(self, graph):
        spatial = self.spatial

        def count(LAYER_NAME):
            # layers created by the server extension are not labelled.
            count = 0
            results = graph.cypher.execute("MATCH (n) RETURN n")
            for result in results:
                node = result.values[0]
                if node.properties.get('layer') == LAYER_NAME:
                    count += 1
            return count

        assert count(LAYER_NAME) == 0

        spatial.create_layer(LAYER_NAME)
        assert count(LAYER_NAME) == 1

        spatial.create_layer(LAYER_NAME)
        assert count(LAYER_NAME) == 1

    def test_cannot_create_geometry_if_layer_does_not_exist(self):
        with pytest.raises(LayerNotFoundError):
            self.spatial.create(
                geometry_name="test", wkt_string=GEOMETRY_A,
                layer_name="missing")

    def test_create_geometry(self, graph):
        geometry_name = "shape"

        spatial = self.spatial
        spatial.create_layer(LAYER_NAME)
        spatial.create(
            geometry_name=geometry_name, wkt_string=GEOMETRY_A,
            layer_name=LAYER_NAME)

        assert self._geometry_exists(graph, geometry_name, LAYER_NAME)

    def test_geometry_uniqueness(self):
        geometry_name = "shape"

        spatial = self.spatial
        spatial.create_layer(LAYER_NAME)
        spatial.create(
            geometry_name=geometry_name, wkt_string=GEOMETRY_A,
            layer_name=LAYER_NAME)

        with pytest.raises(GeometryExistsError):
            spatial.create(
                geometry_name=geometry_name, wkt_string=GEOMETRY_A,
                layer_name=LAYER_NAME)

    def test_handle_bad_wkt(self):
        geometry_name = "shape"
        bad_geometry = 'isle of wight'

        spatial = self.spatial
        spatial.create_layer(LAYER_NAME)

        with pytest.raises(InvalidWKTError):
            spatial.create(
                geometry_name=geometry_name, wkt_string=bad_geometry,
                layer_name=LAYER_NAME)

    def test_delete_geometry(self, graph):
        geometry_name = "shape"

        spatial = self.spatial
        spatial.create_layer(LAYER_NAME)
        spatial.create(
            geometry_name=geometry_name, wkt_string=GEOMETRY_A,
            layer_name=LAYER_NAME)

        assert self._geometry_exists(graph, geometry_name, LAYER_NAME)

        spatial.delete(geometry_name, GEOMETRY_A, LAYER_NAME)

        assert not self._geometry_exists(graph, geometry_name, LAYER_NAME)

    def test_delete_layer(self, graph):
        spatial = self.spatial
        spatial.create_layer(LAYER_NAME)
        spatial.create(
            geometry_name="shape_a", wkt_string=GEOMETRY_A,
            layer_name=LAYER_NAME)
        spatial.create(
            geometry_name="shape_b", wkt_string=GEOMETRY_B,
            layer_name=LAYER_NAME)

        assert self._geometry_exists(graph, "shape_a", LAYER_NAME)
        assert self._geometry_exists(graph, "shape_b", LAYER_NAME)

        spatial.delete_layer(LAYER_NAME, force=True)

        assert not self._geometry_exists(graph, "shape_a", LAYER_NAME)
        assert not self._geometry_exists(graph, "shape_b", LAYER_NAME)

        assert not self._layer_exists(graph, LAYER_NAME)
