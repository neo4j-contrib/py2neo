import pytest

from .. exceptions import (
    GeometryExistsError, LayerNotFoundError, InvalidWKTError)


LAYER_NAME = "geometry_layer"


class Base(object):
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


class TestLayers(Base):
    def test_create_layer(self, spatial):
        spatial.create_layer(LAYER_NAME)
        assert self._layer_exists(spatial.graph, LAYER_NAME)

    def test_layer_uniqueness(self, spatial):
        graph = spatial.graph

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

    def test_cannot_create_geometry_if_layer_does_not_exist(self, spatial):
        with pytest.raises(LayerNotFoundError):
            spatial.create_geometry(
                geometry_name="test", wkt_string='POINT (1,1)',
                layer_name="missing")

    def test_handle_bad_wkt(self, spatial):
        geometry_name = "shape"
        bad_geometry = 'isle of wight'

        spatial.create_layer(LAYER_NAME)

        with pytest.raises(InvalidWKTError):
            spatial.create_geometry(
                geometry_name=geometry_name, wkt_string=bad_geometry,
                layer_name=LAYER_NAME)

    def test_get_layer(self, spatial, cornwall):
        assert spatial.get_layer("cornwall")

    def test_delete_layer(self, spatial, cornwall, devon):
        graph = spatial.graph
        spatial.create_layer(LAYER_NAME)
        spatial.create_geometry(
            geometry_name="shape_a", wkt_string=cornwall,
            layer_name=LAYER_NAME)
        spatial.create_geometry(
            geometry_name="shape_b", wkt_string=devon,
            layer_name=LAYER_NAME)

        assert self._geometry_exists(graph, "shape_a", LAYER_NAME)
        assert self._geometry_exists(graph, "shape_b", LAYER_NAME)

        spatial.delete_layer(LAYER_NAME)

        assert not self._geometry_exists(graph, "shape_a", LAYER_NAME)
        assert not self._geometry_exists(graph, "shape_b", LAYER_NAME)

        assert not self._layer_exists(graph, LAYER_NAME)


class TestGeometries(Base):
    def test_create_geometry(self, spatial, cornwall):
        graph = spatial.graph
        geometry_name = "shape"
        spatial.create_layer(LAYER_NAME)
        spatial.create_geometry(
            geometry_name=geometry_name, wkt_string=cornwall,
            layer_name=LAYER_NAME)

        
        assert self._geometry_exists(graph, geometry_name, LAYER_NAME)

    def test_geometry_uniqueness(self, spatial, cornwall):
        geometry_name = "shape"

        spatial.create_layer(LAYER_NAME)
        spatial.create_geometry(
            geometry_name=geometry_name, wkt_string=cornwall,
            layer_name=LAYER_NAME)

        with pytest.raises(GeometryExistsError):
            spatial.create_geometry(
                geometry_name=geometry_name, wkt_string=cornwall,
                layer_name=LAYER_NAME)

    def test_delete_geometry(self, spatial, cornwall):
        graph = spatial.graph
        geometry_name = "shape"

        spatial.create_layer(LAYER_NAME)
        spatial.create_geometry(
            geometry_name=geometry_name, wkt_string=cornwall,
            layer_name=LAYER_NAME)

        assert self._geometry_exists(graph, geometry_name, LAYER_NAME)

        spatial.delete_geometry(geometry_name, cornwall, LAYER_NAME)

        assert not self._geometry_exists(graph, geometry_name, LAYER_NAME)


class TestQueries(Base):
    def test_contains(self, cornish_towns):
        pass
