import pytest

from .. exceptions import (
    GeometryExistsError, LayerNotFoundError, InvalidWKTError)
from ..util import parse_lat_long


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

    def test_delete_layer(self, spatial, cornwall_wkt, devon_wkt):
        graph = spatial.graph
        spatial.create_layer(LAYER_NAME)
        spatial.create_geometry(
            geometry_name="shape_a", wkt_string=cornwall_wkt,
            layer_name=LAYER_NAME)
        spatial.create_geometry(
            geometry_name="shape_b", wkt_string=devon_wkt,
            layer_name=LAYER_NAME)

        assert self._geometry_exists(graph, "shape_a", LAYER_NAME)
        assert self._geometry_exists(graph, "shape_b", LAYER_NAME)

        spatial.delete_layer(LAYER_NAME)

        assert not self._geometry_exists(graph, "shape_a", LAYER_NAME)
        assert not self._geometry_exists(graph, "shape_b", LAYER_NAME)

        assert not self._layer_exists(graph, LAYER_NAME)


class TestGeometries(Base):
    def test_create_polygon(self, spatial, cornwall_wkt):
        graph = spatial.graph
        geometry_name = "shape"
        spatial.create_layer(LAYER_NAME)
        spatial.create_geometry(
            geometry_name=geometry_name, wkt_string=cornwall_wkt,
            layer_name=LAYER_NAME)
        
        assert self._geometry_exists(graph, geometry_name, LAYER_NAME)

    def test_create_point(self, spatial):
        graph = spatial.graph
        geometry_name = "shape"
        spatial.create_layer(LAYER_NAME)

        shape = parse_lat_long((5.5, -4.5))
        assert shape.type == 'Point'

        spatial.create_geometry(
            geometry_name=geometry_name, wkt_string=shape.wkt,
            layer_name=LAYER_NAME)
        
        assert self._geometry_exists(graph, geometry_name, LAYER_NAME)

    def test_geometry_uniqueness(self, spatial, cornwall_wkt):
        geometry_name = "shape"

        spatial.create_layer(LAYER_NAME)
        spatial.create_geometry(
            geometry_name=geometry_name, wkt_string=cornwall_wkt,
            layer_name=LAYER_NAME)

        with pytest.raises(GeometryExistsError):
            spatial.create_geometry(
                geometry_name=geometry_name, wkt_string=cornwall_wkt,
                layer_name=LAYER_NAME)

    def test_delete_geometry(self, spatial, cornwall, cornwall_wkt):
        graph = spatial.graph
        assert self._geometry_exists(graph, "cornwall", "cornwall")
        spatial.delete_geometry("cornwall", cornwall_wkt, "cornwall")
        assert not self._geometry_exists(graph, "cornwall", "cornwall")


class TestQueries(Base):
    def test_nearby(self, spatial, cornish_towns):
        tourist = (50.500000, -4.500000)
        nearby_geometries = spatial.find_closest_geometries(
            "cornwall", tourist, 100)
        assert len(nearby_geometries) == len(cornish_towns)

    def test_nothing_nearby(self, spatial, cornwall_layer):
        tourist = (50.500000, -4.500000)
        nearby_geometries = spatial.find_closest_geometries(
            "cornwall", tourist, 100)
        assert len(nearby_geometries) == 0
