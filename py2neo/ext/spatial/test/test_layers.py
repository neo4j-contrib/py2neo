import pytest

from .. exceptions import (
    GeometryExistsError, LayerNotFoundError, InvalidWKTError)
from ..util import parse_lat_long


LAYER_NAME = "geometry_layer"


class Base(object):
    @staticmethod
    def _layer_exists(graph, layer_name=LAYER_NAME):
        # assert a spatial index exists (layers created by the server extension 
        # are not labelled so this is brute force)
        results = graph.cypher.execute("MATCH (n) RETURN n")
        for result in results:
            node = result.values[0]
            if node.properties.get('layer') == LAYER_NAME:
                return True
        return False

    @staticmethod
    def _geometry_exists(graph, geometry_name, LAYER_NAME):
        # assert a geometry exists in the *application* graph
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
        spatial.create_layer("this")
        assert spatial.get_layer("this")

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
        assert self._geometry_exists(graph, "cornwall", "uk")
        spatial.delete_geometry("cornwall", cornwall_wkt, "uk")
        assert not self._geometry_exists(graph, "cornwall", "uk")


class TestQueries(Base):
    def test_find_from_bad_layer(
            self, spatial, test_layer, all_features):

        likeable_stoke_newington_character = (51.559676, -0.07937)

        with pytest.raises(LayerNotFoundError):
            pois = spatial.find_within_distance(
                "america", likeable_stoke_newington_character, 1000)

    def test_find_everything_in_the_uk(
            self, spatial, test_layer, all_features):

        likeable_stoke_newington_character = (51.559676, -0.07937)

        # this should only return... everything, as the UK is not this big.
        pois = spatial.find_within_distance(
            test_layer, likeable_stoke_newington_character, 1000)

        assert len(pois) == len(all_features)

    def test_find_only_within_london(
            self, spatial, test_layer, all_features, london_features):

        # london_features *are* on the uk_layer - see conftest.py
        assert len(all_features) > len(london_features)

        likeable_stoke_newington_character = (51.559676, -0.07937)

        # this should only return london features
        pois = spatial.find_within_distance(
            test_layer, likeable_stoke_newington_character, 35)

        assert len(pois) == len(london_features)

        # check it really does
        expected_london_names = sorted([f.name for f in london_features])
        returned_names = sorted([poi.get_properties()['name'] for poi in pois])
        assert expected_london_names == returned_names

    def test_find_closest_geometries(
            self, spatial, test_layer, all_features, cornish_towns):

        tourist = (50.500000, -4.500000)  # Bodmin, Cornwall
        closest_geometries = spatial.find_closest_geometries(
            test_layer, tourist, 1)
        assert len(closest_geometries) == 0

    def test_nothing_close(self, spatial, test_layer):
        tourist = (50.500000, -4.500000)
        nearby_geometries = spatial.find_closest_geometries(
            test_layer, tourist, 100)
        assert len(nearby_geometries) == 0

