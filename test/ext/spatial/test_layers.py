import pytest
from py2neo import Node

from . import TestBase
from py2neo.ext.spatial.exceptions import (
    LayerExistsError, LayerNotFoundError, InvalidWKTError)
from py2neo.ext.spatial.util import parse_lat_long_to_point


LAYER_NAME = "geometry_layer"


class TestLayers(TestBase):
    def test_create_layer(self, spatial):
        spatial.create_layer(LAYER_NAME)
        assert self._layer_exists(spatial.graph, LAYER_NAME)

    def test_layer_uniqueness(self, spatial):
        graph = spatial.graph

        def count(layer_name):
            count = 0
            results = graph.cypher.execute(
                "MATCH (r { name:'spatial_root' }), (r)-[:LAYER]->(n) "
                "RETURN n"
            )

            for record in results:
                node = record[0]
                if node.properties['layer'] == layer_name:
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
                geometry_name="spatial", wkt_string='POINT (1 1)',
                layer_name="missing")

    def test_handle_bad_wkt(self, spatial):
        geometry_name = "shape"
        bad_geometry = 'isle of wight'

        spatial.create_layer(LAYER_NAME)

        with pytest.raises(InvalidWKTError):
            spatial.create_geometry(
                geometry_name=geometry_name, wkt_string=bad_geometry,
                layer_name=LAYER_NAME)

    def test_get_layer(self, spatial):
        spatial.create_layer("this")
        assert self._layer_exists(spatial.graph, "this")
        assert spatial.get_layer("this")

    def test_delete_layer_does_not_delete_indexed_nodes(self, spatial):
        spatial.create_layer("default_layer")

        graph = spatial.graph
        node = Node(address="300 St John Street, London.")
        graph.create(node)
        node_id = int(node.uri.path.segments[-1])

        coords = (51.528453, -0.104489)
        shape = parse_lat_long_to_point(*coords)

        spatial.create_layer("mylayer")
        node = spatial.add_node_to_layer_by_id(
            node_id=node_id, geometry_name="work", wkt_string=shape.wkt,
            layer_name="mylayer")

        assert self._geometry_exists(spatial.graph, "work")
        assert self._node_exists(spatial.graph, node_id)

        spatial.delete_layer("mylayer")

        assert not self._geometry_exists(spatial.graph, "work")
        assert self._node_exists(spatial.graph, node_id)

    def test_delete_layer_and_contained_geometries(
            self, spatial, cornwall_wkt, devon_wkt):

        graph = spatial.graph

        spatial.create_layer("uk")
        spatial.create_geometry(
            geometry_name="cornwall", wkt_string=cornwall_wkt, layer_name="uk")
        spatial.create_geometry(
            geometry_name="devon", wkt_string=devon_wkt, layer_name="uk")

        assert self._geometry_exists(graph, geometry_name="cornwall")
        assert self._geometry_exists(graph, geometry_name="devon")
        spatial.delete_layer("uk")

        assert not self._geometry_exists(graph, geometry_name="cornwall")
        assert not self._geometry_exists(graph, geometry_name="devon")

        assert not self._layer_exists(graph, "uk")

    def test_layer_uniqueness(self, spatial):
        spatial.create_layer(LAYER_NAME)
        with pytest.raises(LayerExistsError):
            spatial.create_layer(LAYER_NAME)

    def test_layer_not_found(self, spatial):
        layer_name = "australia"  # does not exist

        with pytest.raises(LayerNotFoundError):
            spatial.get_layer(layer_name)
