import pytest

from py2neo import Node

from . import TestBase
from ..exceptions import GeometryExistsError, NodeNotFoundError
from ..plugin import NAME_PROPERTY
from ..util import parse_lat_long


class TestGeometries(TestBase):
    def test_create_polygon(self, spatial, cornwall_wkt):
        graph = spatial.graph
        geometry_name = "shape"
        spatial.create_layer("cornwall")
        spatial.create_geometry(
            geometry_name=geometry_name,
            wkt_string=cornwall_wkt,
            layer_name="cornwall"
        )

        assert self._geometry_exists(graph, geometry_name, "cornwall")

    def test_create_point(self, spatial):
        graph = spatial.graph
        geometry_name = "shape"
        spatial.create_layer("my_layer")

        shape = parse_lat_long((5.5, -4.5))
        assert shape.type == 'Point'

        spatial.create_geometry(
            geometry_name=geometry_name, wkt_string=shape.wkt,
            layer_name="my_layer")

        assert self._geometry_exists(graph, geometry_name, "my_layer")

    def test_geometry_node_label(self):
        pass

    def test_application_node_relationship(self):
        pass

    def test_make_existing_node_spatially_aware(self, spatial):
        graph = spatial.graph
        node = Node(address="300 St John Street, London.")
        graph.create(node)
        node_id = int(node.uri.path.segments[-1])
        coords = (51.528453, -0.104489)
        shape = parse_lat_long(coords)

        spatial.create_layer("mylayer")
        spatial.create_geometry(
            geometry_name="mygeom", wkt_string=shape.wkt,
            layer_name="mylayer", node_id=node_id)

        node = next(graph.find(
            label="mylayer", property_key=NAME_PROPERTY,
            property_value="mygeom"))

        labels = node.get_labels()
        properties = node.get_properties()

        assert labels == set(['py2neo_spatial', 'mylayer', 'Point'])
        assert properties[NAME_PROPERTY] == "mygeom"
        assert properties['address'] == "300 St John Street, London."

    def test_geometry_uniqueness(self, spatial, cornwall_wkt):
        geometry_name = "shape"

        spatial.create_layer("my_layer")
        spatial.create_geometry(
            geometry_name=geometry_name, wkt_string=cornwall_wkt,
            layer_name="my_layer")

        with pytest.raises(GeometryExistsError):
            spatial.create_geometry(
                geometry_name=geometry_name, wkt_string=cornwall_wkt,
                layer_name="my_layer")

    def test_delete_geometry(self, spatial, cornwall, cornwall_wkt):
        graph = spatial.graph
        assert self._geometry_exists(graph, "cornwall", "uk")
        spatial.delete_geometry("cornwall", cornwall_wkt, "uk")
        assert not self._geometry_exists(graph, "cornwall", "uk")

    def test_update_geometry(self, spatial):
        graph = spatial.graph

        # bad data
        bad_eiffel_tower = (57.322857, -4.424382)
        bad_shape = parse_lat_long(bad_eiffel_tower)

        spatial.create_layer("paris")
        spatial.create_geometry(
            geometry_name="eiffel_tower", wkt_string=bad_shape.wkt,
            layer_name="paris")

        assert self._geometry_exists(graph, "eiffel_tower", "paris")

        # good data
        eiffel_tower = (48.858370, 2.294481)
        shape = parse_lat_long(eiffel_tower)

        spatial.update_geometry("eiffel_tower", shape.wkt)

        node = self.get_geometry_node(graph, "eiffel_tower")
        node_properties = node.get_properties()

        assert node_properties['wkt'] == shape.wkt

    def test_update_geometry_not_found(self, spatial):
        coords = (57.322857, -4.424382)
        shape = parse_lat_long(coords)

        with pytest.raises(NodeNotFoundError):
            spatial.update_geometry("somewhere", shape.wkt)
