from unittest import skipUnless
from py2neo import Node, Relationship

from py2neo.ext.spatial.exceptions import GeometryExistsError, NodeNotFoundError
from py2neo.ext.spatial.plugin import NAME_PROPERTY
from py2neo.ext.spatial.util import parse_lat_long
from .basetest import SpatialTestCase, spatial_available


class GeometriesTestCase(SpatialTestCase):

    @skipUnless(spatial_available, "no spatial plugin available")
    def test_create_polygon(self):
        graph = self.spatial.graph
        geometry_name = "shape"
        self.spatial.create_layer("cornwall")
        self.spatial.create_geometry(
            geometry_name=geometry_name,
            wkt_string=self.cornwall_wkt,
            layer_name="cornwall"
        )

        assert self._geometry_exists(graph, geometry_name, "cornwall")

    @skipUnless(spatial_available, "no spatial plugin available")
    def test_create_points(self):
        graph = self.spatial.graph
        layer_name = 'point_layer'
        self.spatial.create_layer(layer_name)

        points = [
            ('a', (5.5, -4.5)), ('b', (2.5, -12.5)), ('c', (30.5, 10.5))
        ]

        for geometry_name, coords in points:
            shape = parse_lat_long(coords)
            assert shape.type == 'Point'

            self.spatial.create_geometry(
                geometry_name=geometry_name, wkt_string=shape.wkt,
                layer_name=layer_name)

            geometry_node = self.get_geometry_node(self.spatial, geometry_name)
            assert geometry_node

            application_node = self.get_application_node(self.spatial, geometry_name)
            assert application_node

            # ensure it has been given a label
            labels = application_node.labels()

            assert 'Point' in labels
            assert layer_name in labels

            # ensure the internal name property is set
            assert application_node[NAME_PROPERTY] == geometry_name

            # check it's relation to the Rtree
            query = """MATCH (an {_py2neo_geometry_name:{geometry_name}}),
            (an)<-[r:LOCATES]-(gn) RETURN r""" 
            params = {
                'geometry_name': geometry_name,
            }

            relationship = graph.evaluate(query, params)

            assert isinstance(relationship, Relationship)

    @skipUnless(spatial_available, "no spatial plugin available")
    def test_make_existing_node_spatially_aware(self):
        graph = self.spatial.graph
        node = Node(address="300 St John Street, London.")
        graph.create(node)
        node_id = int(node.remote._id)
        coords = (51.528453, -0.104489)
        shape = parse_lat_long(coords)

        self.spatial.create_layer("mylayer")
        self.spatial.create_geometry(
            geometry_name="mygeom", wkt_string=shape.wkt,
            layer_name="mylayer", node_id=node_id)

        node = next(graph.find(
            label="mylayer", property_key=NAME_PROPERTY,
            property_value="mygeom"))

        assert set(node.labels()) == {'py2neo_spatial', 'mylayer', 'Point'}
        assert node[NAME_PROPERTY] == "mygeom"
        assert node['address'] == "300 St John Street, London."

    @skipUnless(spatial_available, "no spatial plugin available")
    def test_geometry_uniqueness(self):
        geometry_name = "shape"

        self.spatial.create_layer("my_layer")
        self.spatial.create_geometry(
            geometry_name=geometry_name, wkt_string=self.cornwall_wkt,
            layer_name="my_layer")

        with self.assertRaises(GeometryExistsError):
            self.spatial.create_geometry(
                geometry_name=geometry_name, wkt_string=self.cornwall_wkt,
                layer_name="my_layer")

    @skipUnless(spatial_available, "no spatial plugin available")
    def test_delete_geometry(self):
        graph = self.spatial.graph
        assert self._geometry_exists(graph, "cornwall", "uk")
        self.spatial.delete_geometry("cornwall", self.cornwall_wkt, "uk")
        assert not self._geometry_exists(graph, "cornwall", "uk")

    @skipUnless(spatial_available, "no spatial plugin available")
    def test_update_geometry(self):
        graph = self.spatial.graph

        # bad data
        bad_eiffel_tower = (57.322857, -4.424382)
        bad_shape = parse_lat_long(bad_eiffel_tower)

        self.spatial.create_layer("paris")
        self.spatial.create_geometry(
            geometry_name="eiffel_tower", wkt_string=bad_shape.wkt,
            layer_name="paris")

        assert self._geometry_exists(graph, "eiffel_tower", "paris")

        # good data
        eiffel_tower = (48.858370, 2.294481)
        shape = parse_lat_long(eiffel_tower)

        self.spatial.update_geometry("eiffel_tower", shape.wkt)

        node = self.get_geometry_node(self.spatial, "eiffel_tower")

        assert node['wkt'] == shape.wkt

    @skipUnless(spatial_available, "no spatial plugin available")
    def test_update_geometry_not_found(self):
        coords = (57.322857, -4.424382)
        shape = parse_lat_long(coords)

        with self.assertRaises(NodeNotFoundError):
            self.spatial.update_geometry("somewhere", shape.wkt)
