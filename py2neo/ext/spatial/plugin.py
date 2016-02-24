#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright 2015, Simon Harrison
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
Extended py2neo REST API for the Neo4j Spatial Extension
========================================================

py2neo Spatial is an extension to the py2neo project and provides REST APIs
for the Neo4j Spatial Extension.

The Neo4j Spatial Extension should be considered in two ways: embedded server
and REST API, and the former has a richer API than the latter. The REST API was
more of a prototype than a complete product and designed primarily to support
Point objects and some WKT APIs.

py2neo Spatial implements *some* of the "prototype" JAVA REST API for *WKT*
geometries. The current status is::

    implemented = [
        # add an editable WKT geometry encoded Layer to the graph
        'addEditableLayer',
        # get a (WKT) Layer from the graph
        'getLayer',
        # add a Node with a WKT geometry property to an EditableLayer
        'addGeometryWKTToLayer',
        # add an existing, non-geographically aware Node, to a Layer
        'addNodeToLayer',
        # update the geometry on a Node
        'updateGeometryFromWKT',
        # query APIs
        'findGeometriesWithinDistance',
        'find_within_bounding_box',
    ]

    not_implemented = [
        # for bespoke and optimised layer queries. no case for this yet.
        'addCQLDynamicLayer',
        # for bulk `addNodeToLayer` type requests. likely to follow.
        'addNodesToLayer',
        # not implemented. currently prefer WKT Nodes on EditableLayers.
        'addSimplePointLayer',
        # this appears to be broken upstream, with the `distanceInKm` behaving
        # more like a tolerance. No test cases could be written against this.
        'findClosestGeometries',
    ]

"""
try:
    from shapely.geos import ReadingError
    from shapely.wkt import loads as wkt_from_string_loader
    from shapely.wkt import dumps
except ImportError:
    print("Please install extension requirements. See README.rst.")

from py2neo import Node, GraphError
from py2neo.ext import ServerPlugin
from py2neo.packages.jsonstream import assembled
from py2neo.ext.spatial.exceptions import (
    AddNodeToLayerError, GeometryExistsError, GeometryNotFoundError,
    InvalidWKTError, LayerExistsError, LayerNotFoundError, NodeNotFoundError,
    ValidationError)
from py2neo.ext.spatial.util import parse_lat_long_to_point


EXTENSION_NAME = "SpatialPlugin"
NAME_PROPERTY = "geometry_name"

# extension configs exist for point and wkt geometries, but py2neo Spatial
# only implements type WKT
WKT_PROPERTY = "wkt"
# wkt index config for the contrib spatial extension
EXTENSION_CONFIG = {
    'format': 'WKT',
    'nodePropertyName': WKT_PROPERTY,
}

# shape identifiers
MULTIPOLYGON = 'MultiPolygon'
POLYGON = 'Polygon'
POINT = 'Point'

# a baseline label so we can retieve all data added via this extension
DEFAULT_LABEL = 'py2neo_spatial'


class Spatial(ServerPlugin):
    """ A py2neo extension for WKT type GIS operations """

    def __init__(self, graph):
        super(Spatial, self).__init__(graph, EXTENSION_NAME)

    def _assemble(self, json_stream):
        nodes = map(Node.hydrate, assembled(json_stream))
        return nodes

    def _handle_post_from_resource(self, resource, spatial_payload):
        try:
            json_stream = resource.post(spatial_payload)
        except GraphError as exc:
            if 'NullPointerException' in exc.fullname:
                # no results leads to a NullPointerException.
                # this is probably a bug on the Java side, but this
                # happens with some resources and must be managed.
                return []
            raise

        if json_stream.status_code == 204:
            # no content
            return []

        return json_stream

    def _get_shape_from_wkt(self, wkt_string):
        try:
            shape = wkt_from_string_loader(wkt_string)
        except ReadingError:
            raise InvalidWKTError(
                'Invalid WKT:{}'.format(wkt_string)
            )

        return shape

    def _geometry_exists(self, shape, geometry_name):
        match = "MATCH (n:{label} ".format(label=shape.type)
        query = match + "{geometry_name:{geometry_name}}) RETURN n"
        params = {
            'geometry_name': geometry_name,
            'wkt': shape.wkt,
        }

        exists = self.graph.cypher.execute(query, params)

        return bool(exists)

    def _layer_exists(self, layer_name):
        query = "MATCH (l {layer:{layer_name}})<-[:LAYER]-() RETURN l"
        params = {'layer_name': layer_name}
        exists = self.graph.cypher.execute(query, params)

        return bool(exists)

    def create_layer(self, layer_name):
        """ Create a GIS map Layer to add geometries to. The Layer is encoded
        by the WKTGeometryEncoder.

        :Parameters:
            layer_name : str
                The name to give the Layer created. Must be unique.

        :Returns:
            An HTTP status code.

        :Raises:
            LayerExistsError
                If a Layer with `layer_name` already exists.

        """
        resource = self.resources['addEditableLayer']

        if self._layer_exists(layer_name):
            raise LayerExistsError(
                'Layer Exists: "{}"'.format(layer_name)
            )

        spatial_data = dict(layer=layer_name, **EXTENSION_CONFIG)
        raw = resource.post(spatial_data)

        return raw.status_code

    def get_layer(self, layer_name):
        """ Get the Layer identified by `layer_name`.

        :Parameters:
            layer_name : str
                The name of the Layer to return.

        :Returns:
            The Layer Node.

        :Raises:
            LayerNotFoundError
                If a Layer with `layer_name` does not exist.

        """
        resource = self.resources['getLayer']

        spatial_data = dict(layer=layer_name, **EXTENSION_CONFIG)
        raw = self._handle_post_from_resource(resource, spatial_data)
        if not raw:
            raise LayerNotFoundError(
                'Layer Not Found: "{}"'.format(layer_name)
            )

        nodes = self._assemble(raw)
        layer_node = nodes[0]

        return layer_node

    def delete_layer(self, layer_name):
        """ Delete a GIS map Layer.

        This will remove a representation of a GIS map Layer from the Neo4j
        data store - it will not remove any nodes you may have added to it, or
        any labels or properties py2neo Spatial may have added to your own
        Nodes.

        :Parameters:
            layer_name : str
                The name of the Layer to delete.

        :Returns:
            None

        :Raises:
            LayerNotFoundError
                When the Layer to delete does not exist.

        .. note::
            The return type here is `None` and not an HTTP status because
            there is no Neo4j Spatial Extension REST endpoint for deleting a
            layer, so we use the Py2neo cypher API, which returns nothing on
            delete.

        """
        if not self._layer_exists(layer_name):
            raise LayerNotFoundError(
                'Layer Not Found: "{}"'.format(layer_name)
            )

        # remove the bounding box, metadata and root from the rtree index
        query = (
            "MATCH (layer { layer:{layer_name} })-[r_layer:LAYER]-(), "
            "(metadata)<-[r_meta:RTREE_METADATA]-(l), "
            "(geometry_node)-[r_ref:RTREE_REFERENCE]-"
            "(bounding_box)-[r_root:RTREE_ROOT]-(l) "
            "DELETE r_meta, r_layer, r_ref, r_root, "
            "metadata, bounding_box, layer"
        )

        params = {
            'layer_name': layer_name
        }

        self.graph.cypher.execute(query, params)

    def add_node_to_layer_by_id(
            self, node_id, layer_name, geometry_name, wkt_string, labels=None):
        """ Add a non-geographically aware Node to a Layer (spatial index) by
        it's internal ID. This is any Node without a WKT property, as defined
        by the extensions `WKT_PROPERTY` value.

        :Parameters:
            node_id : int
                The internal Neo4j identifier of a Node.
            layer_name : string
                The name of the Layer (index) to add the Node to.
            geometry_name : string
                A unique name to give the geometry.
            wkt_string : string
                A WKT geometry to give the Node.
            labels : list
                An optional list of labels to give to the Node.

        :Returns:
            An HTTP status code.

        :Raises:
            NodeNotFoundError
                When a Node with ID `node_id` cannot be found.
            GeometryExistsError
                When a geometry with `geometry_name` already exists.

        """
        resource = self.resources['addNodeToLayer']

        query = 'MATCH (n) WHERE id(n) = {node_id} RETURN n'
        params = {'node_id': node_id}
        results = self.graph.cypher.execute(query, params)
        if not results:
            raise NodeNotFoundError(
                'Node not found: "{}"'.format(node_id)
            )

        shape = self._get_shape_from_wkt(wkt_string)

        if self._geometry_exists(shape, geometry_name):
            raise GeometryExistsError(
                'Geometry exists: "{}"'.format(geometry_name)
            )

        record = results[0]
        node = record[0]
        node[NAME_PROPERTY] = geometry_name
        node[WKT_PROPERTY] = wkt_string

        labels = labels or []
        labels.extend([DEFAULT_LABEL, layer_name, shape.type])

        node.add_labels(*tuple(labels))
        node.push()

        # add the node to the spatial index
        spatial_data = {
            'node': str(node.uri),
            'layer': layer_name
        }

        raw = self._handle_post_from_resource(resource, spatial_data)
        status_code = raw.status_code

        return status_code

    def create_point_of_interest(
            self, poi_name, layer_name, longitude, latitude,
            labels=None, node_properties=None):
        """ Create a Point of Interest (POI) on a Layer.

        :Parameters:
            layer_name : str
                The Layer to add the POI to.
            poi_name: str
                A unique name for the POI.
            longitude : Decimal
                Decimal number between -180.0 and 180.0, east or west of the
                Prime Meridian.
            latitude : Decimal
                Decimal number between -90.0 and 90.0, north or south of the
                equator.
            labels : list
                Optional list of labels to apply to the POI Node.
            node_properties : dict
                Optional keyword arguments to apply as properties on the Node.

        :Returns:
            An HTTP status_code.

        :Raises:
            LayerNotFoundError
                When the Layer does not exist.
            GeometryExistsError
                When a geometry with `geometry_name` already exists.

        """
        resource = self.resources['addNodeToLayer']

        if not self._layer_exists(layer_name):
            raise LayerNotFoundError(
                'Layer Not Found: "{0}".',
                'Use ``create_layer(layer_name="{0}"")`` first.'.format(
                    layer_name)
            )

        shape = parse_lat_long_to_point(latitude, longitude)

        if self._geometry_exists(shape, poi_name):
            raise GeometryExistsError(
                'geometry exists: "{}"'.format(poi_name)
            )

        graph = self.graph

        labels = labels or []
        node_properties = node_properties or {}

        labels.extend([DEFAULT_LABEL, layer_name, shape.type])
        wkt = dumps(shape, rounding_precision=4)

        params = {
            NAME_PROPERTY: poi_name,
            WKT_PROPERTY: wkt,
        }

        params.update(node_properties)
        node = Node(*labels, **params)
        graph.create(node)

        # add the node to the spatial index
        spatial_data = {
            'node': str(node.uri),
            'layer': layer_name
        }

        http_response = resource.post(spatial_data)
        status_code = http_response.status_code

        if status_code != 200:
            raise AddNodeToLayerError(
                'Failed to add POI "{}" to layer "{}"'.format(
                    poi_name, layer_name)
            )

        return status_code

    def create_geometry(
            self, geometry_name, layer_name, wkt_string,
            labels=None, node_properties=None):
        """ Create a geometry Node with any WKT string type.

        :Parameters:
            geometry_name : string
                A unique name to give the geometry.
            layer_name : string
                The name of the Layer to add the Node to.
            wkt_string : string
                A WKT geometry to give the Node.
            labels : list
                An optional list of labels to give to the Node.
            node_properties : dict
                Optional keyword arguments to apply as properties on the Node.

        :Returns:
            An HTTP status code.

        :Raises:
            LayerNotFoundError
                When the Layer does not exist.
            GeometryExistsError
                When a geometry with `geometry_name` already exists.

        """
        resource = self.resources['addGeometryWKTToLayer']

        # validate WKT
        shape = self._get_shape_from_wkt(wkt_string)

        # validate layer
        if not self._layer_exists(layer_name):
            raise LayerNotFoundError(
                'Layer Not Found: "{0}".',
                'Use ``create_layer(layer_name="{0}"")`` first.'.format(
                    layer_name)
            )

        # validate uniqueness
        if self._geometry_exists(shape, geometry_name):
            raise GeometryExistsError(
                'geometry already exists. ignoring request.'
            )

        labels = labels or []
        node_properties = node_properties or {}

        labels.extend([DEFAULT_LABEL, layer_name, shape.type])

        spatial_data = {
            'geometry': wkt_string,
            'layer': layer_name,
        }

        http_response = resource.post(spatial_data)
        status_code = http_response.status_code
        if status_code != 200:
            return status_code

        content = http_response.content[0]
        node_id = content['metadata']['id']

        # update the geometry node with provided node properties and labels
        # manually as the upstream API does not except extra args in tbis case
        query = (
            "MATCH geometry_node WHERE id(geometry_node) = {node_id} "
            "RETURN geometry_node"
        )

        params = {
            'node_id': node_id,
        }

        results = self.graph.cypher.execute(query, params)
        result = results[0]
        node = result.geometry_node

        params = {
            NAME_PROPERTY: geometry_name,
        }

        params.update(node_properties)
        node.properties.update(params)
        node.labels.update(set(labels))
        node.push()

        return status_code

    def update_geometry(self, layer_name, geometry_name, new_wkt_string):
        """ Update the WKT geometry on a Node.

        :Parameters:
            layer_name : str
                The name of the Layer containing the geometry to update.
            geometry_name : str
                The name of the Node geometry to update.
            new_wkt_string : str
                The new Well Known Text string that will replace that
                existing on the Node with `geometry_name`.

        :Returns:
            An HTTP status code.

        :Raises:
            GeometryNotFoundError
                When the geometry to update cannot be found.
            InvalidWKTError
                When the `new_wkt_string` is invalid.
            LayerNotFoundError
                When the Layer does not exist.

        """
        resource = self.resources['updateGeometryFromWKT']

        if not self._layer_exists(layer_name):
            raise LayerNotFoundError(
                'Layer Not Found: "{}"'.format(layer_name)
            )

        # validate WKT string
        self._get_shape_from_wkt(new_wkt_string)

        query = (
            "MATCH (geometry_node {geometry_name:{geometry_name}}) "
            "<-[:RTREE_REFERENCE]-()<-[:RTREE_ROOT]-"
            "(layer_node {layer:{layer_name}}) "
            "RETURN geometry_node"
        )

        params = {
            'layer_name': layer_name,
            'geometry_name': geometry_name,
        }

        result = self.graph.cypher.execute(query, params)

        if not result:
            raise GeometryNotFoundError(
                'Cannot update Node - Node not found: "{}"'.format(
                    geometry_name)
            )

        result = result[0]
        geometry_node = result.geometry_node
        geometry_node_id = int(geometry_node.uri.path.segments[-1])

        spatial_data = {
            'geometry': new_wkt_string,
            'geometryNodeId': geometry_node_id,
            'layer': layer_name,
        }

        # update the geometry node
        http_response = self._handle_post_from_resource(resource, spatial_data)
        status_code = http_response.status_code

        return status_code

    def delete_geometry(self, layer_name, geometry_name):
        """ Remove a geometry Node from a Layer.

        .. note::
            This does not delete the Node itself, just removes it from the
            spatial index. Use the standard py2neo API to delete Nodes.

        :Parameters:
            layer_name : str
                The name of the Layer to remove the geometry from.
            geometry_name : str
                The unique name of the geometry to delete.

        :Returns:
            None

        :Raises:
            LayerNotFoundError
                When the Layer does not exist.

        .. note::
            The return type here is `None` and not an HTTP status because
            there is no Neo4j Spatial Extension REST endpoint for deleting a
            layer, so we use the Py2neo cypher API, which returns nothing on
            delete.

        """
        if not self._layer_exists(layer_name):
            raise LayerNotFoundError(
                'Layer Not Found: "{}"'.format(layer_name)
            )

        # find the geographically aware Node and remove its relationship to
        # the spatial index.
        query = (
            "MATCH (geometry_node {geometry_name:{geometry_name}}) "
            "<-[ref:RTREE_REFERENCE]-()<-[:RTREE_ROOT]-"
            "(layer_node {layer:{layer_name}}) "
            "DELETE ref"
        )

        params = {
            'layer_name': layer_name,
            'geometry_name': geometry_name,
        }

        self.graph.cypher.execute(query, params)

    def find_within_distance(self, layer_name, longitude, latitude, distance):
        """ Find all WKT geometry primitive within a given distance from
        location coord.

        :Parameters:
            layer_name : str
                The name of the Layer to remove the geometry from.
            longitude : Decimal
                Decimal number between -180.0 and 180.0, east or west of the
                Prime Meridian.
            latitude : Decimal
                Decimal number between -90.0 and 90.0, north or south of the
                equator.
            distance : int
                The radius of the search area in Kilometres (km).

        :Returns:
            A list of all matched nodes.

        :Raises:
            LayerNotFoundError
                When the Layer does not exist.

        """
        resource = self.resources['findGeometriesWithinDistance']

        if not self._layer_exists(layer_name):
            raise LayerNotFoundError(
                'Layer Not Found: "{}"'.format(layer_name)
            )

        # validate coordinates
        shape = parse_lat_long_to_point(latitude, longitude)
        spatial_data = {
            'pointX': shape.x,
            'pointY': shape.y,
            'layer': layer_name,
            'distanceInKm': distance,
        }

        raw = self._handle_post_from_resource(resource, spatial_data)
        nodes = self._assemble(raw)

        return nodes

    def find_within_bounding_box(self, layer_name, minx, miny, maxx, maxy):
        """ Find the points of interest from a given layer enclosed by a
        bounding box.

        The bounding box is definded by the lat-longs of the bottom left and
        the top right, essentially::

            bbox = (min Longitude, min Latitude, max Longitude, max Latitude)

        :Parameters:
            layer_name : str
                The name of the Layer to remove the geometry from.
            minx : Decimal
                Longitude of the bottom-left corner.
            miny : Decimal
                Latitude of the bottom-left corner.
            maxx : Decimal
                Longitude of the top-right corner.
            minx : Decimal
                Latitude of the top-right corner.

        :Returns:
            A list of all matched nodes in order of distance.

        """
        resource = self.resources['findGeometriesInBBox']

        spatial_data = {
            'layer': layer_name,
            'minx': minx,
            'maxx': maxx,
            'miny': miny,
            'maxy': maxy,
        }

        raw = self._handle_post_from_resource(resource, spatial_data)
        nodes = self._assemble(raw)

        return nodes

    def find_containing_geometries(self, layer_name, longitude, latitude):
        """ Given the position of a point of interest, find all the geometries
        that contain it on a given Layer.

        :Parameters:
            layer_name : str
                The name of the Layer to find containing geometries from.
            longitude : Decimal
                Decimal number between -180.0 and 180.0, east or west of the
                Prime Meridian.
            latitude : Decimal
                Decimal number between -90.0 and 90.0, north or south of the
                equator.

        :Returns:
            A list of Nodes of geometry type Polygon or MultiPolygon.

        """
        resource = self.resources['findGeometriesWithinDistance']

        shape = parse_lat_long_to_point(latitude, longitude)
        spatial_data = {
            'pointX': shape.x,
            'pointY': shape.y,
            'layer': layer_name,
            'distanceInKm': 0,  # set this to zero to ensure POI is contained
        }

        raw = self._handle_post_from_resource(resource, spatial_data)
        nodes = self._assemble(raw)

        # exclude any Points or LineString geometries etc we may have collected
        polygon_nodes = [
            node for node in nodes if
            MULTIPOLYGON in node.labels or POLYGON in node.labels
        ]

        return polygon_nodes

    def find_points_of_interest(
            self, layer_name, longitude, latitude, max_distance, labels):
        """ Given a coordinate, find the Point geometries that are "nearby" it
        and are "interesting". Only Nodes that have matching `labels` are
        deemed "interesting".

        :Parameters:
            layer_name : str
                The name of the Layer to remove the geometry from.
            longitude : Decimal
                Decimal number between -180.0 and 180.0, east or west of the
                Prime Meridian.
            latitude : Decimal
                Decimal number between -90.0 and 90.0, north or south of the
                equator.
            max_distance : int
                The radius of the search area to look for "interesting" Points.
            labels : list
                List of node labels which determine which points are
                "interesting".

        :Returns:
            A list of interesting Nodes.

        """
        resource = self.resources['findGeometriesWithinDistance']

        if not self._layer_exists(layer_name):
            raise LayerNotFoundError(
                'Layer Not Found: "{}"'.format(layer_name)
            )

        if len(labels) == 0:
            raise ValidationError('At least one label must be given')

        interesting_labels = set(labels)

        # validates coordinates before use
        shape = parse_lat_long_to_point(latitude, longitude)
        spatial_data = {
            'pointX': shape.x,
            'pointY': shape.y,
            'layer': layer_name,
            'distanceInKm': max_distance,
        }

        raw = self._handle_post_from_resource(resource, spatial_data)
        nodes = self._assemble(raw)

        interesting_nodes = [
            node for node in nodes if
            interesting_labels.intersection(node.labels)
        ]

        return interesting_nodes
