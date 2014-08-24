try:
    from shapely.geos import ReadingError
    from shapely.wkt import loads as wkt_from_string_loader
except ImportErrror:
    print("Please install extension requirements. See README.")

from py2neo import Node, ServerPlugin
from py2neo.error import GraphError
from py2neo.packages.jsonstream import assembled
from .exceptions import (
    GeometryExistsError, LayerNotFoundError, InvalidWKTError)
from util import parse_lat_long


EXTENSION_NAME = "SpatialPlugin"
WKT_PROPERTY = "wkt"

# wkt index config for the contrib spatial extension
EXTENSION_CONFIG = {
    'format': 'WKT',
    'nodePropertyName': WKT_PROPERTY,
}

# shape identifiers
MULTIPOLYGON = 'MultiPolygon'
POINT = 'Point'

# a baseline label so we can retieve all data added via this extension
DEFAULT_LABEL = 'py2neo_spatial'


class Spatial(ServerPlugin):
    """ An API to the contrib Neo4j Spatial Extension for creating, destroying
    and querying Well Known Text (WKT) geometries over GIS map Layers.

    Each Layer you create will build a sub-graph modelling geographically aware
    nodes as an R-tree - which is your magical spatial index! You will also have
    a standard Lucene index for this data because not all your queries will be
    spatial.

    .. note::

        Internally, the R-tree index uses the WKBGeometryEncoder for storing
        all geometry types as byte[] properties of one node per geometry
        instance.

        An OSMLayer is also quite possible, but not implemented here.

        Any data added through this API can be visualised by compiling the
        Neo4j Spatial Extension for Geoserver, and this is encouraged, because
        it is tremendous fun! Please refer to this extension's documentation
        for basic guidance, however, these two projects are not coordinated.

    """
    def __init__(self, graph):
        """ An API extension to py2neo to take advantage of (some of) the REST
        resources provided by the contrib neo4j spatial extension.

        Implemented end-points are: ```addEditableLayer```, ```getLayer```,
        ```addGeometryWKTToLayer``` and ```findClosestGeometries```.

        TODO: updateGeometryFromWKT, findGeometriesInBBox,
        findGeometriesWithinDistance

        .. note::

            For now, this api prefers WKT indexes and geometries. Because of this
            the following are not implemented:

                addSimplePointLayer
                    - no plan to implement as WKT is preferred
                addNodeToLayer
                    - no plan to implement the point geometry type
                addNodesToLayer
                    - no plan to implement point geometry type or batch inserts
                addCQLDynamicLayer
                    - no idea what this is

        """
        super(Spatial, self).__init__(graph, EXTENSION_NAME)

    def _get_shape(self, wkt_string):
        try:
            shape = wkt_from_string_loader(wkt_string)
        except ReadingError:
            raise InvalidWKTError(
                'Invalid WKT:{}'.format(wkt_string)
            )

        return shape

    def _geometry_exists(self, shape, geometry_name):
        match = "MATCH (n:{label}".format(label=shape.type)
        query = match + """{name:{geometry_name}, wkt:{wkt}})
RETURN n"""
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
        """ Create a Layer to add geometries to. If a Layer with the
        name property value of ``layer_name`` already exists, nothing
        happens.

        .. note::
            This directly translates to a Spatial Index in Neo of type WKT.

        """
        resource = self.resources['addEditableLayer']
        spatial_data = dict(layer=layer_name, **EXTENSION_CONFIG)
        raw = resource.post(spatial_data)
        layer = assembled(raw) 
        return layer

    def get_layer(self, layer_name):
        resource = self.resources['getLayer']
        spatial_data = dict(layer=layer_name, **EXTENSION_CONFIG)
        raw = resource.post(spatial_data)
        layer = assembled(raw) 
        return layer

    def delete_layer(self, layer_name):
        """ Remove a GIS map Layer.

        .. note ::
            There is no Resource for this action so in the meantime we
            use the core py2neo cypher api.

        This will remove a representation of a GIS map Layer from the Neo4j
        data store - it will not remove any nodes you may have added to it.

        The operation removes the layer data from the internal GIS R-tree model,
        removes the neo indexes (lucene and spatial) and removes the layer's
        label from all nodes that exist on it. It does not want to destroy any
        Nodes on the DB - use the standard py2neo library for these actions.

        :Raises:
            LayerNotFoundError if the index does not exist.

        """
        if not self._layer_exists(layer_name):
            raise LayerNotFoundError(
                'Layer Not Found: "{}"'.format(layer_name)
            )

        graph = self.graph
        params = {
            'layer_name': layer_name
        }

        # remove labels on Nodes relating to this layer
        query = """MATCH (n:{layer_name})
REMOVE n:{default_label}
REMOVE n:{layer_name}
REMOVE n:{point_label}
REMOVE n:{multipolygon_label}""".format(
            layer_name=layer_name, default_label=DEFAULT_LABEL,
            point_label=POINT, multipolygon_label=MULTIPOLYGON,
        )

        graph.cypher.execute(query)

        # remove the bounding box, metadata and root from the rtree index
        query = """MATCH (l { layer:{layer_name} })-\
[r_layer:LAYER]-(),
(metadata)<-[r_meta:RTREE_METADATA]-(l),
(reference_node)-[r_ref:RTREE_REFERENCE]-
(bounding_box)-[r_root:RTREE_ROOT]-(l)
DELETE r_meta, r_layer, r_ref, r_root,
metadata, reference_node, bounding_box, l"""

        graph.cypher.execute(query, params)

        # remove lucene index
        graph.legacy.delete_index(Node, layer_name)

    def create_geometry(
            self, geometry_name, wkt_string, layer_name, labels=None):
        """ Create a geometry of type Well Known Text (WKT).

        Internally this creates a node in your graph with a wkt property
        and also adds it to a GIS map layer (an index). Optionaly add
        Labels to the Node.

        :Params:
            geometry_name : str
                A unique name for the geometry.
            wkt_string : str
                A Well Known Text string of any geometry
            layer_name : str
                The name of the layer to add the geometry to.
            labels : list
                Optional list of Label names to apply to the geometry Node.

        :Raises:
            LayerNotFoundError if the index does not exist.
            InvalidWKTError if the WKT cannot be read.

        TODO: accept an optional node id to add the geometry property to
        so we can make existing nodes geographically aware.

        """
        if not self._layer_exists(layer_name):
            raise LayerNotFoundError(
                'Layer Not Found: "{0}".',
                'Use ``create_layer(layer_name="{0}"")`` first.'.format(
                    layer_name)
            )

        shape = self._get_shape(wkt_string)

        if self._geometry_exists(shape, geometry_name):
            raise GeometryExistsError(
                'geometry already exists. ignoring request.'
            )

        graph = self.graph
        resource = self.resources['addGeometryWKTToLayer']

        labels = labels or []
        labels.extend([DEFAULT_LABEL, layer_name, shape.type])

        params = {
            WKT_PROPERTY: shape.wkt,
            'name': geometry_name,
        }
        node = Node(*labels, **params)
        graph.create(node)

        spatial_data = {'geometry': shape.wkt, 'layer': layer_name}
        resource.post(spatial_data)

    def delete_geometry(self, geometry_name, wkt_string, layer_name):
        """ Remove a geometry node from a GIS map layer.

        .. note ::
            There is no Resource for this action so in the meantime we
            use the core py2neo cypher api.

        :Params:
            geometry_name : str
                The unique name of the geometry to delete.
            wkt_string : str
                A Well Known Text string of any geometry
            layer_name : str
                The name of the layer/index to remove the geometry from.

        :Raises:
            LayerNotFoundError if the index does not exist.
            InvalidWKTError if the WKT cannot be read.

        """
        if not self._layer_exists(layer_name):
            raise LayerNotFoundError(
                'Layer Not Found: "{}"'.format(layer_name)
            )

        graph = self.graph
        shape = self._get_shape(wkt_string)

        # remove the node from the graph
        match = """MATCH (n:{label}""".format(label=shape.type)
        query = match + """{ name:{geometry_name} })
OPTIONAL MATCH n<-[r]-()
DELETE r, n"""
        params = {
            'label': shape.type,
            'geometry_name': geometry_name,
        }
        graph.cypher.execute(query, params)

        # tidy up the index. This will remove the node,
        # it's bounding box node, and the relationship between them.
        query = """MATCH (l { layer:{layer_name} }),
(n { wkt:{wkt} })-[ref:RTREE_REFERENCE]-()
DELETE ref, n"""
        params = {
            'layer_name': layer_name,
            'wkt': shape.wkt,
        }
        graph.cypher.execute(query, params)

    def find_closest_geometries(self, layer_name, coords, distance):
        """ Find the "closest" points of interest (poi) within a given 
        distance from a lat-lon location coord.

        TODO: what is the difference between "closest" and "find within"?

        :Params:
            layer_name : str
                The name of the layer/index to remove the geometry from.
            coords : tuple
                WGS84 (EPSG 4326) lat, lon pair
            distance : int
                The radius of the search area in Kilometres (km)

        :Raises:
            LayerNotFoundError if the index does not exist.

        """
        if not self._layer_exists(layer_name):
            raise LayerNotFoundError(
                'Layer Not Found: "{}"'.format(layer_name)
            )

        resource = self.resources['findClosestGeometries']
        shape = parse_lat_long(coords)

        spatial_data = {
            'layer': layer_name,
            'pointX': shape.x, 
            'pointY': shape.y, 
            'distanceInKm': distance,
        }

        try:
            json_stream = resource.post(spatial_data)
        except GraphError as exc:
            if 'NullPointerException' in exc.full_name:
                # no results leads to a NullPointerException
                return []
            raise

        nodes = map(Node.hydrate, assembled(json_stream))
        return nodes

    def find_within_distance(self):
        resource = self.resources['findGeometriesWithinDistance']
