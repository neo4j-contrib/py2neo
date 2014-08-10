from shapely.geos import ReadingError
from shapely.wkt import loads as wkt_from_string_loader

from py2neo import Node, ServerPlugin
from . exceptions import (
    GeometryExistsError, LayerNotFoundError, InvalidWKTError)


EXTENSION_NAME = "SpatialPlugin"

PROVIDER = 'spatial'
WKT_PROPERTY = 'wkt'
# compatible config for the contrib spatial extension
WKT_CONFIG = {
    "provider": PROVIDER,
    "wkt": WKT_PROPERTY,
}

# shape identifiers
MULTIPOLYGON = 'MultiPolygon'
POINT = 'Point'

# a baseline label so we can retieve all data added via this extension
DEFAULT_LABEL = 'py2neo_spatial'


class Spatial(ServerPlugin):
    """ An API to the contrib Neo4j Spatial Extension for creating, destroying
    and querying Well Known Text (WKT) geometries over your own GIS map Layers.

    Each Layer you create is a collection of geometry nodes which form an
    R-tree index graph within your applications neo datastore - quite separate
    to what you may normally perceive as a "neo index". A traditional lucene
    index is also created for each Layer because not all your queries will be
    geographical.

    Internally, the R-tree index uses the WKBGeometryEncoder for storing all
    geometry types as byte[] properties of one node per geometry instance.

    .. note::

        An OSMLayer is also quite possible, but not implemented here.

        Any data added through this API can be visualised by compiling the
        Neo4j Spatial Extension for Geoserver, and this is encouraged, because
        it is tremendous fun! Please refer to this extension's documentation
        for basic guidance, however, these two projects are not coordinated.

        The (legacy) lucene index is created for each Layer because not all
        your queries will be geographical.

    """
    def __init__(self, graph):
        super(Spatial, self).__init__(graph, EXTENSION_NAME)

    def _get_shape(self, wkt_string):
        try:
            shape = wkt_from_string_loader(wkt_string)
        except ReadingError:
            raise InvalidWKTError(
                'Invalid WKT: {}'.format(wkt_string)
            )

        return shape

    def _geometry_exists(self, shape, geometry_name):
        match = """ MATCH (n:{label} """.format(label=shape.type)
        query = match + """ {name: {geometry_name}, wkt: {wkt}})
                            RETURN n """
        params = {
            'geometry_name': geometry_name,
            'wkt': shape.wkt,
        }

        exists = self.graph.cypher.execute(query, params)
        return bool(exists)

    def _layer_exists(self, layer_name):
        query = """ MATCH (l {layer: {layer_name}})<-[:LAYER]-()
                    RETURN l """

        params = {
            'layer_name': layer_name,
        }

        exists = self.graph.cypher.execute(query, params)
        return bool(exists)

    def create_layer(self, layer_name):
        """ Create a Layer to add geometries to.

        .. note::
            This directly translates to a Spatial Index in Neo of type WKT.

        """
        self.graph.legacy.get_or_create_index(
            Node, layer_name, config=WKT_CONFIG)

    def destroy_layer(self, layer_name, force=False):
        """ Destroy a Layer and all indexed nodes on it iff `force` is True.

        .. note::

            Internally this removes an index and all nodes that have
            been added to it. This is not a "cascade" delete, and will just
            remove the immediate nodes, and, because of this, may leave
            orphaned nodes behind - know your graph before calling this!

        """
        if not self._layer_exists(layer_name):
            raise LayerNotFoundError(
                'Layer Not Found: "{}"'.format(layer_name)
            )

        graph = self.graph

        if force:
            params = {
                'layer_name': layer_name
            }

            # remvove all nodes on the layer
            query = """ MATCH (n:{layer_name})
                        OPTIONAL MATCH n-[r]-()
                        DELETE r, n """.format(layer_name=layer_name)

            graph.cypher.execute(query)

            # remove the bounding box, metadata and root from the rtree index
            query = """ MATCH (l { layer: {layer_name} })-\
                        [r_layer:LAYER]-(),
                        (metadata)<-[r_meta:RTREE_METADATA]-(l),
                        (reference_node)-[r_ref:RTREE_REFERENCE]-\
                        (bounding_box)-[r_root:RTREE_ROOT]-(l)
                        DELETE r_meta, r_layer, r_ref, r_root,
                        metadata, reference_node, bounding_box, l """
            graph.cypher.execute(query, params)

            # remove lucene index - TODO: don't think this works
            graph.legacy.delete_index(Node, layer_name)

        else:
            # simply return what would be lost as this call can be devastating.
            print(
                'nothing is going to be deleted.\n'
                'use `force=True` to actually delete.\n'
                'doing dry run...'
            )

            # TODO: dry run results

    def create(
            self, geometry_name, wkt_string, layer_name, labels=None):
        """ Create a Well Known Text Node and add it to a GIS map layer.
        Optionaly add Labels to the Node.

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
            IndexNotFoundError if the index does not exist.
            InvalidWKTError if the WKT cannot be read.

        """
        if not self._layer_exists(layer_name):
            raise LayerNotFoundError(
                '\n',
                'Layer Not Found: "{0}".',
                '\n',
                'Use ``create_layer(layer_name="{0}"")`` first.'.format(
                    layer_name)
            )

        shape = self._get_shape(wkt_string)

        if self._geometry_exists(shape, geometry_name):
            raise GeometryExistsError(
                '\n',
                'geometry already exists.'
                '\n',
                'ignoring request.'
            )

        graph = self.graph
        labels = labels or []
        labels.extend([DEFAULT_LABEL, layer_name, shape.type])
        params = {
            WKT_PROPERTY: shape.wkt,
            'name': geometry_name,
        }

        node = Node(*labels, **params)
        graph.create(node)

        index = graph.legacy.get_index(Node, layer_name)
        index.add(WKT_PROPERTY, shape.wkt, node)

    def destroy(self, geometry_name, wkt_string, layer_name):
        """ Remove a geometry node from a GIS map layer.

        "Params:
            geometry_name : str
                The unique name of the geometry to delete.
            wkt_string : str
                A Well Known Text string of any geometry
            layer_name : str
                The name of the layer/index to remove the geometry from.

        :Raises:
            InvalidWKTError if the WKT cannot be read.

        """
        if not self._layer_exists(layer_name):
            raise LayerNotFoundError(
                'Layer Not Found: "{}"'.format(layer_name)
            )

        graph = self.graph
        shape = self._get_shape(wkt_string)

        # remove the node from the graph
        match = """ MATCH (n:{label} """.format(label=shape.type)
        query = match + """ { name: {geometry_name} })
                            OPTIONAL MATCH n<-[r]-()
                            DELETE r, n """
        params = {
            'label': shape.type,
            'geometry_name': geometry_name,
        }
        graph.cypher.execute(query, params)

        # tidy up the index. at time of writing there is *no* api for this,
        # so we are forced to do this manually. This will remove the node,
        # it's bounding box node, and the relationship between them.
        query = """ MATCH (l { layer: {layer_name} }),
                    (n { wkt:{wkt} })-[ref:RTREE_REFERENCE]-()
                    DELETE ref, n """
        params = {
            'layer_name': layer_name,
            'wkt': shape.wkt,
        }
        graph.cypher.execute(query, params)
