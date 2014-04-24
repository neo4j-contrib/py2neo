from py2neo.exceptions import ClientError, ServerError
from py2neo.neo4j import GraphDatabaseService, Node, Cacheable, Resource, _hydrated
from py2neo.packages.jsonstream import grouped, assembled
from py2neo.packages.urimagic import URI
from py2neo.util import ustr, compact

__author__ = 'meier_ul'


class SpatialGraphDatabaseService(GraphDatabaseService):

    def __init__(self, uri=None):
        super(SpatialGraphDatabaseService, self).__init__(uri)
        if not self._has_spatial_extension:
            raise NotImplementedError("Neo4j Spatial-Plugin is not available")
        self.spatial_extension = _SpatialExtension.get_instance(URI(self).resolve("ext/SpatialPlugin"))

    @property
    def _has_spatial_extension(self):
        return "SpatialPlugin" in self.__metadata__[u'extensions']

    @property
    def spatial_root(self):
        nodes = list(self.find('ReferenceNode', 'name', 'spatial_root'))
        if nodes:
            return nodes[0]
        else:
            return None

    @property
    def layers(self):
        if not self.spatial_root:
            return None
        else:
            layers = []
            rels = self.spatial_root.match_outgoing(rel_type='LAYER')
            for rel in rels:
                layers.append(Layer(rel.end_node.__uri__))
            return layers

    def add_layer(self, name=None, layer_type=None, **kwargs):
        layer = Layer.create(self, name, layer_type, **kwargs)
        return layer

    def find_layer(self, name=None):
        if not name:
            raise ValueError('name can not be empty.')
        resource = Resource(ustr(URI(self.spatial_extension)) + u'/graphdb/getLayer')
        nodes = []
        try:
            for i, result in grouped(resource._post(compact({'layer': name}))):
                nodes.append(_hydrated(assembled(result)))
        except (ClientError, ServerError):
            return None
        layer = Layer(nodes[0].__uri__)
        return layer


class Layer(Node):

    @property
    def spatial_extension(self):
        return _SpatialExtension.get_instance(URI(self.graph_db).resolve("ext/SpatialPlugin"))

    @property
    def name(self):
        return self.__metadata__['data']['layer']

    @property
    def type(self):
        encoder = self.__metadata__['data']['geomencoder'].rsplit('.', 1)[1]
        if encoder == 'SimplePointEncoder':
            return 'POINT'
        elif encoder == 'WKTGeometryEncoder':
            return 'WKT'
        else:
            raise RuntimeError('Layer type unknown.')

    @property
    def config(self):
        try:
            return self.__metadata__['data']['geomencoder_config']
        except KeyError:
            if self.type == 'POINT':
                return 'lon:lat'
            else:
                raise

    @classmethod
    def create(cls, graph_database, name, layer_type, **kwargs):
        if not name or not layer_type:
            raise ValueError("Neither name or layer_type can be empty.")
        if not layer_type.upper() in ['POINT', 'WKT']:
            raise ValueError("layer_type must be one of 'POINT', 'WKT")
        params = {'layer': name}
        if layer_type.upper() == 'POINT':
            if 'lat' in kwargs:
                params['lat'] = kwargs['lat']
            else:
                params['lat'] = 'lat'
            if 'lon' in kwargs:
                params['lon'] = kwargs['lon']
            else:
                params['lon'] = 'lon'
            command = u'addSimplePointLayer'
        elif layer_type.upper() == 'WKT':
            params['format'] = 'WKT'
            if 'property_name' in kwargs:
                params['nodePropertyName'] = kwargs['property_name']
            else:
                params['nodePropertyName'] = 'wkt'
            command = u'addEditableLayer'
        else:
            raise RuntimeError('Could not create layer of type %s' % layer_type)
        resource = Resource(ustr(URI(graph_database.spatial_extension)) + u'/graphdb/' + command)
        nodes = []
        try:
            for i, result in grouped(resource._post(compact(params))):
                nodes.append(_hydrated(assembled(result)))
        except ClientError:
            raise
        layer = Layer(nodes[0].__uri__)
        return layer

    def add_node(self, node=None):
        if not node:
            raise ValueError('node can not be empty.')
        resource = Resource(ustr(URI(self.spatial_extension)) + u'/graphdb/addNodeToLayer')
        data = {"layer": self.name, "node": ustr(URI(node))}
        try:
            resource._post(compact(data))
        except ClientError:
            raise

    def update_geometry(self, node=None, geometry=None):
        if not node or not geometry:
            raise ValueError("Neither node or geometry can be empty.")
        resource = Resource(ustr(URI(self.spatial_extension)) + u'/graphdb/updateGeometryFromWKT')
        data = {"layer": self.name, "geometry": geometry, "geometryNodeId": node._id}
        try:
            resource._post(compact(data))
        except ClientError:
            raise

    def find_geometries_in_bbox(self, minx=None, maxx=None, miny=None, maxy=None):
        if not minx or not maxx or not miny or not maxy:
            raise ValueError("Neither minx or maxx or miny or maxy can be empty.")
        resource = Resource(ustr(URI(self.spatial_extension)) + u'/graphdb/findGeometriesInBBox')
        params = {'layer': self.name, 'minx': minx, 'maxx': maxx, 'miny': miny, 'maxy': maxy}
        nodes = []
        try:
            for i, result in grouped(resource._post(compact(params))):
                nodes.append(_hydrated(assembled(result)))
        except ClientError:
            raise
        return nodes

    def find_geometries_within_distance(self, x=None, y=None, distance=None):
        if not x or not y or not distance:
            raise ValueError("Neither x, y or distance can be empty.")
        resource = Resource(ustr(URI(self.spatial_extension)) + u'/graphdb/findGeometriesWithinDistance')
        params = {'layer': self.name, 'pointX': x, 'pointY': y, 'distanceInKm': distance}
        nodes = []
        try:
            for i, result in grouped(resource._post(compact(params))):
                nodes.append(_hydrated(assembled(result)))
        except ClientError:
            raise
        return nodes


class _SpatialExtension(Cacheable, Resource):
    def __init__(self, *args, **kwargs):
        Resource.__init__(self, *args, **kwargs)