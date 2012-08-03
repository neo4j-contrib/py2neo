#!/usr/bin/env python

# Copyright 2011 Nigel Small
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

"""The neo4j module provides the main Neo4j client functionality and will be
the starting point for most applications.
"""

try:
    import simplejson as json
except ImportError:
    import json
try:
    from urllib.parse import quote
except ImportError:
    from urllib import quote
try:
    from . import rest, batch, cypher
except ImportError:
    import rest, batch, cypher
except ValueError:
    import rest, batch, cypher
import logging


__author__    = "Nigel Small <py2neo@nigelsmall.org>"
__copyright__ = "Copyright 2011 Nigel Small"
__license__   = "Apache License, Version 2.0"


DEFAULT_URI = "http://localhost:7474/db/data/"

logger = logging.getLogger(__name__)



def _numberise(n):
    """Convert a value to an integer if possible, simply returning the input
    value itself if not.
    """
    try:
        return int(n)
    except ValueError:
        return n

def _quote(string, safe='/'):
    try:
        return quote(string, safe)
    except Exception:
        return string


class Direction(object):
    """Used to define the direction of a relationship.
    """

    BOTH     = 'all'
    INCOMING = 'incoming'
    OUTGOING = 'outgoing'


def _direction_prefix(direction):
    if direction == Direction.BOTH:
        return "all"
    elif direction == Direction.INCOMING:
        return "incoming"
    elif direction == Direction.OUTGOING:
        return "outgoing"


class GraphDatabaseService(rest.Resource):
    """An instance of a `Neo4j <http://neo4j.org/>`_ database identified by its
    base URI. Generally speaking, this is the only URI which a system
    attaching to this service should need to be directly aware of; all further
    entity URIs will be discovered automatically from within response content
    when possible (see `Hypermedia <http://en.wikipedia.org/wiki/Hypermedia>`_)
    or will be derived from existing URIs.

    :param uri:       the base URI of the database (defaults to
                      <http://localhost:7474/db/data/>)
    :param metadata:  optional resource metadata

    The following code illustrates how to connect to a database server and
    display its version number::

        from py2neo import rest, neo4j
        uri = "http://localhost:7474/db/data/"
        try:
            graph_db = neo4j.GraphDatabaseService(uri)
            print graph_db.neo4j_version
        except rest.NoResponse:
            print "Cannot connect to host"
        except rest.ResourceNotFound:
            print "Database service not found"

    """

    def __init__(self, uri=None, metadata=None):
        uri = uri or DEFAULT_URI
        rest.Resource.__init__(self, uri, "/", metadata=metadata)
        # force metadata update to populate `_last_response` attribute
        self._metadata.update(self._get(self._uri))
        # force URI adjustment (in case supplied without trailing slash)
        self._uri = rest.URI(self._last_location, "/")
        self._extensions = self._lookup('extensions')
        if 'neo4j_version' in self._metadata:
            # must be version 1.5 or greater
            self._neo4j_version = self._lookup('neo4j_version')
            self._batch_uri = self._lookup('batch')
        else:
            # assume version 1.4
            self._neo4j_version = "1.4"
            self._batch_uri = self._uri.base + "/batch"
        if 'cypher' in self._metadata:
            self._cypher_uri = self._lookup('cypher')
        else:
            try:
                self._cypher_uri = self._extension_uri('CypherPlugin', 'execute_query')
            except NotImplementedError:
                self._cypher_uri = None
        try:
            self._geoff_uri = self._extension_uri('GeoffPlugin', 'load_from_string')
        except NotImplementedError:
            self._geoff_uri = None
        try:
            self._gremlin_uri = self._extension_uri('GremlinPlugin', 'execute_script')
        except NotImplementedError:
            self._gremlin_uri = None
        self._neo4j_version = tuple(map(_numberise, self._neo4j_version.replace("-", ".").split(".")))
        self._node_indexes = {}
        self._relationship_indexes = {}
        self._indexes = {Node: {}, Relationship: {}}

    def _execute(self, plugin_name, function_name, data):
        """Execute a POST request against the specified plugin function using
        the supplied data; returns the value from the response.

        :param plugin_name: the name of the plugin to call
        :param function_name: the name of the function to call within the specified plugin
        :param data: the data to pass to the function call
        :raise NotImplementedError: when the specified plugin or function is not available
        :return: the data returned from the function call

        """
        function_uri = self._extension_uri(plugin_name, function_name)
        return self._post(function_uri, data)

    def _extension_uri(self, plugin_name, function_name):
        """Return the URI of an extension function.

        :param plugin_name: the name of the plugin
        :param function_name: the name of the function within the specified plugin
        :raise NotImplementedError: when the specified plugin or function is not available
        :return: the data returned from the function call
        """
        if plugin_name not in self._extensions:
            raise NotImplementedError(plugin_name)
        plugin = self._extensions[plugin_name]
        if function_name not in plugin:
            raise NotImplementedError(plugin_name + "." + function_name)
        return self._extensions[plugin_name][function_name]

    def _resolve(self, value):
        """Create `Node`, `Relationship` or `Path` object from dictionary
        of key:value pairs.
        """
        if isinstance(value, dict) and "self" in value:
            # is a neo4j resolvable entity
            uri = value["self"]
            if "type" in value:
                rel = Relationship(uri, graph_db=self)
                rel._metadata = rest.PropertyCache(value)
                return rel
            else:
                node = Node(uri, graph_db=self)
                node._metadata = rest.PropertyCache(value)
                return node
        elif isinstance(value, dict) and "length" in value and \
             "nodes" in value and "relationships" in value and \
             "start" in value and "end" in value:
            # is a path
            return Path(
                map(Node, value["nodes"], [self for i in value["nodes"]]),
                map(Relationship, value["relationships"], [self for i in value["relationships"]])
            )
        else:
            # is a plain value
            return value

    def clear(self):
        """Clear all nodes and relationships from the graph (except
        reference node).
        """
        ref_node = self.get_reference_node()
        cypher.execute(self, "START n=node(*) "
                             "MATCH n-[r?]-() "
                             "WHERE ID(n) <> {Z} "
                             "DELETE n, r", {
            "Z": ref_node._id
        })

    def create(self, *entities):
        """Create multiple nodes and/or relationships as part of a single
        batch, returning a list of :py:class:`Node` and
        :py:class:`Relationship` instances. For a node, simply pass a
        dictionary of properties; for a relationship, pass a tuple of
        (start, type, end) or (start, type, end, data) where start and end
        may be :py:class:`Node` instances or zero-based integral references
        to other node entities within this batch::

            # create a single node
            alice, = graph_db.create({"name": "Alice"})

            # create multiple nodes
            people = graph_db.create(
                {"name": "Alice", "age": 33}, {"name": "Bob", "age": 44},
                {"name": "Carol", "age": 55}, {"name": "Dave", "age": 66},
            )

            # create two nodes with a connecting relationship
            alice, bob, rel = graph_db.create(
                {"name": "Alice"}, {"name": "Bob"},
                (0, "KNOWS", 1, {"since": 2006})
            )

            # create a node plus a relationship to pre-existing node
            ref_node = graph_db.get_reference_node()
            alice, rel = graph_db.create(
                {"name": "Alice"}, (ref_node, "PERSON", 0)
            )

        """
        return map(batch.result, self._post(self._batch_uri, [
            batch.creator(i, entity, self)
            for i, entity in enumerate(entities)
        ]), [self for e in entities])

    def create_node(self, properties=None):
        """Create and return a new node, optionally with properties supplied as
        a dictionary.

        .. seealso:: :py:func:`create`

        """
        result = self._post(self._lookup('node'), properties)
        return Node(result["self"], graph_db=self)

    def delete(self, *entities):
        """Delete multiple nodes and/or relationships as part of a single
        batch.
        """
        self._post(self._batch_uri, [
            {
                'method': 'DELETE',
                'to': entity._uri.reference,
                'id': i
            }
            for i, entity in enumerate(entities)
        ])

    def get_index(self, type, name, config=None):
        """Fetch a specific index from the current database, returning an
        :py:class:`Index` instance. If an index with the supplied `name` and
        content `type` does not exist, :py:const:`None` is returned.

        .. seealso:: :py:func:`get_or_create_index`
        .. seealso:: :py:class:`Index`
        """
        if name not in self._indexes[type]:
            self.get_indexes(type)
        if name in self._indexes[type]:
            return self._indexes[type][name]
        else:
            return None

    def get_indexes(self, type):
        """Fetch a dictionary of all available indexes of a given type.
        """
        if type == Node:
            indexes = self._get(self._lookup('node_index')) or {}
        elif type == Relationship:
            indexes = self._get(self._lookup('relationship_index')) or {}
        else:
            raise ValueError(type)
        self._indexes[type] = dict([
            (index, Index(type, indexes[index]['template'], graph_db=self))
            for index in indexes
        ])
        return self._indexes[type]

    def get_node(self, id):
        """Fetch a node by its ID.
        """
        return Node(self._lookup('node') + "/" + str(id), graph_db=self)

    def get_node_count(self):
        """Fetch the number of nodes in this graph as an integer.
        """
        data, metadata = cypher.execute(self, "start z=node(*) return count(z)")
        if data and data[0]:
            return data[0][0]
        else:
            return 0

    def get_or_create_index(self, type, name, config=None):
        """Fetch a specific index from the current database, returning an
        :py:class:`Index` instance. If an index with the supplied `name` and
        content `type` does not exist, one is created with either the
        default configuration or that supplied in `config`::

            # get or create a node index called "People"
            people = graph_db.get_or_create_index(neo4j.Node, "People")

            # get or create a relationship index called "Friends"
            friends = graph_db.get_or_create_index(neo4j.Relationship, "Friends")

        .. seealso:: :py:func:`get_index`
        .. seealso:: :py:class:`Index`
        """
        if name not in self._indexes[type]:
            self.get_indexes(type)
        if name in self._indexes[type]:
            return self._indexes[type][name]
        else:
            if type == Node:
                uri = self._lookup('node_index')
            elif type == Relationship:
                uri = self._lookup('relationship_index')
            else:
                raise ValueError(type)
            config = config or {}
            response = self._post(uri, {"name": name, "config": config})
            index = Index(type, response["template"], graph_db=self)
            self._indexes[type].update({name: index})
            return index

    def get_or_create_relationships(self, *relationships):
        """Fetch or create relationships with the specified criteria depending
        on whether or not such relationships exist. Each relationship
        descriptor should be a tuple of (start, type, end) or (start, type,
        end, data) where start and end are either existing :py:class:`Node`
        instances or :py:const:`None` (both nodes cannot be :py:const:`None`)::

            # set up three nodes
            alice, bob, carol = graph_db.create(
                {"name": "Alice"}, {"name": "Bob"}, {"name": "Carol"}
            )

            # ensure Alice and Bob and related
            ab = graph_db.relate((alice, "LOVES", bob, {"since": 2006}))

            # ensure relationships exist between Alice, Bob and Carol
            # creating new relationships only where necessary
            rels = graph_db.relate(
                (alice, "LOVES", bob), (bob, "LIKES", alice),
                (carol, "LOVES", bob), (alice, "HATES", carol),
            )

            # ensure Alice has an outgoing LIKES relationship
            # (a new node will be created if required)
            friendship = graph_db.relate((alice, "LIKES", None))

            # ensure Alice has an incoming LIKES relationship
            # (a new node will be created if required)
            friendship = graph_db.relate((None, "LIKES", alice))

        Uses Cypher `CREATE UNIQUE` clause, raising
        :py:class:`NotImplementedError` if server support not available.
        """
        start, relate, return_, params = [], [], [], {}
        for i, rel in enumerate(relationships):
            try:
                start_node, type, end_node = rel[0:3]
            except IndexError:
                raise ValueError(rel)
            type = "`" + type + "`"
            if len(rel) > 3:
                param = "D" + str(i)
                type += " {" + param + "}"
                params[param] = rel[3]
            if start_node:
                start.append("a{0}=node({1})".format(i, start_node.id))
            if end_node:
                start.append("b{0}=node({1})".format(i, end_node.id))
            if start_node and end_node:
                relate.append("a{0}-[r{0}:{1}]->b{0}".format(i, type))
            elif start_node:
                relate.append("a{0}-[r{0}:{1}]->()".format(i, type))
            elif end_node:
                relate.append("()-[r{0}:{1}]->b{0}".format(i, type))
            else:
                raise ValueError("Neither start node nor end node specified " \
                                 "in relationship {0}: {1} ".format(i, rel))
            return_.append("r{0}".format(i))
        query = "START {0} CREATE UNIQUE {1} RETURN {2}".format(
            ",".join(start), ",".join(relate), ",".join(return_)
        )
        try:
            data, metadata = cypher.execute(self, query, params)
            return data[0]
        except cypher.CypherError:
            raise NotImplementedError(
                "The Neo4j server at <{0}> does not support " \
                "Cypher CREATE UNIQUE clauses or the query contains " \
                "an unsupported property type".format(self._uri)
            )

    def get_properties(self, *entities):
        """Fetch properties for multiple nodes and/or relationships as part
        of a single batch; returns a list of dictionaries in the same order
        as the supplied entities.
        """
        return [
            result['body']['data']
            for result in self._post(self._batch_uri, [
                {
                    'method': 'GET',
                    'to': entity._uri.reference,
                    'id': i
                }
                for i, entity in enumerate(entities)
            ])
        ]

    def get_relationship(self, id):
        """Fetch a relationship by its ID.
        """
        return Relationship(self._lookup('relationship') + "/" + str(id), graph_db=self)

    def get_relationship_count(self):
        """Fetch the number of relationships in this graph as an integer.
        """
        data, metadata = cypher.execute(self, "start z=rel(*) return count(z)")
        if data and data[0]:
            return data[0][0]
        else:
            return 0

    def get_relationship_types(self):
        """Fetch a list of relationship type names currently defined within
        this database instance.
        """
        return self._get(self._lookup('relationship_types'))

    def get_reference_node(self):
        """Fetch the reference node for the current graph.
        """
        return Node(self._lookup('reference_node'), graph_db=self)

    def get_subreference_node(self, name):
        """Fetch a named subreference node from the current graph. If such a
        node does not exist, one is created.
        """
        ref_node = self.get_reference_node()
        subreference_node = ref_node.get_single_related_node(Direction.OUTGOING, name)
        if subreference_node is None:
            subreference_node = self.create_node()
            ref_node.create_relationship_to(subreference_node, name)
        return subreference_node

    @property
    def neo4j_version(self):
        """Return the database software version as a tuple.
        """
        return self._neo4j_version

    def relate(self, *relationships):
        """Alias for get_or_create_relationships. The Cypher RELATE clause was
        replaced with CREATE UNIQUE before the final release of 1.8. Please
        see https://github.com/neo4j/community/pull/724_.
        """
        return self.get_or_create_relationships(*relationships)


class PropertyContainer(rest.Resource):
    """Base class from which :py:class:`Node` and :py:class:`Relationship`
    classes inherit. Provides property management functionality by defining
    standard Python container handler methods::

        # get the `name` property of `node`
        name = node["name"]

        # set the `name` property of `node` to `Alice`
        node["name"] = "Alice"

        # delete the `name` property from `node`
        del node["name"]

        # determine the number of properties within `node`
        count = len(node)

        # determine existence of the `name` property within `node`
        if "name" in node:
            pass

        # iterate through property keys in `node`
        for key in node:
            value = node[key]

    """

    def __init__(self, uri, reference_marker, graph_db=None, metadata=None):
        """Create container for properties with caching capabilities.

        :param uri:       URI identifying this resource
        :param metadata:  index of resource metadata
        """
        rest.Resource.__init__(self, uri, reference_marker, metadata=metadata)
        if graph_db:
            self._must_belong_to(graph_db)
            self._graph_db = graph_db
        else:
            self._graph_db = GraphDatabaseService(self._uri.base + "/")

    def __contains__(self, key):
        return key in self.get_properties()

    def __delitem__(self, key):
        try:
            self._delete(self._lookup('property').format(key=_quote(key, "")))
        except rest.ResourceNotFound:
            pass

    def __getitem__(self, key):
        try:
            return self._get(self._lookup('property').format(key=_quote(key, "")))
        except rest.ResourceNotFound:
            return None

    def __iter__(self):
        return self.get_properties().__iter__()

    def __len__(self):
        return len(self.get_properties())

    def __nonzero__(self):
        return True

    def __setitem__(self, key, value):
        if value is None:
            self.__delitem__(key)
        else:
            self._put(self._lookup('property').format(key=_quote(key, "")), value)

    def _must_belong_to(self, graph_db):
        """Raise a ValueError if this entity does not belong
        to the graph supplied.
        """
        if not isinstance(graph_db, GraphDatabaseService):
            raise TypeError(graph_db)
        if self._uri.base != graph_db._uri.base:
            raise ValueError(
                "Entity <{0}> does not belong to graph <{1}>".format(
                    self._uri, graph_db._uri
                )
            )

    def get_properties(self):
        """Return all properties for this resource.
        """
        return self._get(self._lookup('properties')) or {}

    def set_properties(self, properties=None):
        """Set all properties for this resource to the supplied dictionary of
        values.
        """
        self._put(self._lookup('properties'), properties)

    def remove_properties(self):
        """Delete all properties for this resource.
        """
        self._delete(self._lookup('properties'))


class Node(PropertyContainer):
    """A node within a graph, identified by a URI. This class is
    :py:class:`_Indexable` and, as such, may also contain URIs identifying how
    this relationship is represented within an index.
    
    :param uri:             URI identifying this node
    :param metadata:        index of resource metadata
    :param
    """

    def __init__(self, uri, graph_db=None, metadata=None, **kwargs):
        PropertyContainer.__init__(self, uri, "/node", graph_db=graph_db, metadata=metadata, **kwargs)
        self._id = int('0' + uri.rpartition('/')[-1])

    def __repr__(self):
        return "{0}('{1}')".format(
            self.__class__.__name__,
            repr(self._uri)
        )

    def __str__(self):
        """Return a human-readable string representation of this node
        object, e.g.:
        
            >>> print str(my_node)
            '(42)'
        """
        return "({0})".format(self._id)

    @property
    def id(self):
        """Return the unique id for this node.
        """
        return self._id

    def create_relationship_from(self, other_node, type, *args, **kwargs):
        """Create and return a new relationship of type `type` from the node
        represented by `other_node` to the node represented by the current
        instance.
        """
        if not isinstance(other_node, Node):
            return TypeError("Start node is not a neo4j.Node instance")
        return other_node.create_relationship_to(self, type, *args, **kwargs)

    def create_relationship_to(self, other_node, type, properties=None):
        """Create and return a new relationship of type `type` from the node
        represented by the current instance to the node represented by
        `other_node`.
        """
        if not isinstance(other_node, Node):
            return TypeError("End node is not a neo4j.Node instance")
        result = self._post(self._lookup('create_relationship'), {
            'to': str(other_node._uri),
            'type': type,
            'data': properties
        })
        return Relationship(result["self"])

    def delete(self):
        """Delete this node from the database.
        """
        self._delete(self._lookup('self'))

    def get_related_nodes(self, direction=Direction.BOTH, *types):
        """Fetch all nodes related to the current node by a relationship in a
        given `direction` of a specific `type` (if supplied).
        """
        if types:
            uri = self._lookup(_direction_prefix(direction) + '_typed_relationships').replace(
                '{-list|&|types}', '&'.join(_quote(type, "") for type in types)
            )
        else:
            uri = self._lookup(_direction_prefix(direction) + '_relationships')
        return [
            Node(rel['start'] if rel['end'] == self._uri else rel['end'], graph_db=self._graph_db)
            for rel in self._get(uri)
        ]

    def get_relationships(self, direction=Direction.BOTH, *types):
        """Fetch all relationships from the current node in a given
        `direction` of a specific `type` (if supplied).
        """
        if types:
            uri = self._lookup(_direction_prefix(direction) + '_typed_relationships').replace(
                '{-list|&|types}', '&'.join(_quote(type, "") for type in types)
            )
        else:
            uri = self._lookup(_direction_prefix(direction) + '_relationships')
        return [
            Relationship(rel['self'])
            for rel in self._get(uri)
        ]

    def get_relationships_with(self, other, direction=Direction.BOTH, *types):
        """Return all relationships between this node and another node using
        the relationship criteria supplied.
        """
        if not isinstance(other, Node):
            raise ValueError
        if direction == Direction.BOTH:
            query = "start a=node({0}),b=node({1}) match a-{2}-b return r"
        elif direction == Direction.OUTGOING:
            query = "start a=node({0}),b=node({1}) match a-{2}->b return r"
        elif direction == Direction.INCOMING:
            query = "start a=node({0}),b=node({1}) match a<-{2}-b return r"
        else:
            raise ValueError
        if types:
            type = "[r:" + "|".join("`" + type + "`" for type in types) + "]"
        else:
            type = "[r]"
        query = query.format(self.id, other.id, type)
        data, metadata = cypher.execute(self._graph_db, query)
        return [row[0] for row in data]

    def get_single_related_node(self, direction=Direction.BOTH, *types):
        """Return only one node related to the current node by a relationship
        in the given `direction` of the specified `type`, if any such
        relationships exist.
        """
        nodes = self.get_related_nodes(direction, *types)
        if nodes:
            return nodes[0]
        else:
            return None

    def get_single_relationship(self, direction=Direction.BOTH, *types):
        """Fetch only one relationship from the current node in the given
        `direction` of the specified `type`, if any such relationships exist.
        """
        relationships = self.get_relationships(direction, *types)
        if relationships:
            return relationships[0]
        else:
            return None

    def has_relationship(self, direction=Direction.BOTH, *types):
        """Return :py:const:`True` if this node has any relationships with the
        specified criteria, :py:const:`False` otherwise.
        """
        relationships = self.get_relationships(direction, *types)
        return bool(relationships)

    def has_relationship_with(self, other, direction=Direction.BOTH, *types):
        """Return :py:const:`True` if this node has any relationships with the
        specified criteria, :py:const:`False` otherwise.
        """
        relationships = self.get_relationships_with(other, direction, *types)
        return bool(relationships)

    def is_related_to(self, other, direction=Direction.BOTH, *types):
        """Return :py:const:`True` if the current node is related to the other
        node using the relationship criteria supplied, :py:const:`False`
        otherwise.
        """
        return bool(self.get_relationships_with(other, direction, *types))


class Relationship(PropertyContainer):
    """A relationship within a graph, identified by a URI. This class is
    :py:class:`_Indexable` and, as such, may also contain URIs identifying how
    this relationship is represented within an index.
    
    :param uri:             URI identifying this relationship
    :param metadata:        index of resource metadata
    """

    def __init__(self, uri, graph_db=None, metadata=None, **kwargs):
        PropertyContainer.__init__(self, uri, "/relationship", graph_db=graph_db, metadata=metadata, **kwargs)
        self._type = self._lookup('type')
        self._data = self._lookup('data')
        self._start_node = None
        self._end_node = None
        self._id = int('0' + uri.rpartition('/')[-1])

    def __repr__(self):
        return "{0}('{1}')".format(
            self.__class__.__name__,
            repr(self._uri)
        )

    def __str__(self):
        """Return a human-readable string representation of this relationship
        object, e.g.:
        
            >>> print str(my_rel)
            '-[23:KNOWS]->'
        
        """
        return "-[{0}:{1}]->".format(self._id, self._type)

    def delete(self):
        """Delete this relationship from the database.
        """
        self._delete(self._lookup('self'))

    @property
    def end_node(self):
        """Return the end node of this relationship.
        """
        if not self._end_node:
            self._end_node = Node(self._lookup('end'), graph_db=self._graph_db)
        return self._end_node

    def get_other_node(self, node):
        """Return a node object representing the node within this
        relationship which is not the one supplied.
        """
        if self._lookup('end') == node._uri:
            return self.start_node
        else:
            return self.end_node

    @property
    def id(self):
        """Return the unique id for this relationship.
        """
        return self._id

    def is_type(self, type):
        """Return :py:const:`True` if this relationship is of the given type.
        """
        return self._type == type

    @property
    def nodes(self):
        """Return a tuple of the two nodes attached to this relationship.
        """
        return self.start_node, self.end_node

    @property
    def start_node(self):
        """Return the start node of this relationship.
        """
        if not self._start_node:
            self._start_node = Node(self._lookup('start'), graph_db=self._graph_db)
        return self._start_node

    @property
    def type(self):
        """Return the type of this relationship.
        """
        return self._type


class Path(object):
    """Sequence of nodes connected by relationships.
    Note that there should always be exactly one more node supplied to
    the constructor than there are relationships.

    :raise ValueError: when number of nodes is not exactly one more than number of relationships
    """

    def __init__(self, nodes, relationships):
        if len(nodes) - len(relationships) == 1:
            self._nodes = nodes
            self._relationships = relationships
        else:
            raise ValueError

    def __str__(self):
        """Return a human-readable string representation of this path object,
        e.g.:

            >>> print str(my_path)
            '(0)-[:CUSTOMERS]->(1)-[:CUSTOMER]->(42)'

        """
        return "".join([
            str(self._nodes[i]) + str(self._relationships[i])
            for i in range(len(self._relationships))
        ]) + str(self._nodes[-1])

    def __len__(self):
        """Return the length of this path (equivalent to the number of
        relationships).
        """
        return len(self._relationships)

    @property
    def nodes(self):
        """Return a list of all the nodes which make up this path.
        """
        return self._nodes

    @property
    def relationships(self):
        """Return a list of all the relationships which make up this path.
        """
        return self._relationships

    @property
    def start_node(self):
        """Return the start node from this path.
        """
        return self._nodes[0]

    @property
    def end_node(self):
        """Return the final node from this path.
        """
        return self._nodes[-1]

    @property
    def last_relationship(self):
        """Return the final relationship from this path, or :py:const:`None`
        if path length is zero.
        """
        if self._relationships:
            return self._relationships[-1]
        else:
            return None


class Index(rest.Resource):
    """Searchable database index which can contain either nodes or
    relationships.

    .. seealso:: :py:func:`GraphDatabaseService.get_or_create_index`
    """

    def __init__(self, content_type, template_uri, graph_db=None, metadata=None, **kwargs):
        rest.Resource.__init__(
            self, template_uri.rpartition("/{key}/{value}")[0],
            "/index/", metadata=metadata, **kwargs
        )
        self._name = str(self._uri).rpartition("/")[2]
        self._content_type = content_type
        self._template_uri = template_uri
        if graph_db:
            if not isinstance(graph_db, GraphDatabaseService):
                raise TypeError(graph_db)
            if self._uri.base != graph_db._uri.base:
                raise ValueError(graph_db)
            self._graph_db = graph_db
        else:
            self._graph_db = GraphDatabaseService(self._uri.base + "/")

    def __repr__(self):
        return "{0}({1},'{2}')".format(
            self.__class__.__name__,
            repr(self._content_type.__name__),
            repr(self._uri)
        )

    def add(self, key, value, *entities):
        """Add one or more entities to this index under the `key`:`value` pair
        supplied::

            # create a couple of nodes and obtain
            # a reference to the "People" node index
            alice, bob = graph_db.create({"name": "Alice Smith"}, {"name": "Bob Smith"})
            people = graph_db.get_or_create_index(neo4j.Node, "People")

            # add the nodes to the index
            people.add("family_name", "Smith", alice, bob)

        Note that while Neo4j indexes allow multiple entities to be added under
        a particular key:value, the same entity may only be represented once;
        this method is therefore idempotent.
        """
        self._post(self._graph_db._batch_uri, [
            {
                'method': 'POST',
                'to': self._uri.reference,
                'body': {
                    "key": key,
                    "value": value,
                    "uri": str(entities[i]._uri)
                },
                'id': i
            }
            for i in range(len(entities))
        ])

    def add_if_none(self, key, value, entity):
        """Add a single entity to this index under the `key`:`value` pair
        supplied if no entry already exists at that point::

            # obtain a reference to the "Rooms" node index and
            # add node `alice` to room 100 if empty
            rooms = graph_db.get_or_create_index(neo4j.Node, "Rooms")
            rooms.add_if_none("room", 100, alice)

        ..
        """
        self._post(str(self._uri) + "?unique", {
            "key": key,
            "value": value,
            "uri": str(entity._uri)
        })

    @property
    def content_type(self):
        """Return the type of entity contained within this index. Will return
        either :py:class:`Node` or :py:class:`Relationship`.
        """
        return self._content_type

    @property
    def name(self):
        """Return the name of this index.
        """
        return self._name

    def get(self, key, value):
        """Fetch a list of all entities from the index which are associated
        with the `key`:`value` pair supplied::

            # obtain a reference to the "People" node index and
            # get all nodes where `family_name` equals "Smith"
            people = graph_db.get_or_create_index(neo4j.Node, "People")
            smiths = people.get("family_name", "Smith")

        ..
        """
        results = self._get(self._template_uri.format(
            key=_quote(key, ""),
            value=_quote(value, "")
        ))
        return [
            self._content_type(result['self'])
            for result in results
        ]

    def get_or_create(self, key, value, entity):
        """Fetch a single entity from the index which is associated with the
        `key`:`value` pair supplied, creating a new entity with the supplied
        details if none exists::

            # obtain a reference to the "Contacts" node index and
            # ensure that Alice exists therein
            contacts = graph_db.get_or_create_index(neo4j.Node, "Contacts")
            alice = contacts.get_or_create("name", "SMITH, Alice", {
                "given_name": "Alice Jane", "family_name": "Smith",
                "phone": "01234 567 890", "mobile": "07890 123 456"
            })

            # obtain a reference to the "Friendships" relationship index and
            # ensure that Alice and Bob's friendship is registered (`alice`
            # and `bob` refer to existing nodes)
            friendships = graph_db.get_or_create_index(neo4j.Relationship, "Friendships")
            alice_and_bob = friendships.get_or_create(
                "friends", "Alice & Bob", (alice, "KNOWS", bob)
            )

        ..
        """
        if self._content_type == Node:
            body = {
                "key": key,
                "value": value,
                "properties": entity
            }
        elif self._content_type == Relationship:
            body = {
                "key": key,
                "value": value,
                "start": str(entity[0]._uri),
                "type": entity[1],
                "end": str(entity[2]._uri),
                "properties": entity[3] if len(entity) > 3 else None
            }
        else:
            raise TypeError(self._content_type +
                            " indexes do not support get_or_create")
        result = self._post(str(self._uri) + "?unique", body)
        if result:
            return self._content_type(result["self"])
        else:
            return None

    def remove(self, key, value):
        """Remove any entries from the index which are associated with the
        `key`:`value` pair supplied::

            # obtain a reference to the "People" node index and
            # remove all nodes where `family_name` equals "Smith"
            people = graph_db.get_or_create_index(neo4j.Node, "People")
            people.remove("family_name", "Smith")

        ..
        """
        entities = [
            item['indexed']
            for item in self._get(self._template_uri.format(
                key=_quote(key, ""),
                value=_quote(value, "")
            ))
        ]
        self._post(self._graph_db._batch_uri, [
            {
                'method': 'DELETE',
                'to': rest.URI(entities[i], "/index/").reference,
                'id': i
            }
            for i in range(len(entities))
        ])

    def query(self, query):
        """Query the index according to the supplied query criteria, returning
        a list of matched entities::

            # obtain a reference to the "People" node index and
            # get all nodes where `family_name` equals "Smith"
            people = graph_db.get_or_create_index(neo4j.Node, "People")
            s_people = people.query("family_name:S*")

        The query syntax used should be appropriate for the configuration of
        the index being queried. For indexes with default configuration, this
        should be `Apache Lucene query syntax <http://lucene.apache.org/core/old_versioned_docs/versions/3_5_0/queryparsersyntax.html>`_.
        """
        return [
            self._content_type(item['self'])
            for item in self._get("{0}?query={1}".format(
                self._uri, _quote(query, "")
            ))
        ]
