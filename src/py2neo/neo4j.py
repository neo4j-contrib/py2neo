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

"""The neo4j module provides the main Neo4j client functionality within py2neo.
This module will be the starting point for most people.
"""

__author__    = "Nigel Small <py2neo@nigelsmall.org>"
__copyright__ = "Copyright 2011 Nigel Small"
__license__   = "Apache License, Version 2.0"


try:
    import json
except ImportError:
    import simplejson as json
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
logger = logging.getLogger(__name__)


DEFAULT_URI = "http://localhost:7474/db/data/"

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


class GraphDatabaseService(rest.Resource):
    """An instance of a `Neo4j <http://neo4j.org/>`_ database identified by its
    base URI. Generally speaking, this is the only URI which a system
    attaching to this service should need to be aware of; all further entity
    URIs will be discovered automatically from within response content
    (see `Hypermedia <http://en.wikipedia.org/wiki/Hypermedia>`_).

    :param uri:       the base URI of the database (defaults to <http://localhost:7474/db/data/>)
    :param metadata:  optional resource metadata

    The following code illustrates how to attach to a database server and
    display its version number:

        >>> from py2neo import rest, neo4j
        >>> uri = "http://localhost:7474/db/data/"
        >>> try:
        >>>     graph_db = neo4j.GraphDatabaseService(uri)
        >>>     print graph_db.neo4j_version
        >>> except rest.NoResponse:
        >>>     print "Cannot connect to host"
        >>> except rest.ResourceNotFound:
        >>>     print "Database service not found"

    """

    def __init__(self, uri=None, metadata=None, **kwargs):
        uri = uri or DEFAULT_URI
        rest.Resource.__init__(self, uri, "/", metadata=metadata, **kwargs)
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

    def create(self, *entities):
        """Create and return multiple nodes and/or relationships as part of a
        single batch. For a node, simply pass a dictionary of properties; for a
        relationship, pass a tuple of (start, type, end) or (start, type, end,
        data) where start and end may be Node instances or zero-based integral
        references to other node entities within this batch.

            >>> # create two nodes
            >>> a, b = graph_db.create({"name": "Alice"}, {"name": "Bob"})

            >>> # create two nodes with a connecting relationship
            >>> a, b, ab = graph_db.create(
            ...     {"name": "Alice"}, {"name": "Bob"},
            ...     (0, "KNOWS", 1, {"since": 2006})
            ... )

            >>> # create a node and a relationship to pre-existing node
            >>> ref_node = graph_db.get_reference_node()
            >>> a, rel = graph_db.create(
            ...     {"name": "Alice"}, (ref_node, "PERSON", 0)
            ... )

        """
        try:
            return map(batch.result, self._post(self._batch_uri, [
                batch.creator(i, entity)
                for i, entity in enumerate(entities)
            ]))
        except SystemError as err:
            raise batch.BatchError(*err.args)

    def create_node(self, properties=None):
        """Create and return a new node, optionally with properties supplied as
        a dictionary.
        """
        result = self._post(self._lookup('node'), properties)
        return Node(result["self"])

    def delete(self, *entities):
        """Delete multiple nodes and/or relationships as part of a single
        batch.
        """
        self._post(self._batch_uri, [
            {
                'method': 'DELETE',
                'to': entities[i]._uri.reference,
                'id': i
            }
            for i in range(len(entities))
        ])

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
            (index, Index(type, indexes[index]['template']))
            for index in indexes
        ])
        return self._indexes[type]

    def get_node(self, id):
        """Fetch a node by its ID.
        """
        return Node(self._lookup('node') + "/" + str(id))

    def get_node_count(self):
        """Fetch the number of nodes in this graph as an integer.
        """
        data, metadata = cypher.execute(self, "start z=node(*) return count(z)")
        if data and data[0]:
            return data[0][0]
        else:
            return 0

    def get_or_create_index(self, type, name, config=None):
        """Fetch a specific node index from the current database. If such an
        index does not exist, one is created with default configuration.
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
            index = Index(type, response["template"])
            self._indexes[type].update({name: index})
            return index

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
                    'to': entities[i]._uri.reference,
                    'id': i
                }
                for i in range(len(entities))
            ])
        ]

    def get_relationship(self, id):
        """Fetch a relationship by its ID.
        """
        return Relationship(self._lookup('relationship') + "/" + str(id))

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
        return Node(self._lookup('reference_node'))

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
        """Fetch or create relationships with the specified criteria, depending
        or whether or not such relationships exist. Uses Cypher RELATE clause,
        raising NotImplementedError if server support not available.
        """
        start, relate, return_, params = [], [], [], {}
        for i, rel in enumerate(relationships):
            try:
                start_node, type, end_node = rel[0:3]
            except IndexError:
                raise ValueError(rel)
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
        query = "START {0} RELATE {1} RETURN {2}".format(
            ",".join(start), ",".join(relate), ",".join(return_)
        )
        try:
            data, metadata = cypher.execute(self, query, params)
            return data[0]
        except cypher.CypherError:
            raise NotImplementedError(
                "The Neo4j server at <{0}> does not " \
                "support Cypher RELATE clauses".format(self._uri)
            )


class PropertyContainer(rest.Resource):
    """Base class from which node and relationship classes inherit. Extends a
    :py:class:`py2neo.rest.Resource` by providing property management
    functionality.
    """

    def __init__(self, uri, reference_marker, metadata=None, max_age=0, **kwargs):
        """Create container for properties with caching capabilities.

        :param uri:       URI identifying this resource
        :param metadata:  index of resource metadata
        :param max_age:   maximum allowed age (in seconds) of cached properties
        """
        rest.Resource.__init__(self, uri, reference_marker, metadata=metadata, **kwargs)
        if metadata and "data" in metadata:
            self._properties = rest.PropertyCache(metadata["data"], max_age=max_age)
        else:
            self._properties = rest.PropertyCache(max_age=max_age)

    def refresh(self):
        self._properties.update(self._get(self._lookup('properties')))

    def __getitem__(self, key):
        """Return a named property for this resource.
        """
        if self._properties.needs_update:
            self.refresh()
        return self._properties[key]

    def __setitem__(self, key, value):
        """Set a named property for this resource to the supplied value.
        """
        self._put(self._lookup('property').format(key=key), value)

    def __delitem__(self, key):
        """Delete a named property for this resource.
        """
        self._delete(self._lookup('property').format(key=key))

    def __contains__(self, key):
        if self._properties.needs_update:
            self.refresh()
        return key in self._properties

    def get_properties(self):
        """Return all properties for this resource.
        """
        if self._properties.needs_update:
            self.refresh()
        return self._properties.get_all()

    def set_properties(self, properties=None):
        """Set all properties for this resource to the supplied dictionary of
        values.
        """
        self._put(self._lookup('properties'), properties)
        self._properties.update(properties)

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
    :param max_age:         maximum allowed age (in seconds) of cached
                            properties
    """

    def __init__(self, uri, metadata=None, max_age=0, **kwargs):
        PropertyContainer.__init__(self, uri, "/node", metadata=metadata, \
            max_age=max_age, **kwargs)
        self._id = int('0' + uri.rpartition('/')[-1])
        self._graph_db = GraphDatabaseService(self._uri.base + "/")

    def __repr__(self):
        return '{0}({1})'.format(
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
        """
        Fetch all nodes related to the current node by a relationship in a
        given `direction` of a specific `type` (if supplied).
        """
        if types:
            uri = self._lookup(direction + '_typed_relationships').replace('{-list|&|types}', '&'.join(types))
        else:
            uri = self._lookup(direction + '_relationships')
        return [
            Node(rel['start'] if rel['end'] == self._uri else rel['end'])
            for rel in self._get(uri)
        ]

    def get_relationships(self, direction=Direction.BOTH, *types):
        """Fetch all relationships from the current node in a given
        `direction` of a specific `type` (if supplied).
        """
        if types:
            uri = self._lookup(direction + '_typed_relationships').replace('{-list|&|types}', '&'.join(types))
        else:
            uri = self._lookup(direction + '_relationships')
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
            type = "[r:" + "|".join(types) + "]"
        else:
            type = "[r]"
        query = query.format(self.id, other.id, type)
        rows, columns = cypher.execute(self._graph_db, query)
        return [row[0] for row in rows]

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
    :param max_age:         maximum allowed age (in seconds) of cached
                            properties
    """

    def __init__(self, uri, metadata=None, max_age=0, **kwargs):
        PropertyContainer.__init__(self, uri, "/relationship", \
            metadata=metadata, max_age=max_age, **kwargs)
        self._type = self._lookup('type')
        self._data = self._lookup('data')
        self._start_node = None
        self._end_node = None
        self._id = int('0' + uri.rpartition('/')[-1])

    def __repr__(self):
        return '{0}({1})'.format(
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
            self._end_node = Node(self._lookup('end'))
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
            self._start_node = Node(self._lookup('start'))
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
    """

    def __init__(self, entity_type, template_uri, metadata=None, **kwargs):
        rest.Resource.__init__(
            self, template_uri.rpartition("/{key}/{value}")[0],
            "/index/", metadata=metadata, **kwargs
        )
        self._name = str(self._uri).rpartition("/")[2]
        self._content_type = entity_type
        self._template_uri = template_uri
        self._graph_database_service = GraphDatabaseService(self._uri.base + "/")

    def __repr__(self):
        return '{0}({1},{2})'.format(
            self.__class__.__name__,
            repr(self._content_type.__name__),
            repr(self._uri)
        )

    @property
    def name(self):
        """Return the name of this index.
        """
        return self._name

    @property
    def content_type(self):
        """Return the type of entities contained within this index.
        """
        return self._content_type

    def add(self, key, value, *entities):
        """Add one or more entities to the index under the key:value pair
        supplied.
        """
        self._post(self._graph_database_service._batch_uri, [
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
        """Add an entity to the index under the key:value pair supplied if
        and only if no entry already exists at that point.
        """
        self._post(str(self._uri) + "?unique", {
            "key": key,
            "value": value,
            "uri": str(entity._uri)
        })

    def remove(self, key, value):
        """Remove any entries from the index which are associated with the
        key:value pair supplied.
        """
        entities = [
            item['indexed']
            for item in self._get(self._template_uri.format(
                key=_quote(key, ""),
                value=_quote(value, "")
            ))
        ]
        self._post(self._graph_database_service._batch_uri, [
            {
                'method': 'DELETE',
                'to': rest.URI(entities[i], "/index/").reference,
                'id': i
            }
            for i in range(len(entities))
        ])

    def get(self, key, value):
        """Fetch all entities from the index which are associated with the
        key:value pair supplied.
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
        """Fetch single entity from the index which is associated with the
        key:value pair supplied, creating a new entity with the supplied
        details if none exists.

            >>> people = self.graph_db.get_node_index("People")
            >>> alice = people.get_or_create("surname", "Smith",
            ...     {"name": "Alice Smith"})

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

    def query(self, query):
        """Query the index according to the supplied query criteria, returning
        a list of matched entities.
        """
        return [
            self._content_type(item['self'])
            for item in self._get("{0}?query={1}".format(
                self._uri, _quote(query, "")
            ))
        ]
