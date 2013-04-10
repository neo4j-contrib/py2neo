#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright 2011-2013, Nigel Small
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

""" The neo4j module provides the main `Neo4j <http://neo4j.org/>`_ client
functionality and will be the starting point for most applications. The main
classes provided are:

- :py:class:`GraphDatabaseService` - an instance of a Neo4j database server,
  providing a number of graph-global methods for handling nodes and
  relationships
- :py:class:`Node` - a representation of a database node
- :py:class:`Relationship` - a representation of a relationship between two
  database nodes
- :py:class:`Path` - a sequence of alternating nodes and relationships
- :py:class:`Index` - a index of key-value pairs for storing links to nodes or
  relationships
- :py:class:`ReadBatch` - a batch of read requests to be carried out within a
  single transaction
- :py:class:`WriteBatch` - a batch of write requests to be carried out within
  a single transaction
"""

from __future__ import absolute_import, division, print_function, unicode_literals

import base64
import json
import logging
import re

from . import rest, cypher
from .util import compact, quote, round_robin, deprecated, version_tuple


DEFAULT_URI = "http://localhost:7474/db/data/"
SIMPLE_NAME = re.compile(r"[A-Za-z_][0-9A-Za-z_]*")

logger = logging.getLogger(__name__)


def authenticate(netloc, user_name, password):
    """ Set HTTP basic authentication values for specified `netloc`. The code
    below shows a simple example::

        # set up authentication parameters
        neo4j.authenticate("camelot:7474", "arthur", "excalibur")

        # connect to authenticated graph database
        graph_db = neo4j.GraphDatabaseService("http://camelot:7474/db/data/")

    Note: a `netloc` can be either a server name or a server name and port
    number but must match exactly that used within the GraphDatabaseService
    URI.

    :param netloc: the host and port requiring authentication (e.g. "camelot:7474")
    :param user_name: the user name to authenticate as
    :param password: the password
    """
    credentials = (user_name + ":" + password).encode("UTF-8")
    value = "Basic " + base64.b64encode(credentials).decode("ASCII")
    rest.http_headers.add("Authorization", value, netloc=netloc)


def rewrite(from_scheme_netloc, to_scheme_netloc):
    """ Automatically rewrite all URIs directed to the scheme and netloc
    specified in `from_scheme_netloc` to that specified in `to_scheme_netloc`.
    This applies *before* any netloc-specific headers or timeouts are applied.

    As an example::

        # implicitly convert all URIs beginning with <http://localhost:7474>
        # to instead use <https://dbserver:9999>
        neo4j.rewrite(("http", "localhost:7474"), ("https", "dbserver:9999"))

    if `to_scheme_netloc` is :py:const:`None` then any rewrite rule for
    `from_scheme_netloc` is removed.

    This facility is primarily intended for use by database servers behind
    proxies which are unaware of their externally visible network address.
    """
    if to_scheme_netloc is None:
        try:
            del rest.http_rewrites[from_scheme_netloc]
        except KeyError:
            pass
    else:
        rest.http_rewrites[from_scheme_netloc] = to_scheme_netloc


def set_timeout(netloc, timeout):
    """ Set a timeout for all HTTP blocking operations for specified `netloc`.

    :param netloc: the host and port to set the timeout value for (e.g. "camelot:7474")
    :param timeout: the timeout value in seconds
    """
    rest.http_timeouts[netloc] = timeout


def _assert_expected_response(cls, uri, metadata):
    """ Checks the metadata received against a specific class to confirm this
    is the type of response expected.
    """
    has_all = lambda iterable, items: all(item in iterable for item in items)
    if cls is GraphDatabaseService:
        if has_all(metadata, ("extensions", "node", "node_index",
                              "relationship_index", "relationship_types")):
           return
    elif cls is Node:
        if has_all(metadata, ("self", "property", "properties", "data",
                              "create_relationship", "incoming_relationships",
                              "outgoing_relationships", "all_relationships")):
            return
    elif cls is Relationship:
        if has_all(metadata, ("self", "property", "properties", "data",
                              "start", "type", "end")):
            return
    else:
        raise TypeError("Cannot confirm metadata for class " + cls.__name__)
    raise ValueError(
        "URI <{0}> does not appear to identify a {1}: {2}".format(
            uri, cls.__name__, json.dumps(metadata, separators=(",", ":"))
        )
    )


def _node(*args, **kwargs):
    """ Cast the arguments provided to a :py:class:`neo4j.Node`. The following
    general combinations are possible:

    - ``node(node_instance)``
    - ``node(property_dict)``
    - ``node(**properties)``
    - ``node(*labels, **properties)``

    If :py:const:`None` is passed as the only argument, :py:const:`None` is
    returned instead of a ``Node`` instance.

    Examples::

        node(Node("http://localhost:7474/db/data/node/1"))
        node()
        node(None)
        node(name="Alice")
        node({"name": "Alice"})
        node("Person")
        node("Person", name="Alice")

    """
    if len(args) == 1 and not kwargs:
        arg = args[0]
        if arg is None:
            return None
        elif isinstance(arg, Node):
            return arg
        elif isinstance(arg, dict):
            return Node.abstract(**arg)
        else:
            return Node.abstract(arg)
    else:
        return Node.abstract(*args, **kwargs)


def _rel(*args, **kwargs):
    """ Cast the arguments provided to a :py:class:`neo4j.Relationship`. The
    following general combinations are possible:

    - ``rel(relationship_instance)``
    - ``rel((start_node, type, end_node))``
    - ``rel((start_node, type, end_node, properties))``
    - ``rel((start_node, type, end_node, labels, properties))``
    - ``rel((start_node, (type, labels), end_node))``
    - ``rel((start_node, (type, properties), end_node))``
    - ``rel((start_node, (type, labels, properties), end_node))``
    - ``rel(start_node, type, end_node, **properties)``
    - ``rel(start_node, type, end_node, *labels, **properties)``

    Examples::

        rel(Relationship("http://localhost:7474/db/data/relationship/1"))
        rel((alice, "KNOWS", bob))
        rel((alice, "KNOWS", bob, {"since": 1999}))
        rel((alice, "KNOWS", bob, "Friendship", {"since": 1999}))
        rel((alice, ("KNOWS", {"since": 1999}), bob))
        rel((alice, ("KNOWS", ["Friendship"], {"since": 1999}), bob))
        rel(alice, "KNOWS", bob, since=1999)
        rel(alice, "KNOWS", bob, "Friendship", since=1999)

    """
    if len(args) == 1 and not kwargs:
        arg = args[0]
        if isinstance(arg, Relationship):
            return arg
        elif isinstance(arg, tuple):
            if len(arg) == 3:
                return _UnboundRelationship.cast(arg[1]).bind(arg[0], arg[2])
            elif len(arg) == 4:
                return Relationship.abstract(arg[0], arg[1], arg[2], **arg[3])
            elif len(arg) == 5:
                return Relationship.abstract(arg[0], arg[1], arg[2], *arg[3], **arg[4])
            else:
                raise TypeError(arg)
        else:
            raise TypeError(arg)
    elif len(args) >= 3:
        return Relationship.abstract(*args, **kwargs)
    else:
        raise TypeError((args, kwargs))


class Direction(object):
    """ Defines the direction of a relationship. This class is deprecated as
    all dependent functions are also deprecated.

    .. deprecated:: 1.5
    """

    BOTH     =  0
    EITHER   =  0
    INCOMING = -1
    OUTGOING =  1


class GraphDatabaseService(rest.Resource):
    """ An instance of a `Neo4j <http://neo4j.org/>`_ database identified by
    its base URI. Generally speaking, this is the only URI which a system
    attaching to this service should need to be directly aware of; all further
    entity URIs will be discovered automatically from within response content
    when possible (see `Hypermedia <http://en.wikipedia.org/wiki/Hypermedia>`_)
    or will be derived from existing URIs.

    The following code illustrates how to connect to a database server and
    display its version number::

        from py2neo import neo4j
        
        graph_db = neo4j.GraphDatabaseService(neo4j.DEFAULT_URI)
        print(graph_db.neo4j_version)

    :param uri: the base URI of the database (defaults to the value of
        :py:data:`DEFAULT_URI`)
    """

    _instances = {}

    @classmethod
    def get_instance(cls, uri):
        """ Fetch a cached instance of a :py:class:`GraphDatabaseService` for
        a given URI. This method can be used to reduce both the number of
        instances in existence at any one time and the number of network
        messages sent for discovery.
        """
        if uri not in cls._instances:
            cls._instances[uri] = cls(uri)
        return cls._instances[uri]

    def __init__(self, uri=None):
        uri = uri or DEFAULT_URI
        rest.Resource.__init__(self, uri)
        rs = self._send(rest.Request(self, "GET", self.__uri__))
        _assert_expected_response(self.__class__, self.__uri__, rs.body)
        self._update_metadata(rs.body)
        # force URI adjustment (in case supplied without trailing slash)
        self.__uri__ = rest.URI(rs.uri)
        self._extensions = self.__metadata__.get('extensions', None)
        self._neo4j_version = self.__metadata__.get('neo4j_version', "1.4")
        self._batch_uri = self.__metadata__.get('batch', self.__uri__.base + "/batch")
        self._cypher_uri = self.__metadata__.get('cypher', None)
        self._indexes = {Node: {}, Relationship: {}}

    def __nonzero__(self):
        """ Return :py:const:`True` is this graph contains at least one
        relationship.
        """
        data, metadata = cypher.execute(self, "START r=rel(*) RETURN r LIMIT 1")
        if data and data[0]:
            return True
        else:
            return False

    def __len__(self):
        """ Return the size of this graph (i.e. the number of relationships).
        """
        return self.size()

    def _extension_uri(self, plugin_name, function_name):
        """ Return the URI of an extension function.

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

    def _resolve(self, data, status=200, id_=None):
        """ Create `Node`, `Relationship` or `Path` object from dictionary of
        key:value pairs.
        """
        if data is None:
            return None
        elif status == 400:
            raise rest.BadRequest(data["message"], id_=id_)
        elif status == 404:
            raise rest.ResourceNotFound(data["message"], id_=id_)
        elif status == 409:
            raise rest.ResourceConflict(data["message"], id_=id_)
        elif status // 100 == 5:
            raise SystemError(data["message"])
        elif isinstance(data, dict) and "self" in data:
            # is a neo4j resolvable entity
            uri = data["self"]
            if "type" in data:
                rel = Relationship(uri)
                rel._update_metadata(data)
                rel._properties = data["data"]
                return rel
            else:
                node = Node(uri)
                node._update_metadata(data)
                node._properties = data["data"]
                return node
        elif isinstance(data, dict) and "length" in data and \
                "nodes" in data and "relationships" in data and \
                "start" in data and "end" in data:
            # is a path
            nodes = map(Node, data["nodes"])
            rels = map(Relationship, data["relationships"])
            return Path(*round_robin(nodes, rels))
        elif isinstance(data, dict) and "columns" in data and "data" in data:
            # is a value contained within a Cypher response
            # (should only ever be single row, single value)
            if len(data["columns"]) != 1:
                raise ValueError("Expected single column")
            rows = data["data"]
            if len(rows) != 1:
                raise ValueError("Expected single row")
            values = rows[0]
            if len(values) != 1:
                raise ValueError("Expected single value")
            value = values[0]
            return self._resolve(value, status, id_=id_)
        elif isinstance(data, list):
            return [self._resolve(item, status, id_) for item in data]
        else:
            # is a plain value
            return data

    def clear(self):
        """ Clear all nodes and relationships from the graph.

        .. warning::
            This method will permanently remove **all** nodes and relationships
            from the graph and cannot be undone.
        """
        batch = WriteBatch(self)
        batch._post(self._cypher_uri, {"query": "START r=rel(*) DELETE r"})
        batch._post(self._cypher_uri, {"query": "START n=node(*) DELETE n"})
        batch._submit()

    def create(self, *abstracts):
        """ Create multiple nodes and/or relationships as part of a single
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
        if not abstracts:
            return []
        batch = WriteBatch(self)
        for abstract in abstracts:
            batch.create(abstract)
        return batch.submit()

    def delete(self, *entities):
        """ Delete multiple nodes and/or relationships as part of a single
        batch.
        """
        if not entities:
            return
        batch = WriteBatch(self)
        for entity in entities:
            if entity is None:
                continue
            elif isinstance(entity, Node):
                batch.delete_node(entity)
            elif isinstance(entity, Relationship):
                batch.delete_relationship(entity)
            else:
                raise TypeError(entity)
        batch._submit()

    @deprecated("GraphDatabaseService.get_reference_node is deprecated, "
                "please use indexed nodes instead.")
    def get_reference_node(self):
        """ Fetch the reference node for the current graph.

        .. deprecated:: 1.3.1
            use indexed nodes instead.
        """
        return Node(self.__metadata__['reference_node'])

    @deprecated("GraphDatabaseService.get_or_create_relationships is "
                "deprecated, please use either WriteBatch."
                "get_or_create_relationship or Path.get_or_create instead.")
    def get_or_create_relationships(self, *abstracts):
        """ Fetch or create relationships with the specified criteria depending
        on whether or not such relationships exist. Each relationship
        descriptor should be a tuple of (start, type, end) or (start, type,
        end, data) where start and end are either existing :py:class:`Node`
        instances or :py:const:`None` (both nodes cannot be :py:const:`None`).

        Uses Cypher `CREATE UNIQUE` clause, raising
        :py:class:`NotImplementedError` if server support not available.

        .. deprecated:: 1.5
            use either :py:func:`WriteBatch.get_or_create_relationship` or
            :py:func:`Path.get_or_create` instead.
        """
        batch = WriteBatch(self)
        for abstract in abstracts:
            if 3 <= len(abstract) <= 4:
                batch.get_or_create_relationship(*abstract)
            else:
                raise TypeError(abstract)
        try:
            return batch.submit()
        except cypher.CypherError:
            raise NotImplementedError(
                "The Neo4j server at <{0}> does not support " \
                "Cypher CREATE UNIQUE clauses or the query contains " \
                "an unsupported property type".format(self.__uri__)
            )

    def get_properties(self, *entities):
        """ Fetch properties for multiple nodes and/or relationships as part
        of a single batch; returns a list of dictionaries in the same order
        as the supplied entities.
        """
        if not entities:
            return []
        if len(entities) == 1:
            return [entities[0].get_properties()]
        batch = ReadBatch(self)
        for entity in entities:
            batch.get_properties(entity)
        return [rs.body or {} for rs in batch._submit()]

    def get_relationship_types(self):
        """ Fetch a list of relationship type names currently defined within
        this database instance.
        """
        return self._send(
            rest.Request(self,"GET", self.__metadata__['relationship_types'])
        ).body

    def match(self, start_node=None, rel_type=None, end_node=None,
              bidirectional=False, limit=None):
        """ Fetch all relationships which match a specific set of criteria. The
        arguments provided are all optional and are used to filter the
        relationships returned. Examples are as follows::

            # all relationships from the graph database
            # ()-[r]-()
            rels = graph_db.match()

            # all relationships outgoing from `alice`
            # (alice)-[r]->()
            rels = graph_db.match(start_node=alice)

            # all relationships incoming to `alice`
            # ()-[r]->(alice)
            rels = graph_db.match(end_node=alice)

            # all relationships attached to `alice`, regardless of direction
            # (alice)-[r]-()
            rels = graph_db.match(start_node=alice, bidirectional=True)

            # all relationships from `alice` to `bob`
            # (alice)-[r]->(bob)
            rels = graph_db.match(start_node=alice, end_node=bob)

            # all relationships outgoing from `alice` of type "FRIEND"
            # (alice)-[r:FRIEND]->()
            rels = graph_db.match(start_node=alice, rel_type="FRIEND")

            # up to three relationships outgoing from `alice` of type "FRIEND"
            # (alice)-[r:FRIEND]->()
            rels = graph_db.match(start_node=alice, rel_type="FRIEND", limit=3)

        :param start_node: concrete start :py:class:`Node` to match or
            :py:const:`None` if any
        :param rel_type: type of relationships to match or :py:const:`None` if
            any
        :param end_node: concrete end :py:class:`Node` to match or
            :py:const:`None` if any
        :param bidirectional: :py:const:`True` if reversed relationships should
            also be included
        :param limit: maximum number of relationships to match or
            :py:const:`None` if no limit
        """
        if start_node is None and end_node is None:
            query = "START a=node(*)"
            params = {}
        elif end_node is None:
            query = "START a=node({A})"
            start_node = _cast(start_node, Node, abstract=False)
            params = {"A": start_node._id}
        elif start_node is None:
            query = "START b=node({B})"
            end_node = _cast(end_node, Node, abstract=False)
            params = {"B": end_node._id}
        else:
            query = "START a=node({A}),b=node({B})"
            start_node = _cast(start_node, Node, abstract=False)
            end_node = _cast(end_node, Node, abstract=False)
            params = {"A": start_node._id, "B": end_node._id}
        if rel_type is None:
            if bidirectional:
                query += " MATCH (a)-[r]-(b) RETURN r"
            else:
                query += " MATCH (a)-[r]->(b) RETURN r"
        else:
            if bidirectional:
                query += " MATCH (a)-[r:`" + str(rel_type) + "`]-(b) RETURN r"
            else:
                query += " MATCH (a)-[r:`" + str(rel_type) + "`]->(b) RETURN r"
        if limit is not None:
            query += " LIMIT {0}".format(int(limit))
        data, metadata = cypher.execute(self, query, params)
        return [row[0] for row in data]

    def match_one(self, start_node=None, rel_type=None, end_node=None,
                  bidirectional=False):
        """ Fetch a single relationship which matches a specific set of
        criteria.

        :param start_node: concrete start :py:class:`Node` to match or
            :py:const:`None` if any
        :param rel_type: type of relationships to match or :py:const:`None` if
            any
        :param end_node: concrete end :py:class:`Node` to match or
            :py:const:`None` if any
        :param bidirectional: :py:const:`True` if reversed relationships should
            also be included

        .. seealso::
           :py:func:`GraphDatabaseService.match <py2neo.neo4j.GraphDatabaseService.match>`
        """
        rels = self.match(start_node, rel_type, end_node, bidirectional, 1)
        if rels:
            return rels[0]
        else:
            return None

    @property
    def neo4j_version(self):
        """ Return the database software version as a 4-tuple of (``int``,
        ``int``, ``int``, ``str``).
        """
        return version_tuple(self._neo4j_version)

    def node(self, id):
        """ Fetch a node by ID.
        """
        return Node(self.__metadata__['node'] + "/" + str(id))

    def order(self):
        """ Fetch the number of nodes in this graph.
        """
        data, metadata = cypher.execute(self, "START n=node(*) RETURN count(n)")
        if data and data[0]:
            return data[0][0]
        else:
            raise EnvironmentError("Unable to count nodes")

    def relationship(self, id):
        """ Fetch a relationship by ID.
        """
        uri = "{0}relationship/{1}".format(self.__uri__.base, id)
        return Relationship(uri)

    def size(self):
        """ Fetch the number of relationships in this graph.
        """
        data, metadata = cypher.execute(self, "START r=rel(*) RETURN count(r)")
        if data and data[0]:
            return data[0][0]
        else:
            raise EnvironmentError("Unable to count relationships")

    def get_indexes(self, content_type):
        """ Fetch a dictionary of all available indexes of a given type.

        :param content_type: either :py:class:`neo4j.Node` or
            :py:class:`neo4j.Relationship`
        :return: a list of :py:class:`Index` instances of the specified type
        """
        if content_type == Node:
            rq = rest.Request(self, "GET", self.__metadata__['node_index'])
        elif content_type == Relationship:
            rq = rest.Request(self, "GET", self.__metadata__['relationship_index'])
        else:
            raise ValueError(content_type)
        rs = self._send(rq)
        indexes = rs.body or {}
        self._indexes[content_type] = dict(
            (index, Index(content_type, indexes[index]['template']))
            for index in indexes
        )
        return self._indexes[content_type]

    def get_index(self, content_type, index_name):
        """ Fetch a specific index from the current database, returning an
        :py:class:`Index` instance. If an index with the supplied `name` and
        content `type` does not exist, :py:const:`None` is returned.

        :param content_type: either :py:class:`neo4j.Node` or
            :py:class:`neo4j.Relationship`
        :param index_name: the name of the required index
        :return: an :py:class:`Index` instance or :py:const:`None`

        .. seealso:: :py:func:`get_or_create_index`
        .. seealso:: :py:class:`Index`
        """
        if index_name not in self._indexes[content_type]:
            self.get_indexes(content_type)
        if index_name in self._indexes[content_type]:
            return self._indexes[content_type][index_name]
        else:
            return None

    def get_or_create_index(self, content_type, index_name, config=None):
        """ Fetch a specific index from the current database, returning an
        :py:class:`Index` instance. If an index with the supplied `name` and
        content `type` does not exist, one is created with either the
        default configuration or that supplied in `config`::

            # get or create a node index called "People"
            people = graph_db.get_or_create_index(neo4j.Node, "People")

            # get or create a relationship index called "Friends"
            friends = graph_db.get_or_create_index(neo4j.Relationship, "Friends")

        :param content_type: either :py:class:`neo4j.Node` or
            :py:class:`neo4j.Relationship`
        :param index_name: the name of the required index
        :return: an :py:class:`Index` instance

        .. seealso:: :py:func:`get_index`
        .. seealso:: :py:class:`Index`
        """
        if index_name not in self._indexes[content_type]:
            self.get_indexes(content_type)
        if index_name in self._indexes[content_type]:
            return self._indexes[content_type][index_name]
        if content_type == Node:
            uri = self.__metadata__['node_index']
        elif content_type == Relationship:
            uri = self.__metadata__['relationship_index']
        else:
            raise ValueError(content_type)
        config = config or {}
        rs = self._send(rest.Request(self, "POST", uri, {"name": index_name, "config": config}))
        index = Index(content_type, rs.body["template"])
        self._indexes[content_type].update({index_name: index})
        return index

    def delete_index(self, content_type, index_name):
        """ Delete the entire index identified by the type and name supplied.

        :param content_type: either :py:class:`neo4j.Node` or
            :py:class:`neo4j.Relationship`
        :param index_name: the name of the required index
        :return: :py:const:`True` if the index was deleted, :py:const:`False`
            otherwise
        """
        if index_name not in self._indexes[content_type]:
            self.get_indexes(content_type)
        if index_name in self._indexes[content_type]:
            index = self._indexes[content_type][index_name]
            self._send(rest.Request(self, "DELETE", index.__uri__))
            del self._indexes[content_type][index_name]
            return True
        else:
            return False

    def get_indexed_node(self, index_name, key, value):
        """ Fetch the first node indexed with the specified details, returning
        :py:const:`None` if none found.

        :param index_name: the name of the required index
        :param key: the index key
        :param value: the index value
        :return: a :py:class:`Node` instance
        """
        index = self.get_index(Node, index_name)
        if index:
            nodes = index.get(key, value)
            if nodes:
                return nodes[0]
        return None

    def get_or_create_indexed_node(self, index_name, key, value, properties=None):
        """ Fetch the first node indexed with the specified details, creating
        and returning a new indexed node if none found.

        :param index_name: the name of the required index
        :param key: the index key
        :param value: the index value
        :param properties: properties for the new node, if one is created
            (optional)
        :return: a :py:class:`Node` instance
        """
        index = self.get_or_create_index(Node, index_name)
        return index.get_or_create(key, value, properties or {})

    def get_indexed_relationship(self, index_name, key, value):
        """ Fetch the first relationship indexed with the specified details,
        returning :py:const:`None` if none found.

        :param index_name: the name of the required index
        :param key: the index key
        :param value: the index value
        :return: a :py:class:`Relationship` instance
        """
        index = self.get_index(Relationship, index_name)
        if index:
            relationships = index.get(key, value)
            if relationships:
                return relationships[0]
        return None


class _Entity(rest.Resource):
    """ Base class from which :py:class:`Node` and :py:class:`Relationship`
    classes inherit. Provides property management functionality by defining
    standard Python container handler methods.
    """

    def __init__(self, uri):
        rest.Resource.__init__(self, uri)
        self._labels = set()
        self._properties = {}

    def __contains__(self, key):
        return key in self.get_properties()

    def __delitem__(self, key):
        self.update_properties({key: None})

    def __getitem__(self, key):
        return self.get_properties().get(key, None)

    def __hash__(self):
        if self.__uri__:
            return hash(self.__uri__)
        else:
            return hash((self._labels, self._properties))

    def __iter__(self):
        return self.get_properties().__iter__()

    def __len__(self):
        return len(self.get_properties())

    def __nonzero__(self):
        return True

    def __setitem__(self, key, value):
        self.update_properties({key: value})

    @property
    def _graph_db(self):
        try:
            return GraphDatabaseService.get_instance(self.__uri__.base)
        except AttributeError:
            return None

    def _must_belong_to(self, graph_db):
        """ Raise a :py:error:`ValueError` if this entity does not belong to
        the graph supplied.
        """
        if not isinstance(graph_db, GraphDatabaseService):
            raise TypeError(graph_db)
        if self.__uri__.base != graph_db.__uri__.base:
            raise ValueError(
                "Entity <{0}> does not belong to graph <{1}>".format(
                    self.__uri__, graph_db.__uri__
                )
            )

    def delete(self):
        """ Delete this entity from the database.
        """
        self._send(rest.Request(self._graph_db, "DELETE", self.__metadata__['self']))

    def exists(self):
        """ Determine whether this entity still exists in the database.
        """
        try:
            self._send(rest.Request(self._graph_db, "GET", self.__metadata__['self']))
            return True
        except rest.ResourceNotFound:
            return False

    def get_properties(self):
        """ Fetch all properties.

        :return: dictionary of properties
        """
        if self.__uri__:
            uri = self.__metadata__['properties']
            rs = self._send(rest.Request(self._graph_db, "GET", uri))
            self._properties = rs.body or {}
        return self._properties

    def is_abstract(self):
        """ Return :py:const:`True` if this entity is abstract (i.e. not bound
        to a concrete entity within the database), :py:const:`False` otherwise.

        :return: :py:const:`True` if this entity is abstract
        """
        return self.__uri__ is None

    def set_properties(self, properties):
        """ Replace all properties with those supplied.

        :param properties: dictionary of new properties
        """
        self._properties = properties
        if self.__uri__:
            uri = self.__metadata__['properties']
            if self._properties:
                self._send(rest.Request(self._graph_db, "PUT", uri, compact(self._properties)))
            else:
                self._send(rest.Request(self._graph_db, "DELETE", uri))

    def delete_properties(self):
        """ Delete all properties.
        """
        self.set_properties({})

    def update_properties(self, properties):
        raise NotImplementedError("Entity.update_properties")


class Node(_Entity):
    """ A node within a graph, identified by a URI. For example:

        >>> alice = neo4j.Node("http://localhost:7474/db/data/node/1")

    Typically, concrete nodes will not be constructed directly in this way
    by client applications. Instead, methods such as
    :py:func:`GraphDatabaseService.create` build node objects indirectly as
    required. Once created, however, nodes can be treated like any other
    container types in order to manage properties::

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

    :param uri: URI identifying this node
    """

    @classmethod
    def abstract(cls, *labels, **properties):
        """ Create and return a new abstract node containing properties drawn
        from the keyword arguments supplied. An abstract node is not bound to
        a concrete node within a database but properties can be managed
        similarly to those within bound nodes::

            >>> alice = neo4j.Node.abstract(name="Alice")
            >>> alice["name"]
            'Alice'
            >>> alice["age"] = 34
            alice.get_properties()
            {'age': 34, 'name': 'Alice'}

        If more complex property keys are required, abstract nodes may be
        instantiated with the ``**`` syntax::

            >>> alice = neo4j.Node.abstract(**{"first name": "Alice"})
            >>> alice["first name"]
            'Alice'

        :param properties: node properties
        """
        instance = cls(None)
        instance._labels = set(labels)
        instance._properties = dict(properties)
        return instance

    def __init__(self, uri):
        _Entity.__init__(self, uri)

    def __eq__(self, other):
        other = _cast(other, Node)
        if self.__uri__:
            return _Entity.__eq__(self, other)
        else:
            return self._labels == other._labels and \
                   self._properties == other._properties

    def __ne__(self, other):
        other = _cast(other, Node)
        if self.__uri__:
            return _Entity.__ne__(self, other)
        else:
            return self._labels != other._labels or \
                   self._properties != other._properties

    def __repr__(self):
        if self.__uri__:
            return "{0}({1})".format(
                self.__class__.__name__,
                repr(str(self.__uri__))
            )
        elif self._properties:
            return "node(**{1})".format(
                self.__class__.__name__,
                repr(self._properties)
            )
        else:
            return "node()".format(
                self.__class__.__name__
            )

    def __str__(self):
        """ Return Cypher/Geoff style representation of this node.
        """
        if self.is_abstract():
            return "({0})".format(json.dumps(self._properties, separators=(",", ":")))
        elif self._properties:
            return "({0} {1})".format(
                "" if self._id is None else self._id,
                json.dumps(self._properties, separators=(",", ":")),
            )
        else:
            return "({0})".format("" if self._id is None else self._id)

    @property
    def _id(self):
        """ Return the internal ID for this node.

        :return: integer ID of this node within the database or
            :py:const:`None` if abstract
        """
        if self.__uri__ is None:
            return None
        else:
            return int('0' + str(self.__uri__).rpartition('/')[-1])

    @property
    def id(self):
        return self._id

    @deprecated("Node.create_relationship_from is deprecated, please use "
                "Node.create_path instead.")
    def create_relationship_from(self, other_node, type, properties=None):
        """ Create and return a new relationship of type `type` from the node
        represented by `other_node` to the node represented by the current
        instance.

        .. deprecated:: 1.5
            use :py:func:`Node.create_path` instead.
        """
        if not isinstance(other_node, Node):
            return TypeError("Start node is not a neo4j.Node instance")
        return other_node.create_relationship_to(self, type, properties)

    @deprecated("Node.create_relationship_to is deprecated, please use "
                "Node.create_path instead.")
    def create_relationship_to(self, other_node, type, properties=None):
        """ Create and return a new relationship of type `type` from the node
        represented by the current instance to the node represented by
        `other_node`.

        .. deprecated:: 1.5
            use :py:func:`Node.create_path` instead.
        """
        if not isinstance(other_node, Node):
            return TypeError("End node is not a neo4j.Node instance")
        rs = self._send(rest.Request(self._graph_db, "POST", self.__metadata__['create_relationship'], {
            'to': str(other_node.__uri__),
            'type': type,
            'data': compact(properties or {})
        }))
        return Relationship(rs.body["self"])

    def delete_related(self):
        """ Delete this node, plus all related nodes and relationships.
        """
        query = (
            "START a=node({a}) "
            "MATCH (a)-[rels*0..]-(z) "
            "FOREACH(rel IN rels: DELETE rel) "
            "DELETE a, z"
        )
        cypher.execute(self._graph_db, query, {"a": self._id})

    # only used by deprecated methods below
    def _relationships_uri(self, direction):
        if not isinstance(direction, int):
            raise ValueError("Relationship direction must be an integer value")
        if direction > 0:
            uri = self.__metadata__['outgoing_relationships']
        elif direction < 0:
            uri = self.__metadata__['incoming_relationships']
        else:
            uri = self.__metadata__['all_relationships']
        return uri

    # only used by deprecated methods below
    def _typed_relationships_uri(self, direction, types):
        if not isinstance(direction, int):
            raise ValueError("Relationship direction must be an integer value")
        if direction > 0:
            uri = self.__metadata__['outgoing_typed_relationships']
        elif direction < 0:
            uri = self.__metadata__['incoming_typed_relationships']
        else:
            uri = self.__metadata__['all_typed_relationships']
        return uri.replace(
            '{-list|&|types}', '&'.join(quote(type, "") for type in types)
        )

    @deprecated("Node.get_related_nodes is deprecated, please use "
                "Node.match instead.")
    def get_related_nodes(self, direction=Direction.EITHER, *types):
        """ Fetch all nodes related to the current node by a relationship in a
        given `direction` of a specific `type` (if supplied).

        .. deprecated:: 1.5
            use :py:func:`Node.match` instead.
        """
        if types:
            uri = self._typed_relationships_uri(direction, types)
        else:
            uri = self._relationships_uri(direction)
        return [
            Node(rel['start'] if rel['end'] == self.__uri__ else rel['end'])
            for rel in self._send(rest.Request(self._graph_db, "GET", uri)).body
        ]

    @deprecated("Node.get_relationships is deprecated, please use "
                "Node.match instead.")
    def get_relationships(self, direction=Direction.EITHER, *types):
        """ Fetch all relationships from the current node in a given
        `direction` of a specific `type` (if supplied).

        .. deprecated:: 1.5
            use :py:func:`Node.match` instead.
        """
        if types:
            uri = self._typed_relationships_uri(direction, types)
        else:
            uri = self._relationships_uri(direction)
        return [
            Relationship(rel['self'])
            for rel in self._send(rest.Request(self._graph_db, "GET", uri)).body
        ]

    @deprecated("Node.get_relationships_with is deprecated, please use "
                "Node.match instead.")
    def get_relationships_with(self, other, direction=Direction.EITHER, *types):
        """ Return all relationships between this node and another node using
        the relationship criteria supplied.

        .. deprecated:: 1.5
            use :py:func:`Node.match` instead.
        """
        if not isinstance(other, Node):
            raise ValueError
        if direction == Direction.EITHER:
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
        query = query.format(self._id, other._id, type)
        data, metadata = cypher.execute(self._graph_db, query)
        return [row[0] for row in data]

    @deprecated("Node.get_single_related_node is deprecated, please use "
                "Node.match_one instead.")
    def get_single_related_node(self, direction=Direction.EITHER, *types):
        """ Return only one node related to the current node by a relationship
        in the given `direction` of the specified `type`, if any such
        relationships exist.

        .. deprecated:: 1.5
            use :py:func:`Node.match_one` instead.
        """
        nodes = self.get_related_nodes(direction, *types)
        if nodes:
            return nodes[0]
        else:
            return None

    @deprecated("Node.get_single_relationship is deprecated, please use "
                "Node.match_one instead.")
    def get_single_relationship(self, direction=Direction.EITHER, *types):
        """ Fetch only one relationship from the current node in the given
        `direction` of the specified `type`, if any such relationships exist.

        .. deprecated:: 1.5
            use :py:func:`Node.match_one` instead.
        """
        relationships = self.get_relationships(direction, *types)
        if relationships:
            return relationships[0]
        else:
            return None

    @deprecated("Node.has_relationship is deprecated, please use "
                "Node.match_one instead.")
    def has_relationship(self, direction=Direction.EITHER, *types):
        """ Return :py:const:`True` if this node has any relationships with the
        specified criteria, :py:const:`False` otherwise.

        .. deprecated:: 1.5
            use :py:func:`Node.match_one` instead.
        """
        relationships = self.get_relationships(direction, *types)
        return bool(relationships)

    @deprecated("Node.has_relationship_with is deprecated, please use "
                "Node.match_one instead.")
    def has_relationship_with(self, other, direction=Direction.EITHER, *types):
        """ Return :py:const:`True` if this node has any relationships with the
        specified criteria, :py:const:`False` otherwise.

        .. deprecated:: 1.5
            use :py:func:`Node.match_one` instead.
        """
        relationships = self.get_relationships_with(other, direction, *types)
        return bool(relationships)

    @deprecated("Node.is_related_to is deprecated, please use "
                "Node.match_one instead.")
    def is_related_to(self, other, direction=Direction.EITHER, *types):
        """ Return :py:const:`True` if the current node is related to the other
        node using the relationship criteria supplied, :py:const:`False`
        otherwise.

        .. deprecated:: 1.5
            use :py:func:`Node.match_one` instead.
        """
        return bool(self.get_relationships_with(other, direction, *types))

    def isolate(self):
        """ Delete all relationships connected to this node, both incoming and
        outgoing.
        """
        cypher.execute(self._graph_db, (
            "START a=node({A}) "
            "MATCH a-[r]-b "
            "DELETE r "
        ), {"A": self._id})

    def match(self, rel_type=None, end_node=None, bidirectional=False,
              limit=None):
        """ Match one or more relationships attached to this node.

        :param rel_type: type of relationships to match or :py:const:`None` if
            any
        :param end_node: concrete end :py:class:`Node` to match or
            :py:const:`None` if any
        :param bidirectional: :py:const:`True` if reversed relationships should
            also be included
        :param limit: maximum number of relationships to match or
            :py:const:`None` if no limit

        .. seealso::
           :py:func:`GraphDatabaseService.match <py2neo.neo4j.GraphDatabaseService.match>`
        """
        return self._graph_db.match(self, rel_type, end_node, bidirectional, limit)

    def match_one(self, rel_type=None, end_node=None, bidirectional=False):
        """ Match a single relationship attached to this node.

        :param rel_type: type of relationships to match or :py:const:`None` if
            any
        :param end_node: concrete end :py:class:`Node` to match or
            :py:const:`None` if any
        :param bidirectional: :py:const:`True` if reversed relationships should
            also be included

        .. seealso::
           :py:func:`GraphDatabaseService.match <py2neo.neo4j.GraphDatabaseService.match>`
        """
        return self._graph_db.match(self, rel_type, end_node, bidirectional)

    def create_path(self, *items):
        """ Create a new path, starting at this node and chaining together the
        alternating relationships and nodes provided::

            (self)-[rel_0]->(node_0)-[rel_1]->(node_1) ...
                   |-----|  |------| |-----|  |------|
             item:    0        1        2        3

        Each relationship may be specified as one of the following:

        - an existing Relationship instance
        - a string holding the relationship type, e.g. "KNOWS"
        - a (`str`, `dict`) tuple holding both the relationship type and
          its properties, e.g. ("KNOWS", {"since": 1999})

        Nodes can be any of the following:

        - an existing Node instance
        - an integer containing the ID of an existing node
        - a `dict` holding a set of properties for a new node
        - a 3-tuple holding an index name, key and value for identifying
          indexed nodes, e.g. ("People", "email", "bob@example.com")
        - :py:const:`None`, representing an unspecified node that will be
          created as required

        :param items: alternating relationships and nodes
        :return: `Path` object representing the newly-created path
        """
        path = Path(self, *items)
        return path.create(self._graph_db)

    def get_or_create_path(self, *items):
        """ Identical to `create_path` except will reuse parts of the path
        which already exist.

        Some examples::

            # add dates to calendar, starting at calendar_root
            christmas_day = calendar_root.get_or_create_path(
                "YEAR",  {"number": 2000},
                "MONTH", {"number": 12},
                "DAY",   {"number": 25},
            )
            # `christmas_day` will now contain a `Path` object
            # containing the nodes and relationships used:
            # (CAL)-[:YEAR]->(2000)-[:MONTH]->(12)-[:DAY]->(25)

            # adding a second, overlapping path will reuse
            # nodes and relationships wherever possible
            christmas_eve = calendar_root.get_or_create_path(
                "YEAR",  {"number": 2000},
                "MONTH", {"number": 12},
                "DAY",   {"number": 24},
            )
            # `christmas_eve` will contain the same year and month nodes
            # as `christmas_day` but a different (new) day node:
            # (CAL)-[:YEAR]->(2000)-[:MONTH]->(12)-[:DAY]->(25)
            #                                  |
            #                                [:DAY]
            #                                  |
            #                                  v
            #                                 (24)

        """
        path = Path(self, *items)
        return path.get_or_create(self._graph_db)

    def update_properties(self, properties):
        """ Update properties with the values supplied.

        :param properties: dictionary of properties to integrate with existing
            properties
        """
        if self.__uri__:
            query, params = ["START a=node({A})"], {"A": self._id}
            for i, (key, value) in enumerate(properties.items()):
                value_tag = "V" + str(i)
                query.append("SET a.`" + key + "`={" + value_tag + "}")
                params[value_tag] = value
            query.append("RETURN a")
            data, metadata = cypher.execute(self._graph_db, " ".join(query), params)
            self._properties = data[0][0].__metadata__["data"]
        else:
            self._properties.update(properties)


class Relationship(_Entity):
    """ A relationship within a graph, identified by a URI.
    
    :param uri: URI identifying this relationship
    """

    @classmethod
    def abstract(cls, start_node, type, end_node, *labels, **properties):
        """ Create and return a new abstract relationship.
        """
        instance = cls(None)
        instance._start_node = start_node
        instance._type = type
        instance._end_node = end_node
        instance._labels = set(labels)
        instance._properties = dict(properties)
        return instance

    def __init__(self, uri):
        _Entity.__init__(self, uri)
        self._start_node = None
        self._type = None
        self._end_node = None

    def __eq__(self, other):
        other = _cast(other, Relationship)
        if self.__uri__:
            return _Entity.__eq__(self, other)
        else:
            return self._start_node == other._start_node and \
                   self._type == other._type and \
                   self._end_node == other._end_node and \
                   self._labels == other._labels and \
                   self._properties == other._properties

    def __ne__(self, other):
        other = _cast(other, Relationship)
        if self.__uri__:
            return _Entity.__ne__(self, other)
        else:
            return self._start_node != other._start_node or \
                   self._type != other._type or \
                   self._end_node != other._end_node or \
                   self._labels != other._labels or \
                   self._properties != other._properties

    def __repr__(self):
        if self.__uri__:
            return "{0}({1})".format(
                self.__class__.__name__,
                repr(str(self.__uri__))
            )
        elif self._properties:
            return "rel({1}, {2}, {3}, **{4})".format(
                self.__class__.__name__,
                repr(self.start_node),
                repr(self.type),
                repr(self.end_node),
                repr(self._properties)
            )
        else:
            return "rel({1}, {2}, {3})".format(
                self.__class__.__name__,
                repr(self.start_node),
                repr(self.type),
                repr(self.end_node)
            )

    def __str__(self):
        type_str = str(self.type)
        if not SIMPLE_NAME.match(type_str):
            type_str = json.dumps(type_str)
        if self._properties:
            return "{0}-[:{1} {2}]->{3}".format(
                str(self.start_node),
                type_str,
                json.dumps(self._properties, separators=(",", ":")),
                str(self.end_node),
            )
        else:
            return "{0}-[:{1}]->{2}".format(
                str(self.start_node),
                type_str,
                str(self.end_node),
            )

    @property
    def _id(self):
        """ Return the internal ID for this relationship.

        :return: integer ID of this relationship within the database or
            :py:const:`None` if abstract
        """
        if self.__uri__ is None:
            return None
        else:
            return int('0' + str(self.__uri__).rpartition('/')[-1])

    @property
    def end_node(self):
        """ Return the end node of this relationship.
        """
        if self.__uri__ and not self._end_node:
            self._end_node = Node(self.__metadata__['end'])
        return self._end_node

    @deprecated("Relationship.other_node is deprecated, please compare "
                "values with Relationship.start_node and "
                "Relationship.end_node instead.")
    def other_node(self, node):
        """ Return a node object representing the node within this
        relationship which is not the one supplied.
        """
        if self.__metadata__['end'] == node.__uri__:
            return self.start_node
        else:
            return self.end_node

    @property
    def id(self):
        """ Return the unique id for this relationship.
        """
        return self._id

    @deprecated("Relationship.is_type is deprecated, please compare values "
                "with Relationship.type instead.")
    def is_type(self, type):
        """ Return :py:const:`True` if this relationship is of the given type,
        :py:const:`False` otherwise.

        .. deprecated:: 1.5
            compare values with :py:attr:`Relationship.type` instead.
        """
        return self.type == type

    @property
    @deprecated("Relationship.nodes is deprecated, please use "
                "Relationship.start_node and Relationship.end_node instead.")
    def nodes(self):
        """ Return a tuple of the two nodes attached to this relationship.

        .. deprecated:: 1.5
            use :py:attr:`Relationship.start_node` and
            :py:attr:`Relationship.end_node` instead.
        """
        return self.start_node, self.end_node

    @property
    def start_node(self):
        """ Return the start node of this relationship.
        """
        if self.__uri__ and not self._start_node:
            self._start_node = Node(self.__metadata__['start'])
        return self._start_node

    @property
    def type(self):
        """ Return the type of this relationship as a string.
        """
        if self.__uri__ and not self._type:
            self._type = self.__metadata__['type']
        return self._type

    def update_properties(self, properties):
        """ Update the properties for this relationship with the values
        supplied.
        """
        if self.__uri__:
            query, params = ["START a=rel({A})"], {"A": self._id}
            for i, (key, value) in enumerate(properties.items()):
                value_tag = "V" + str(i)
                query.append("SET a.`" + key + "`={" + value_tag + "}")
                params[value_tag] = value
            query.append("RETURN a")
            data, metadata = cypher.execute(self._graph_db, " ".join(query), params)
            self._properties = data[0][0].__metadata__["data"]
        else:
            self._properties.update(properties)


class _UnboundRelationship(object):
    """ An abstract, partial relationship with no start or end nodes.
    """

    @classmethod
    def cast(cls, arg):
        if isinstance(arg, cls):
            return arg
        elif isinstance(arg, Relationship):
            # LABELS: replace below with get_labels when it exists!
            return cls(arg.type, *arg._labels, **arg.get_properties())
        elif isinstance(arg, tuple):
            if len(arg) == 1:
                return cls(str(arg[0]))
            elif len(arg) == 2:
                if isinstance(arg[1], dict):
                    return cls(str(arg[0]), **arg[1])
                else:
                    return cls(str(arg[0]), *arg[1])
            elif len(arg) == 3:
                return cls(str(arg[0]), *arg[1], **arg[2])
            else:
                raise TypeError(arg)
        else:
            return cls(str(arg))

    def __init__(self, type, *labels, **properties):
        self._type = type
        self._labels = set(labels)
        self._properties = dict(properties)

    def __eq__(self, other):
        return self._type == other._type and \
               self._labels == other._labels and \
               self._properties == other._properties

    def __ne__(self, other):
        return self._type != other._type or \
               self._labels != other._labels or \
               self._properties != other._properties

    def __repr__(self):
        return "({0}, *{1}, **{2})".format(
            repr(str(self._type)),
            repr(tuple(self._labels)),
            repr(self._properties),
        )

    def __str__(self):
        return "-[:{0}]->".format(
            json.dumps(str(self._type)),
        )

    def bind(self, start_node, end_node):
        return Relationship.abstract(start_node, self._type, end_node,
                                     *self._labels, **self._properties)


class Path(object):
    """ A representation of a sequence of nodes connected by relationships. for
    example::

        >>> from py2neo import neo4j, node
        >>> alice, bob, carol = node(name="Alice"), node(name="Bob"), node(name="Carol")
        >>> abc = neo4j.Path(alice, "KNOWS", bob, "KNOWS", carol)
        >>> abc.nodes
        [node(**{'name': 'Alice'}), node(**{'name': 'Bob'}), node(**{'name': 'Carol'})]
        >>> dave, eve = node(name="Dave"), node(name="Eve")
        >>> de = neo4j.Path(dave, "KNOWS", eve)
        >>> de.nodes
        [node(**{'name': 'Dave'}), node(**{'name': 'Eve'})]
        >>> abcde = neo4j.Path.join(abc, "KNOWS", de)
        >>> str(abcde)
        '({"name":"Alice"})-[:"KNOWS"]->({"name":"Bob"})-[:"KNOWS"]->({"name":"Carol"})-[:"KNOWS"]->({"name":"Dave"})-[:"KNOWS"]->({"name":"Eve"})'

    """

    def __init__(self, node, *rels_and_nodes):
        self._nodes = [_node(node)]
        self._nodes.extend(_node(n) for n in rels_and_nodes[1::2])
        if len(rels_and_nodes) % 2 != 0:
            # If a trailing relationship is supplied, add a dummy end node
            self._nodes.append(_node())
        self._relationships = [
            _UnboundRelationship.cast(r)
            for r in rels_and_nodes[0::2]
        ]

    def __repr__(self):
        out = ", ".join(repr(item) for item in round_robin(self._nodes, self._relationships))
        return "Path({0})".format(out)

    def __str__(self):
        out = []
        for i, rel in enumerate(self._relationships):
            out.append(str(self._nodes[i]))
            out.append(str(rel))
        out.append(str(self._nodes[-1]))
        return "".join(out)

    def __nonzero__(self):
        return bool(self._relationships)

    def __len__(self):
        return len(self._relationships)

    def __eq__(self, other):
        return self._nodes == other._nodes and \
               self._relationships == other._relationships

    def __ne__(self, other):
        return self._nodes != other._nodes or \
               self._relationships != other._relationships

    def __getitem__(self, item):
        size = len(self._relationships)
        def adjust(value, default=None):
            if value is None:
                return default
            if value < 0:
                return value + size
            else:
                return value
        if isinstance(item, slice):
            if item.step is not None:
                raise ValueError("Steps not supported in path slicing")
            start, stop = adjust(item.start, 0), adjust(item.stop, size)
            path = Path(self._nodes[start])
            for i in range(start, stop):
                path._relationships.append(self._relationships[i])
                path._nodes.append(self._nodes[i + 1])
            return path
        else:
            i = int(item)
            if i < 0:
                i += len(self._relationships)
            return Path(self._nodes[i], self._relationships[i], self._nodes[i + 1])

    def __iter__(self):
        return iter(
            _rel((self._nodes[i], rel, self._nodes[i + 1]))
            for i, rel in enumerate(self._relationships)
        )

    def order(self):
        """ Return the number of nodes within this path.
        """
        return len(self._nodes)

    def size(self):
        """ Return the number of relationships within this path.
        """
        return len(self._relationships)

    @property
    def nodes(self):
        """ Return a list of all the nodes which make up this path.
        """
        return list(self._nodes)

    @property
    def relationships(self):
        """ Return a list of all the relationships which make up this path.
        """
        return [
            _rel((self._nodes[i], rel, self._nodes[i + 1]))
            for i, rel in enumerate(self._relationships)
        ]

    @classmethod
    def join(cls, left, rel, right):
        """ Join the two paths `left` and `right` with the relationship `rel`.
        """
        if isinstance(left, Path):
            left = left[:]
        else:
            left = Path(left)
        if isinstance(right, Path):
            right = right[:]
        else:
            right = Path(right)
        left._relationships.append(_UnboundRelationship.cast(rel))
        left._nodes.extend(right._nodes)
        left._relationships.extend(right._relationships)
        return left

    def _create(self, graph_db, verb):
        nodes, path, values, params = [], [], [], {}
        def append_node(i, node):
            if node is None:
                path.append("(n{0})".format(i))
                values.append("n{0}".format(i))
            elif node.is_abstract():
                path.append("(n{0} {{p{0}}})".format(i))
                params["p{0}".format(i)] = compact(node._properties)
                values.append("n{0}".format(i))
            else:
                path.append("(n{0})".format(i))
                nodes.append("n{0}=node({{i{0}}})".format(i))
                params["i{0}".format(i)] = node._id
                values.append("n{0}".format(i))
        def append_rel(i, rel):
            if rel._properties:
                path.append("-[r{0}:`{1}` {{q{0}}}]->".format(i, rel._type))
                params["q{0}".format(i)] = compact(rel._properties)
                values.append("r{0}".format(i))
            else:
                path.append("-[r{0}:`{1}`]->".format(i, rel._type))
                values.append("r{0}".format(i))
        append_node(0, self._nodes[0])
        for i, rel in enumerate(self._relationships):
            append_rel(i, rel)
            append_node(i + 1, self._nodes[i + 1])
        clauses = []
        if nodes:
            clauses.append("START {0}".format(",".join(nodes)))
        clauses.append("{0} {1}".format(verb, "".join(path)))
        clauses.append("RETURN {0}".format(",".join(values)))
        query = " ".join(clauses)
        try:
            data, metadata = cypher.execute(graph_db, query, params)
            return Path(*data[0])
        except cypher.CypherError:
            raise NotImplementedError(
                "The Neo4j server at <{0}> does not support "
                "Cypher CREATE UNIQUE clauses or the query contains "
                "an unsupported property type".format(graph_db.__uri__)
            )

    def create(self, graph_db):
        """ Construct a path within the specified `graph_db` from the nodes
        and relationships within this :py:class:`Path` instance. This makes
        use of Cypher's ``CREATE`` clause.
        """
        return self._create(graph_db, "CREATE")

    def get_or_create(self, graph_db):
        """ Construct a unique path within the specified `graph_db` from the
        nodes and relationships within this :py:class:`Path` instance. This
        makes use of Cypher's ``CREATE UNIQUE`` clause.
        """
        return self._create(graph_db, "CREATE UNIQUE")


class Index(rest.Resource):
    """ Searchable database index which can contain either nodes or
    relationships.

    .. seealso:: :py:func:`GraphDatabaseService.get_or_create_index`
    """

    def __init__(self, content_type, template_uri):
        rest.Resource.__init__(
            self, template_uri.rpartition("/{key}/{value}")[0]
        )
        self._name = str(self.__uri__).rpartition("/")[2]
        self._content_type = content_type
        self._template_uri = template_uri
        self._graph_db = GraphDatabaseService.get_instance(self.__uri__.base)

    def __repr__(self):
        return "{0}({1},'{2}')".format(
            self.__class__.__name__,
            repr(self._content_type.__name__),
            repr(self.__uri__)
        )

    def add(self, key, value, entity):
        """ Add an entity to this index under the `key`:`value` pair supplied::

            # create a node and obtain a reference to the "People" node index
            alice, = graph_db.create({"name": "Alice Smith"})
            people = graph_db.get_or_create_index(neo4j.Node, "People")

            # add the node to the index
            people.add("family_name", "Smith", alice)

        Note that while Neo4j indexes allow multiple entities to be added under
        a particular key:value, the same entity may only be represented once;
        this method is therefore idempotent.
        """
        self._send(rest.Request(self._graph_db, "POST", str(self.__uri__), {
            "key": key,
            "value": value,
            "uri": str(entity.__uri__)
        }))
        return entity

    def add_if_none(self, key, value, entity):
        """ Add an entity to this index under the `key`:`value` pair
        supplied if no entry already exists at that point::

            # obtain a reference to the "Rooms" node index and
            # add node `alice` to room 100 if empty
            rooms = graph_db.get_or_create_index(neo4j.Node, "Rooms")
            rooms.add_if_none("room", 100, alice)

        If added, this method returns the entity, otherwise :py:const:`None`
        is returned.
        """
        rs = self._send(rest.Request(self._graph_db, "POST", str(self.__uri__) + "?unique", {
            "key": key,
            "value": value,
            "uri": str(entity.__uri__)
        }))
        if rs.status == 201:
            return entity
        else:
            return None

    @property
    def content_type(self):
        """ Return the type of entity contained within this index. Will return
        either :py:class:`Node` or :py:class:`Relationship`.
        """
        return self._content_type

    @property
    def name(self):
        """ Return the name of this index.
        """
        return self._name

    def get(self, key, value):
        """ Fetch a list of all entities from the index which are associated
        with the `key`:`value` pair supplied::

            # obtain a reference to the "People" node index and
            # get all nodes where `family_name` equals "Smith"
            people = graph_db.get_or_create_index(neo4j.Node, "People")
            smiths = people.get("family_name", "Smith")

        ..
        """
        results = self._send(rest.Request(self._graph_db, "GET", self._template_uri.format(
            key=quote(key, ""),
            value=quote(value, "")
        )))
        return [
            self._content_type(result['self'])
            for result in results.body
        ]

    def create(self, key, value, abstract):
        """ Create and index a new node or relationship using the abstract
        provided.
        """
        batch = WriteBatch(self._graph_db)
        if self._content_type is Node:
            batch.create_node(abstract)
            batch.add_indexed_node(self, key, value, 0)
        elif self._content_type is Relationship:
            if len(abstract) == 3:
                (start_node, type_, end_node), properties = abstract, None
            elif len(abstract) == 4:
                start_node, type_, end_node, properties = abstract
            else:
                raise ValueError(abstract)
            if not isinstance(start_node, Node):
                raise TypeError(start_node)
            if not isinstance(end_node, Node):
                raise TypeError(end_node)
            batch.create_relationship(start_node, type_, end_node, properties)
            batch.add_indexed_relationship(self, key, value, 0)
        else:
            raise TypeError(self._content_type)
        entity, index_entry = batch.submit()
        return entity

    def _create_unique(self, key, value, abstract):
        """ Internal method to support `get_or_create` and `create_if_none`.
        """
        if self._content_type is Node:
            body = {
                "key": key,
                "value": value,
                "properties": abstract
            }
        elif self._content_type is Relationship:
            body = {
                "key": key,
                "value": value,
                "start": str(abstract[0].__uri__),
                "type": abstract[1],
                "end": str(abstract[2].__uri__),
                "properties": abstract[3] if len(abstract) > 3 else None
            }
        else:
            raise TypeError(self._content_type)
        return self._send(rest.Request(
            self._graph_db, "POST", str(self.__uri__) + "?unique", body)
        )

    def get_or_create(self, key, value, abstract):
        """ Fetch a single entity from the index which is associated with the
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
        rs = self._create_unique(key, value, abstract)
        return self._content_type(rs.body["self"])

    def create_if_none(self, key, value, abstract):
        """ Create a new entity with the specified details within the current
        index, under the `key`:`value` pair supplied, if no such entity already
        exists. If creation occurs, the new entity will be returned, otherwise
        :py:const:`None` will be returned::

            # obtain a reference to the "Contacts" node index and
            # create a node for Alice if one does not already exist
            contacts = graph_db.get_or_create_index(neo4j.Node, "Contacts")
            alice = contacts.create_if_none("name", "SMITH, Alice", {
                "given_name": "Alice Jane", "family_name": "Smith",
                "phone": "01234 567 890", "mobile": "07890 123 456"
            })

        ..
        """
        rs = self._create_unique(key, value, abstract)
        if rs.status == 201:
            return self._content_type(rs.body["self"])
        else:
            return None

    def remove(self, key=None, value=None, entity=None):
        """ Remove any entries from the index which match the parameters
        supplied. The allowed parameter combinations are:

        `key`, `value`, `entity`
            remove a specific entity indexed under a given key-value pair

        `key`, `value`
            remove all entities indexed under a given key-value pair

        `key`, `entity`
            remove a specific entity indexed against a given key but with
            any value

        `entity`
            remove all occurrences of a specific entity regardless of
            key and value

        """
        if key and value and entity:
            self._send(rest.Request(
                self._graph_db, "DELETE", "{0}/{1}/{2}/{3}".format(
                    self.__uri__,
                    quote(key, ""),
                    quote(value, ""),
                    entity._id,
                )
            ))
        elif key and value:
            entities = [
                item['indexed']
                for item in self._send(rest.Request(
                    self._graph_db, "GET", self._template_uri.format(
                        key=quote(key, ""),
                        value=quote(value, "")
                    )
                )).body
            ]
            batch = WriteBatch(self._graph_db)
            for entity in entities:
                batch._append(rest.Request(
                    self._graph_db, "DELETE",
                    rest.URI(entity).reference,
                ))
            batch._submit()
        elif key and entity:
            self._send(rest.Request(
                self._graph_db, "DELETE", "{0}/{1}/{2}".format(
                    self.__uri__,
                    quote(key, ""),
                    entity._id,
                )
            ))
        elif entity:
            self._send(rest.Request(
                self._graph_db, "DELETE", "{0}/{1}".format(
                    self.__uri__,
                    entity._id,
                )
            ))
        else:
            raise TypeError("Illegal parameter combination for index removal")

    def query(self, query):
        """ Query the index according to the supplied query criteria, returning
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
            for item in self._send(rest.Request(self._graph_db, "GET", "{0}?query={1}".format(
                self.__uri__, quote(query, "")
            ))).body
        ]


def _cast(obj, cls=(Node, Relationship), abstract=None):
    if obj is None:
        return None
    elif isinstance(obj, Node) or isinstance(obj, dict):
        entity = _node(obj)
    elif isinstance(obj, Relationship) or isinstance(obj, tuple):
        entity = _rel(obj)
    else:
        raise TypeError(obj)
    if not isinstance(entity, cls):
        raise TypeError(obj)
    if abstract is not None and bool(abstract) != bool(entity.is_abstract()):
        raise TypeError(obj)
    return entity


class _Batch(object):

    def __init__(self, graph_db):
        if not isinstance(graph_db, GraphDatabaseService):
            raise TypeError(graph_db)
        self._graph_db = graph_db
        self._create_node_uri = rest.URI(self._graph_db.__metadata__["node"]).reference
        self._cypher_uri = rest.URI(self._graph_db._cypher_uri).reference
        self.clear()

    def __len__(self):
        return len(self.requests)

    def __nonzero__(self):
        return bool(self.requests)

    def _submit(self):
        """ Submits batch of requests, returning list of Response objects.
        """
        rs = self._graph_db._send(rest.Request(self._graph_db, "POST", self._graph_db._batch_uri, [
            request.description(id_)
            for id_, request in enumerate(self.requests)
        ]))
        self.clear()
        return [
            rest.Response(
                self._graph_db,
                response.get("status", rs.status),
                response["from"],
                response.get("location", None),
                response.get("body", None),
                id=response.get("id", None),
            )
            for response in rs.body
        ]

    def _append(self, request):
        """ Append a :py:class:`rest.Request` to this batch.
        """
        self.requests.append(request)

    def clear(self):
        """ Clear all requests from this batch.
        """
        self.requests = []

    def submit(self):
        """ Submit the current batch of requests, returning a list of
            the objects returned.
        """
        return [
            self._graph_db._resolve(response.body, response.status, id_=response.id)
            for response in self._submit()
        ]


class ReadBatch(_Batch):

    def __init__(self, graph_db):
        _Batch.__init__(self, graph_db)

    def _get(self, uri, body=None):
        self._append(rest.Request(self._graph_db, "GET", uri, body))

    def _index(self, content_type, index):
        if isinstance(index, Index):
            if content_type != index._content_type:
                raise TypeError("Index is not for {0}s".format(content_type))
            return index
        else:
            return self._graph_db.get_or_create_index(content_type, str(index))

    def get_properties(self, entity):
        """ Fetch properties for the given entity.

        :param entity: concrete entity from which to fetch properties
        """
        entity = _cast(entity, abstract=False)
        self._get(rest.URI(entity.__metadata__["properties"]).reference)

    def get_indexed_nodes(self, index, key, value):
        """ Fetch all nodes indexed under the given key-value pair.

        :param index: index name or instance
        :param key: key under which nodes are indexed
        :param value: value under which nodes are indexed
        """
        index = self._index(Node, index)
        self._get(index._template_uri.format(
            key=quote(key, ""),
            value=quote(value, "")
        ))


class WriteBatch(_Batch):

    def __init__(self, graph_db):
        _Batch.__init__(self, graph_db)

    def _post(self, uri, body=None):
        self._append(rest.Request(self._graph_db, "POST", uri, body))

    def _delete(self, uri, body=None):
        self._append(rest.Request(self._graph_db, "DELETE", uri, body))

    def _put(self, uri, body=None):
        self._append(rest.Request(self._graph_db, "PUT", uri, body))

    def _relative_node_uri(self, node):
        if isinstance(node, Node):
            node._must_belong_to(self._graph_db)
            node = _cast(node, Node, abstract=False)
            return rest.URI(node).reference
        else:
            return "{" + str(node) + "}"

    def create(self, abstract):
        """ Create a node or relationship based on the abstract entity
        provided. For example:

        ::

            batch = WriteBatch(graph_db)
            batch.create(node(name="Alice"))
            batch.create(node(name="Bob"))
            batch.create(rel(0, "KNOWS", 1))
            results = batch.submit()

        :param abstract: abstract node or relationship
        """
        entity = _cast(abstract, abstract=True)
        if isinstance(entity, Node):
            uri = self._create_node_uri
            body = compact(entity._properties)
        elif isinstance(entity, Relationship):
            uri = self._relative_node_uri(entity._start_node) + "/relationships"
            body = {
                "type": entity._type,
                "to": self._relative_node_uri(entity._end_node),
            }
            if entity._properties:
                body["data"] = compact(entity._properties)
        else:
            raise TypeError(entity)
        self._post(uri, body)

    @deprecated("WriteBatch.create_node is deprecated, use "
                "WriteBatch.create instead.")
    def create_node(self, properties=None):
        """

        .. deprecated:: 1.5
            use :py:func:`WriteBatch.create` instead.
        """
        self._post(self._create_node_uri, compact(properties or {}))

    @deprecated("WriteBatch.create_relationship is deprecated, use "
                "WriteBatch.create instead.")
    def create_relationship(self, start_node, type_, end_node, properties=None):
        """

        .. deprecated:: 1.5
            use :py:func:`WriteBatch.create` instead.
        """
        body = {
            "type": type_,
            "to": self._relative_node_uri(end_node),
        }
        if properties:
            body["data"] = compact(properties)
        self._post(self._relative_node_uri(start_node) + "/relationships", body)

    def get_or_create(self, rel_abstract):
        """ Use the abstract supplied to create a new relationship if one does
        not already exist.

        :param rel_abstract: relationship abstract to be fetched or created
        """
        rel = _cast(rel_abstract, Relationship, abstract=True)
        if not (isinstance(rel._start_node, Node) or rel._start_node is None):
            raise TypeError("Relationship start node must be a "
                            "Node instance or None")
        if not (isinstance(rel._end_node, Node) or rel._end_node is None):
            raise TypeError("Relationship end node must be a "
                            "Node instance or None")
        if rel._start_node and rel._end_node:
            query = (
                "START a=node({A}), b=node({B}) "
                "CREATE UNIQUE (a)-[ab:`" + str(rel._type) + "` {P}]->(b) "
                "RETURN ab"
            )
        elif rel._start_node:
            query = (
                "START a=node({A}) "
                "CREATE UNIQUE (a)-[ab:`" + str(rel._type) + "` {P}]->() "
                "RETURN ab"
            )
        elif rel._end_node:
            query = (
                "START b=node({A}) "
                "CREATE UNIQUE ()-[ab:`" + str(rel._type) + "` {P}]->(b) "
                "RETURN ab"
            )
        else:
            raise ValueError("Either start node or end node must be "
                             "specified for a unique relationship")
        params = {"P": compact(rel._properties or {})}
        if rel._start_node:
            params["A"] = rel._start_node._id
        if rel._end_node:
            params["B"] = rel._end_node._id
        self._post(self._cypher_uri, {"query": query, "params": params})

    @deprecated("WriteBatch.get_or_create_relationship is deprecated, use "
                "WriteBatch.get_or_create instead.")
    def get_or_create_relationship(self, start_node, type_, end_node, properties=None):
        """

        .. deprecated:: 1.5
            use :py:func:`WriteBatch.get_or_create` instead.
        """
        if not (isinstance(start_node, Node) or start_node is None):
            raise TypeError(start_node)
        if not (isinstance(end_node, Node) or end_node is None):
            raise TypeError(end_node)
        if start_node and end_node:
            query = "START a=node({a}), b=node({b}) " \
                    "CREATE UNIQUE (a)-[ab:`" + str(type_) + "` {p}]->(b) " \
                    "RETURN ab"
        elif start_node:
            query = "START a=node({a}) " \
                    "CREATE UNIQUE (a)-[ab:`" + str(type_) + "` {p}]->() " \
                    "RETURN ab"
        elif end_node:
            query = "START b=node({b}) " \
                    "CREATE UNIQUE ()-[ab:`" + str(type_) + "` {p}]->(b) " \
                    "RETURN ab"
        else:
            raise ValueError("Either start node or end node must be "
                             "specified for a unique relationship")
        params = {"p": compact(properties or {})}
        if start_node:
            params["a"] = start_node._id
        if end_node:
            params["b"] = end_node._id
        self._post(self._cypher_uri, {"query": query, "params": params})

    def delete(self, entity):
        """ Delete the specified entity from the graph.

        :param entity: concrete node or relationship to be deleted
        """
        entity = _cast(entity, abstract=False)
        self._delete(rest.URI(entity).reference)

    @deprecated("WriteBatch.delete_node is deprecated, use "
                "WriteBatch.delete instead.")
    def delete_node(self, node):
        """

        .. deprecated:: 1.5
            use :py:func:`WriteBatch.delete` instead.
        """
        self._delete(node.__uri__.reference)

    @deprecated("WriteBatch.delete_relationship is deprecated, use "
                "WriteBatch.delete instead.")
    def delete_relationship(self, relationship):
        """

        .. deprecated:: 1.5
            use :py:func:`WriteBatch.delete` instead.
        """
        self._delete(relationship.__uri__.reference)

    def set_property(self, entity, key, value):
        """ Set a single property on an entity.

        :param entity: concrete entity on which to set property
        :param key: property key
        :param value: property value
        """
        if value is None:
            self.delete_property(entity, key)
        else:
            entity = _cast(entity, abstract=False)
            uri = rest.URI(entity.__metadata__['property'].format(key=quote(key, "")))
            self._put(uri.reference, value)

    @deprecated("WriteBatch.set_node_property is deprecated, use "
                "WriteBatch.set_property instead.")
    def set_node_property(self, node, key, value):
        """

        .. deprecated:: 1.5
            use :py:func:`WriteBatch.set_property` instead.
        """
        if value is None:
            self.delete_node_property(node, key)
        else:
            uri = rest.URI(node.__metadata__['property'].format(key=quote(key, "")))
            self._put(uri.reference, value)

    def set_properties(self, entity, properties):
        """ Replace all properties on an entity.

        :param entity: concrete entity on which to set properties
        :param properties: dictionary of properties
        """
        entity = _cast(entity, abstract=False)
        uri = rest.URI(entity.__metadata__['properties'])
        self._put(uri.reference, compact(properties))

    @deprecated("WriteBatch.set_node_properties is deprecated, use "
                "WriteBatch.set_properties instead.")
    def set_node_properties(self, node, properties):
        """

        .. deprecated:: 1.5
            use :py:func:`WriteBatch.set_properties` instead.
        """
        uri = rest.URI(node.__metadata__['properties'])
        self._put(uri.reference, compact(properties))

    def delete_property(self, entity, key):
        """ Delete a single property from an entity.

        :param entity: concrete entity from which to delete property
        :param key: property key
        """
        entity = _cast(entity, abstract=False)
        uri = rest.URI(entity.__metadata__['property'].format(key=quote(key, "")))
        self._delete(uri.reference)

    @deprecated("WriteBatch.delete_node_property is deprecated, use "
                "WriteBatch.delete_property instead.")
    def delete_node_property(self, node, key):
        """

        .. deprecated:: 1.5
            use :py:func:`WriteBatch.delete_property` instead.
        """
        uri = rest.URI(node.__metadata__['property'].format(key=quote(key, "")))
        self._delete(uri.reference)

    def delete_properties(self, entity):
        """ Delete all properties from an entity.

        :param entity: concrete entity from which to delete properties
        """
        entity = _cast(entity, abstract=False)
        uri = rest.URI(entity.__metadata__['properties'])
        self._delete(uri.reference)

    @deprecated("WriteBatch.delete_node_properties is deprecated, use "
                "WriteBatch.delete_properties instead.")
    def delete_node_properties(self, node):
        """ Delete all properties from a node.

        .. deprecated:: 1.5
            use :py:func:`WriteBatch.delete_properties` instead.
        """
        uri = rest.URI(node.__metadata__['properties'])
        self._delete(uri.reference)

    @deprecated("WriteBatch.set_relationship_property is deprecated, use "
                "WriteBatch.set_property instead.")
    def set_relationship_property(self, relationship, key, value):
        """ Set a single property on a relationship.

        .. deprecated:: 1.5
            use :py:func:`WriteBatch.set_property` instead.
        """
        if value is None:
            self.delete_relationship_property(relationship, key)
        else:
            uri = rest.URI(relationship.__metadata__['property'].format(key=quote(key, "")))
            self._put(uri.reference, value)

    @deprecated("WriteBatch.set_relationship_properties is deprecated, use "
                "WriteBatch.set_properties instead.")
    def set_relationship_properties(self, relationship, properties):
        """

        .. deprecated:: 1.5
            use :py:func:`WriteBatch.set_properties` instead.
        """
        uri = rest.URI(relationship.__metadata__['properties'])
        self._put(uri.reference, compact(properties))

    @deprecated("WriteBatch.delete_relationship_property is deprecated, use "
                "WriteBatch.delete_property instead.")
    def delete_relationship_property(self, relationship, key):
        """

        .. deprecated:: 1.5
            use :py:func:`WriteBatch.delete_property` instead.
        """
        uri = rest.URI(relationship.__metadata__['property'].format(key=quote(key, "")))
        self._delete(uri.reference)

    @deprecated("WriteBatch.delete_relationship_properties is deprecated, use "
                "WriteBatch.delete_properties instead.")
    def delete_relationship_properties(self, relationship):
        """

        .. deprecated:: 1.5
            use :py:func:`WriteBatch.delete_properties` instead.
        """
        uri = rest.URI(relationship.__metadata__['properties'])
        self._delete(uri.reference)

    def _node_uri(self, node):
        if isinstance(node, Node):
            return str(node.__uri__)
        else:
            return "{" + str(node) + "}"

    def _relationship_uri(self, relationship):
        if isinstance(relationship, Relationship):
            return str(relationship.__uri__)
        else:
            return "{" + str(relationship) + "}"

    def _index(self, content_type, index):
        if isinstance(index, Index):
            if content_type != index._content_type:
                raise TypeError("Index is not for {0}s".format(content_type))
            return index
        else:
            return self._graph_db.get_or_create_index(content_type, str(index))

    def _create_indexed_node(self, index, uri_suffix, key, value, properties):
        index_uri = self._index(Node, index).__uri__
        self._post(index_uri.reference + uri_suffix, body = {
            "key": key,
            "value": value,
            "properties": compact(properties or {})
        })

    def get_or_create_indexed_node(self, index, key, value, properties=None):
        """ Create and index a new node if one does not already exist,
            returning either the new node or the existing one.
        """
        if self._graph_db.neo4j_version >= (1, 9):
            self._create_indexed_node(index, "?uniqueness=get_or_create", key, value, compact(properties))
        else:
            self._create_indexed_node(index, "?unique", key, value, compact(properties))

    def create_indexed_node_or_fail(self, index, key, value, properties=None):
        """ Create and index a new node if one does not already exist,
            fail otherwise.
        """
        if self._graph_db.neo4j_version >= (1, 9):
            self._create_indexed_node(index, "?uniqueness=create_or_fail", key, value, compact(properties))
        else:
            raise NotImplementedError("Uniqueness mode `create_or_fail` "
                                      "requires version 1.9 or above")

    def _add_indexed_node(self, index, uri_suffix, key, value, node):
        index_uri = self._index(Node, index).__uri__
        self._post(index_uri.reference + uri_suffix, body = {
            "key": key,
            "value": value,
            "uri": self._node_uri(node)
        })

    def add_indexed_node(self, index, key, value, node):
        """ Add an existing node to the index specified.
        """
        self._add_indexed_node(index, "", key, value, node)

    def get_or_add_indexed_node(self, index, key, value, node):
        """ Add an existing node to the index specified if an entry does not
            already exist for the given key-value pair, returning either the
            added node or the one already in the index.
        """
        if self._graph_db.neo4j_version >= (1, 9):
            self._add_indexed_node(index, "?uniqueness=get_or_create", key, value, node)
        else:
            self._add_indexed_node(index, "?unique", key, value, node)

    def add_indexed_node_or_fail(self, index, key, value, node):
        """ Add an existing node to the index specified if an entry does not
            already exist for the given key-value pair, fail otherwise.
        """
        if self._graph_db.neo4j_version >= (1, 9):
            self._add_indexed_node(index, "?uniqueness=create_or_fail", key, value, node)
        else:
            raise NotImplementedError("Uniqueness mode `create_or_fail` "
                                      "requires version 1.9 or above")

    def remove_indexed_node(self, index, key=None, value=None, node=None):
        """Remove any entries from the index which pertain to the parameters
        supplied. The allowed parameter combinations are:

        `key`, `value`, `node`
            remove a specific node indexed under a given key-value pair

        `key`, `node`
            remove a specific node indexed against a given key but with
            any value

        `node`
            remove all occurrences of a specific node regardless of
            key and value

        """
        index_uri = self._index(Node, index).__uri__
        if key and value and node:
            self._delete("{0}/{1}/{2}/{3}".format(
                index_uri,
                quote(key, ""),
                quote(value, ""),
                node._id,
            ))
        elif key and node:
            self._delete("{0}/{1}/{2}".format(
                index_uri,
                quote(key, ""),
                node._id,
            ))
        elif node:
            self._delete("{0}/{1}".format(
                index_uri,
                node._id,
            ))
        else:
            raise TypeError("Illegal parameter combination for index removal")

    def _create_indexed_relationship(self, index, uri_suffix, key, value, start_node, type_, end_node, properties):
        index_uri = self._index(Relationship, index).__uri__
        self._post(index_uri.reference + uri_suffix, body = {
            "key": key,
            "value": value,
            "start": self._node_uri(start_node),
            "type": str(type_),
            "end": self._node_uri(end_node),
            "properties": properties or {}
        })

    def get_or_create_indexed_relationship(self, index, key, value, start_node, type_, end_node, properties=None):
        """ Create and index a new relationship if one does not already exist,
            returning either the new relationship or the existing one.
        """
        if self._graph_db.neo4j_version >= (1, 9):
            self._create_indexed_relationship(index, "?uniqueness=get_or_create", key, value, start_node, type_, end_node, properties)
        else:
            self._create_indexed_relationship(index, "?unique", key, value, start_node, type_, end_node, properties)

    def create_indexed_relationship_or_fail(self, index, key, value, start_node, type_, end_node, properties=None):
        """ Create and index a new relationship if one does not already exist,
            fail otherwise.
        """
        if self._graph_db.neo4j_version >= (1, 9):
            self._create_indexed_relationship(index, "?uniqueness=create_or_fail", key, value, start_node, type_, end_node, properties)
        else:
            raise NotImplementedError("Uniqueness mode `create_or_fail` "
                                      "requires version 1.9 or above")

    def _add_indexed_relationship(self, index, uri_suffix, key, value, relationship):
        index_uri = self._index(Relationship, index).__uri__
        self._post(index_uri.reference + uri_suffix, body = {
            "key": key,
            "value": value,
            "uri": self._relationship_uri(relationship)
        })

    def add_indexed_relationship(self, index, key, value, relationship):
        """ Add an existing relationship to the index specified.
        """
        self._add_indexed_relationship(index, "", key, value, relationship)

    def get_or_add_indexed_relationship(self, index, key, value, relationship):
        """ Add an existing relationship to the index specified if an entry does not
            already exist for the given key-value pair, returning either the
            added relationship or the one already in the index.
        """
        if self._graph_db.neo4j_version >= (1, 9):
            self._add_indexed_relationship(index, "?uniqueness=get_or_create", key, value, relationship)
        else:
            self._add_indexed_relationship(index, "?unique", key, value, relationship)

    def add_indexed_relationship_or_fail(self, index, key, value, relationship):
        """ Add an existing relationship to the index specified if an entry does not
            already exist for the given key-value pair, fail otherwise.
        """
        if self._graph_db.neo4j_version >= (1, 9):
            self._add_indexed_relationship(index, "?uniqueness=create_or_fail", key, value, relationship)
        else:
            raise NotImplementedError("Uniqueness mode `create_or_fail` "
                                      "requires version 1.9 or above")

    def remove_indexed_relationship(self, index, key=None, value=None, relationship=None):
        """Remove any entries from the index which pertain to the parameters
        supplied. The allowed parameter combinations are:

        `key`, `value`, `relationship`
            remove a specific relationship indexed under a given key-value pair

        `key`, `relationship`
            remove a specific relationship indexed against a given key but with
            any value

        `relationship`
            remove all occurrences of a specific relationship regardless of
            key and value

        """
        index_uri = self._index(Relationship, index).__uri__
        if key and value and relationship:
            self._delete("{0}/{1}/{2}/{3}".format(
                index_uri,
                quote(key, ""),
                quote(value, ""),
                relationship._id,
            ))
        elif key and relationship:
            self._delete("{0}/{1}/{2}".format(
                index_uri,
                quote(key, ""),
                relationship._id,
            ))
        elif relationship:
            self._delete("{0}/{1}".format(
                index_uri,
                relationship._id,
            ))
        else:
            raise TypeError("Illegal parameter combination for index removal")
