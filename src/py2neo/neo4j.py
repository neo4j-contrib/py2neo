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

""" The neo4j module provides the main Neo4j client functionality and will be
the starting point for most applications.
"""

from __future__ import absolute_import, division, print_function, unicode_literals

import base64
import json
import logging

from . import rest, cypher
from .util import compact, quote, round_robin, deprecated, version_tuple

logger = logging.getLogger(__name__)

DEFAULT_URI = "http://localhost:7474/db/data/"


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
    raise AssertionError(
        "URI <{0}> does not appear to identify a {1}: {2}".format(
            uri, cls.__name__, json.dumps(metadata, separators=(",", ":"))
        )
    )


class Direction(object):
    """ Defines the direction of a relationship. This class is deprecated as
    all dependent functions are also deprecated.
    """

    BOTH     =  0
    EITHER   =  0
    INCOMING = -1
    OUTGOING =  1


class _Batch(object):

    def __init__(self, graph_db):
        assert isinstance(graph_db, GraphDatabaseService)
        self._graph_db = graph_db
        self._create_node_uri = rest.URI(self._graph_db.__metadata__["node"], "/node").reference
        self._cypher_uri = rest.URI(self._graph_db._cypher_uri, "/cypher").reference
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

    def get(self, entity):
        self._get(entity._uri.reference)


class WriteBatch(_Batch):

    def __init__(self, graph_db):
        _Batch.__init__(self, graph_db)

    def _post(self, uri, body=None):
        self._append(rest.Request(self._graph_db, "POST", uri, body))

    def _delete(self, uri, body=None):
        self._append(rest.Request(self._graph_db, "DELETE", uri, body))

    def _put(self, uri, body=None):
        self._append(rest.Request(self._graph_db, "PUT", uri, body))

    def create_node(self, properties=None):
        """ Create a new node with the properties supplied.
        """
        self._post(self._create_node_uri, compact(properties or {}))

    def create_relationship(self, start_node, type_, end_node, properties=None):
        """ Create a new relationship with the values supplied.
        """
        def node_uri(node):
            if isinstance(node, Node):
                node._must_belong_to(self._graph_db)
                return rest.URI(node.__metadata__["self"], "/node").reference
            else:
                return "{" + str(node) + "}"
        body = {
            "type": type_,
            "to": node_uri(end_node),
        }
        if properties:
            body["data"] = compact(properties)
        self._post(node_uri(start_node) + "/relationships", body)

    def get_or_create_relationship(self, start_node, type_, end_node, properties=None):
        """ Create a new relationship with the values supplied if one does not
            already exist.
        """
        assert isinstance(start_node, Node) or start_node is None
        assert isinstance(end_node, Node) or end_node is None
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

    def delete_node(self, node):
        """ Delete the specified node from the graph.
        """
        assert isinstance(node, Node)
        self._delete(node._uri.reference)

    def delete_relationship(self, relationship):
        """ Delete the specified relationship from the graph.
        """
        assert isinstance(relationship, Relationship)
        self._delete(relationship._uri.reference)

    def set_node_property(self, node, key, value):
        """ Set a single property on a node.
        """
        if value is None:
            self.delete_node_property(node, key)
        else:
            assert isinstance(node, Node)
            uri = rest.URI(node.__metadata__['property'].format(key=quote(key, "")), "/node")
            self._put(uri.reference, value)

    def set_node_properties(self, node, properties):
        """ Replace all properties on a node.
        """
        assert isinstance(node, Node)
        uri = rest.URI(node.__metadata__['properties'], "/node")
        self._put(uri.reference, compact(properties))

    def delete_node_property(self, node, key):
        """ Delete a single property from a node.
        """
        assert isinstance(node, Node)
        uri = rest.URI(node.__metadata__['property'].format(key=quote(key, "")), "/node")
        self._delete(uri.reference)

    def delete_node_properties(self, node):
        """ Delete all properties from a node.
        """
        assert isinstance(node, Node)
        uri = rest.URI(node.__metadata__['properties'], "/node")
        self._delete(uri.reference)

    def set_relationship_property(self, relationship, key, value):
        """ Set a single property on a relationship.
        """
        if value is None:
            self.delete_relationship_property(relationship, key)
        else:
            assert isinstance(relationship, Relationship)
            uri = rest.URI(relationship.__metadata__['property'].format(key=quote(key, "")), "/relationship")
            self._put(uri.reference, value)

    def set_relationship_properties(self, relationship, properties):
        """ Replace all properties on a relationship.
        """
        assert isinstance(relationship, Relationship)
        uri = rest.URI(relationship.__metadata__['properties'], "/relationship")
        self._put(uri.reference, compact(properties))

    def delete_relationship_property(self, relationship, key):
        """ Delete a single property from a relationship.
        """
        assert isinstance(relationship, Relationship)
        uri = rest.URI(relationship.__metadata__['property'].format(key=quote(key, "")), "/relationship")
        self._delete(uri.reference)

    def delete_relationship_properties(self, relationship):
        """ Delete all properties from a relationship.
        """
        assert isinstance(relationship, Relationship)
        uri = rest.URI(relationship.__metadata__['properties'], "/relationship")
        self._delete(uri.reference)

    def _node_uri(self, node):
        if isinstance(node, Node):
            return str(node._uri)
        else:
            return "{" + str(node) + "}"

    def _relationship_uri(self, relationship):
        if isinstance(relationship, Relationship):
            return str(relationship._uri)
        else:
            return "{" + str(relationship) + "}"

    def _index(self, content_type, index):
        if isinstance(index, Index):
            assert content_type == index._content_type
            return index
        else:
            return self._graph_db.get_or_create_index(content_type, str(index))

    def _create_indexed_node(self, index, uri_suffix, key, value, properties):
        index_uri = self._index(Node, index)._uri
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
        index_uri = self._index(Node, index)._uri
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
        index_uri = self._index(Node, index)._uri
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
        index_uri = self._index(Relationship, index)._uri
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
        index_uri = self._index(Relationship, index)._uri
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
        index_uri = self._index(Relationship, index)._uri
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
    :param metadata: optional resource metadata
    """

    def __init__(self, uri=None, metadata=None):
        uri = uri or DEFAULT_URI
        rest.Resource.__init__(self, uri, "/", metadata=metadata)
        rs = self._send(rest.Request(self, "GET", self._uri))
        _assert_expected_response(self.__class__, self._uri, rs.body)
        self._update_metadata(rs.body)
        # force URI adjustment (in case supplied without trailing slash)
        self._uri = rest.URI(rs.uri, "/")
        self._extensions = self.__metadata__.get('extensions', None)
        self._neo4j_version = self.__metadata__.get('neo4j_version', "1.4")
        self._batch_uri = self.__metadata__.get('batch', self._uri.base + "/batch")
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
                rel = Relationship(uri, graph_db=self, metadata=data)
                return rel
            else:
                node = Node(uri, graph_db=self, metadata=data)
                return node
        elif isinstance(data, dict) and "length" in data and \
             "nodes" in data and "relationships" in data and \
             "start" in data and "end" in data:
            # is a path
            nodes = map(Node, data["nodes"], [self] * len(data["nodes"]))
            rels = map(Relationship, data["relationships"], [self] * len(data["relationships"]))
            return Path(*round_robin(nodes, rels))
        elif isinstance(data, dict) and "columns" in data and "data" in data:
            # is a value contained within a Cypher response
            # (should only ever be single row, single value)
            assert len(data["columns"]) == 1
            rows = data["data"]
            assert len(rows) == 1
            values = rows[0]
            assert len(values) == 1
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
        cypher.execute(self, "START n=node(*), r=rel(*) DELETE r, n", {})

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
        if len(abstracts) == 1 and isinstance(abstracts[0], dict):
            rs = self._send(
                rest.Request(self, "POST", self.__metadata__["node"], compact(abstracts[0]))
            )
            return [Node(rs.body["self"], graph_db=self)]
        batch = WriteBatch(self)
        for abstract in abstracts:
            if isinstance(abstract, dict):
                batch.create_node(abstract)
            else:
                if 3 <= len(abstract) <= 4:
                    batch.create_relationship(*abstract)
                else:
                    raise TypeError(abstract)
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
        """Fetch the reference node for the current graph.

        .. deprecated:: 1.3.1
            use indexed nodes instead.
        """
        return Node(self.__metadata__['reference_node'], graph_db=self)

    @deprecated("GraphDatabaseService.get_or_create_relationships is "
                "deprecated, please use either WriteBatch."
                "get_or_create_relationship or Path.get_or_create instead.")
    def get_or_create_relationships(self, *abstracts):
        """ Fetch or create relationships with the specified criteria depending
        on whether or not such relationships exist. Each relationship
        descriptor should be a tuple of (start, type, end) or (start, type,
        end, data) where start and end are either existing :py:class:`Node`
        instances or :py:const:`None` (both nodes cannot be :py:const:`None`)::

            # set up three nodes
            alice, bob, carol = graph_db.create(
                {"name": "Alice"}, {"name": "Bob"}, {"name": "Carol"}
            )

            # ensure Alice and Bob and related
            ab, = graph_db.get_or_create_relationships(
                (alice, "LOVES", bob, {"since": 2006})
            )

            # ensure relationships exist between Alice, Bob and Carol
            # creating new relationships only where necessary
            rels = graph_db.get_or_create_relationships(
                (alice, "LOVES", bob), (bob, "LIKES", alice),
                (carol, "LOVES", bob), (alice, "HATES", carol),
            )

            # ensure Alice has an outgoing LIKES relationship
            # (a new node will be created if required)
            friendship, = graph_db.get_or_create_relationships(
                (alice, "LIKES", None)
            )

            # ensure Alice has an incoming LIKES relationship
            # (a new node will be created if required)
            friendship, = graph_db.get_or_create_relationships(
                (None, "LIKES", alice)
            )

        Uses Cypher `CREATE UNIQUE` clause, raising
        :py:class:`NotImplementedError` if server support not available.
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
                "an unsupported property type".format(self._uri)
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
            batch.get(entity)
        return [rs.body["data"] for rs in batch._submit()]

    def get_relationship_types(self):
        """ Fetch a list of relationship type names currently defined within
        this database instance.
        """
        return self._send(
            rest.Request(self,"GET", self.__metadata__['relationship_types'])
        ).body

    def match(self, start_node=None, type=None, end_node=None, bidirectional=False, limit=None):
        """ Fetch all relationships which match a specific set of criteria.

        :param start_node: start :py:class:`Node` to match
        :param type: type of the relationships to match
        :param end_node: end :py:class:`Node` to match
        :param bidirectional: :py:const:`True` if reversed relationships should
            also be included
        :param limit: maximum number of relationships to match
        """
        if start_node is None and end_node is None:
            query = "START a=node(*)"
            params = {}
        elif end_node is None:
            query = "START a=node({A})"
            params = {"A": start_node._id}
        elif start_node is None:
            query = "START b=node({B})"
            params = {"B": end_node._id}
        else:
            query = "START a=node({A}),b=node({B})"
            params = {"A": start_node._id, "B": end_node._id}
        if type is None:
            if bidirectional:
                query += " MATCH (a)-[r]-(b) RETURN r"
            else:
                query += " MATCH (a)-[r]->(b) RETURN r"
        else:
            if bidirectional:
                query += " MATCH (a)-[r:`" + str(type) + "`]-(b) RETURN r"
            else:
                query += " MATCH (a)-[r:`" + str(type) + "`]->(b) RETURN r"
        if limit is not None:
            query += " LIMIT {0}".format(int(limit))
        data, metadata = cypher.execute(self, query, params)
        return [row[0] for row in data]

    def match_one(self, start_node=None, type=None, end_node=None, bidirectional=False):
        """ Fetch a single relationship which matches a specific set of
        criteria.

        :param start_node: start :py:class:`Node` to match
        :param type: type of the relationships to match
        :param end_node: end :py:class:`Node` to match
        :param bidirectional: :py:const:`True` if reversed relationships should
            also be included
        """
        rels = self.match(start_node, type, end_node, bidirectional, 1)
        if rels:
            return rels[0]
        else:
            return None

    @property
    def neo4j_version(self):
        """ Return the database software version as a tuple.
        """
        return version_tuple(self._neo4j_version)

    def node(self, id):
        """ Fetch a node by ID.
        """
        return Node(self.__metadata__['node'] + "/" + str(id), graph_db=self)

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
        uri = "{0}/relationship/{1}".format(self._uri.base, id)
        return Relationship(uri, graph_db=self)

    def size(self):
        """ Fetch the number of relationships in this graph.
        """
        data, metadata = cypher.execute(self, "START r=rel(*) RETURN count(r)")
        if data and data[0]:
            return data[0][0]
        else:
            raise EnvironmentError("Unable to count relationships")

    def get_indexes(self, type):
        """ Fetch a dictionary of all available indexes of a given type.
        """
        if type == Node:
            rq = rest.Request(self, "GET", self.__metadata__['node_index'])
        elif type == Relationship:
            rq = rest.Request(self, "GET", self.__metadata__['relationship_index'])
        else:
            raise ValueError(type)
        rs = self._send(rq)
        indexes = rs.body or {}
        self._indexes[type] = dict(
            (index, Index(type, indexes[index]['template'], graph_db=self))
            for index in indexes
        )
        return self._indexes[type]

    def get_index(self, type, name):
        """ Fetch a specific index from the current database, returning an
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

    def get_or_create_index(self, type, name, config=None):
        """ Fetch a specific index from the current database, returning an
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
        if type == Node:
            uri = self.__metadata__['node_index']
        elif type == Relationship:
            uri = self.__metadata__['relationship_index']
        else:
            raise ValueError(type)
        config = config or {}
        rs = self._send(rest.Request(self, "POST", uri, {"name": name, "config": config}))
        index = Index(type, rs.body["template"], graph_db=self)
        self._indexes[type].update({name: index})
        return index

    def delete_index(self, type, name):
        """ Delete the entire index identified by the type and name supplied.
        """
        if name not in self._indexes[type]:
            self.get_indexes(type)
        if name in self._indexes[type]:
            index = self._indexes[type][name]
            self._send(rest.Request(self, "DELETE", index._uri))
            del self._indexes[type][name]
            return True
        else:
            return False

    def get_indexed_node(self, index, key, value):
        """ Fetch the first node indexed with the specified details, returning
        :py:const:`None` if none found.
        """
        index = self.get_index(Node, index)
        if index:
            nodes = index.get(key, value)
            if nodes:
                return nodes[0]
        return None

    def get_or_create_indexed_node(self, index, key, value, properties=None):
        """ Fetch the first node indexed with the specified details, creating
        and returning a new indexed node if none found.
        """
        index = self.get_or_create_index(Node, index)
        return index.get_or_create(key, value, properties or {})

    def get_indexed_relationship(self, index, key, value):
        """ Fetch the first relationship indexed with the specified details,
        returning :py:const:`None` if none found.
        """
        index = self.get_index(Relationship, index)
        if index:
            relationships = index.get(key, value)
            if relationships:
                return relationships[0]
        return None


class PropertyContainer(rest.Resource):
    """ Base class from which :py:class:`Node` and :py:class:`Relationship`
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
        """ Create container for properties with caching capabilities.

        :param uri: URI identifying this resource
        :param graph_db: graph database to which this resource belongs
        :param metadata: resource metadata
        """
        rest.Resource.__init__(self, uri, reference_marker, metadata=metadata)
        if graph_db is not None:
            self._must_belong_to(graph_db)
            self._graph_db = graph_db
        else:
            self._graph_db = GraphDatabaseService(self._uri.base + "/")

    def __contains__(self, key):
        return key in self.get_properties()

    def __delitem__(self, key):
        try:
            self._send(rest.Request(self._graph_db, "DELETE", self.__metadata__['property'].format(key=quote(key, ""))))
        except rest.ResourceNotFound:
            pass

    def __getitem__(self, key):
        try:
            return self._send(
                rest.Request(self._graph_db, "GET", self.__metadata__['property'].format(key=quote(key, "")))
            ).body
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
            self._send(
                rest.Request(self._graph_db, "PUT", self.__metadata__['property'].format(key=quote(key, "")), value)
            )

    def _must_belong_to(self, graph_db):
        """ Raise a :py:error:`ValueError` if this entity does not belong to
        the graph supplied.
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
        """ Fetch all properties for this resource.
        """
        rs = self._send(
            rest.Request(self._graph_db, "GET", self.__metadata__['properties'])
        )
        if rs.body:
            return rs.body
        else:
            return {}

    def set_properties(self, properties):
        """ Replace all properties for this resource with the supplied
        dictionary of values.
        """
        self._send(rest.Request(
            self._graph_db, "PUT", self.__metadata__['properties'], compact(properties)
        ))

    def delete_properties(self):
        """ Delete all properties for this resource.
        """
        self._send(rest.Request(
            self._graph_db, "DELETE", self.__metadata__['properties']
        ))


class Node(PropertyContainer):
    """ A node within a graph, identified by a URI.
    
    :param uri: URI identifying this node
    :param graph_db: the graph database to which this node belongs
    :param metadata: resource metadata
    """

    def __init__(self, uri, graph_db=None, metadata=None):
        PropertyContainer.__init__(self, uri, "/node", graph_db=graph_db, metadata=metadata)
        self._id = int('0' + uri.rpartition('/')[-1])

    def __repr__(self):
        return "{0}('{1}')".format(
            self.__class__.__name__,
            repr(self._uri)
        )

    def __str__(self):
        """ Return a human-readable string representation of this node
        object, e.g.:
        
            >>> print str(my_node)
            '(42)'
        """
        return "({0})".format(self._id)

    @property
    def id(self):
        """ Return the unique id for this node.
        """
        return self._id

    def exists(self):
        """ Determine whether this node still exists in the database.
        """
        try:
            self._send(rest.Request(self._graph_db, "GET", self.__metadata__['self']))
            return True
        except rest.ResourceNotFound:
            return False

    def update_properties(self, properties):
        """ Update the properties for this node with the values supplied.
        """
        batch = WriteBatch(self._graph_db)
        for key, value in properties.items():
            if value is None:
                batch.delete_node_property(self, key)
            else:
                batch.set_node_property(self, key, value)
        batch._submit()

    @deprecated("Node.create_relationship_from is deprecated, please use "
                "Node.create_path instead.")
    def create_relationship_from(self, other_node, type, properties=None):
        """ Create and return a new relationship of type `type` from the node
        represented by `other_node` to the node represented by the current
        instance.
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
        """
        if not isinstance(other_node, Node):
            return TypeError("End node is not a neo4j.Node instance")
        rs = self._send(rest.Request(self._graph_db, "POST", self.__metadata__['create_relationship'], {
            'to': str(other_node._uri),
            'type': type,
            'data': compact(properties or {})
        }))
        return Relationship(rs.body["self"])

    def delete(self):
        """ Delete this node from the database.
        """
        self._send(rest.Request(self._graph_db, "DELETE", self.__metadata__['self']))

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
        """
        if types:
            uri = self._typed_relationships_uri(direction, types)
        else:
            uri = self._relationships_uri(direction)
        return [
            Node(rel['start'] if rel['end'] == self._uri else rel['end'], graph_db=self._graph_db)
            for rel in self._send(rest.Request(self._graph_db, "GET", uri)).body
        ]

    @deprecated("Node.get_relationships is deprecated, please use "
                "Node.match instead.")
    def get_relationships(self, direction=Direction.EITHER, *types):
        """ Fetch all relationships from the current node in a given
        `direction` of a specific `type` (if supplied).
        """
        if types:
            uri = self._typed_relationships_uri(direction, types)
        else:
            uri = self._relationships_uri(direction)
        return [
            Relationship(rel['self'], graph_db=self._graph_db)
            for rel in self._send(rest.Request(self._graph_db, "GET", uri)).body
        ]

    @deprecated("Node.get_relationships_with is deprecated, please use "
                "Node.match instead.")
    def get_relationships_with(self, other, direction=Direction.EITHER, *types):
        """ Return all relationships between this node and another node using
        the relationship criteria supplied.
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
        query = query.format(self.id, other.id, type)
        data, metadata = cypher.execute(self._graph_db, query)
        return [row[0] for row in data]

    @deprecated("Node.get_single_related_node is deprecated, please use "
                "Node.match_one instead.")
    def get_single_related_node(self, direction=Direction.EITHER, *types):
        """ Return only one node related to the current node by a relationship
        in the given `direction` of the specified `type`, if any such
        relationships exist.
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
        """
        relationships = self.get_relationships(direction, *types)
        return bool(relationships)

    @deprecated("Node.has_relationship_with is deprecated, please use "
                "Node.match_one instead.")
    def has_relationship_with(self, other, direction=Direction.EITHER, *types):
        """ Return :py:const:`True` if this node has any relationships with the
        specified criteria, :py:const:`False` otherwise.
        """
        relationships = self.get_relationships_with(other, direction, *types)
        return bool(relationships)

    @deprecated("Node.is_related_to is deprecated, please use "
                "Node.match_one instead.")
    def is_related_to(self, other, direction=Direction.EITHER, *types):
        """ Return :py:const:`True` if the current node is related to the other
        node using the relationship criteria supplied, :py:const:`False`
        otherwise.
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

    def match(self, type=None, end_node=None, bidirectional=False, limit=None):
        return self._graph_db.match(self, type, end_node, bidirectional, limit)

    def match_one(self, type=None, end_node=None, bidirectional=False):
        return self._graph_db.match(self, type, end_node, bidirectional)

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


class Relationship(PropertyContainer):
    """ A relationship within a graph, identified by a URI.
    
    :param uri: URI identifying this relationship
    :param graph_db: the graph database to which this relationship belongs
    :param metadata: resource metadata
    """

    def __init__(self, uri, graph_db=None, metadata=None):
        PropertyContainer.__init__(self, uri, "/relationship", graph_db=graph_db, metadata=metadata)
        self._type = None
        self._start_node = None
        self._end_node = None
        self._id = int('0' + uri.rpartition('/')[-1])

    def __repr__(self):
        return "{0}('{1}')".format(
            self.__class__.__name__,
            repr(self._uri)
        )

    def __str__(self):
        """ Return an ASCII art representation of this relationship, e.g.:
        
            >>> print str(my_rel)
            '[23:KNOWS]'
    
        """
        return "[{0}:{1}]".format(self.id, self.type)

    def exists(self):
        """ Determine whether this relationship still exists in the database.
        """
        try:
            self._send(rest.Request(self._graph_db, "GET", self.__metadata__['self']))
            return True
        except rest.ResourceNotFound:
            return False

    def update_properties(self, properties):
        """ Update the properties for this relationship with the values
        supplied.
        """
        batch = WriteBatch(self._graph_db)
        for key, value in properties.items():
            if value is None:
                batch.delete_relationship_property(self, key)
            else:
                batch.set_relationship_property(self, key, value)
        batch._submit()

    def delete(self):
        """ Delete this relationship from the database.
        """
        self._send(rest.Request(self._graph_db, "DELETE", self.__metadata__['self']))

    @property
    def end_node(self):
        """ Return the end node of this relationship.
        """
        if not self._end_node:
            self._end_node = Node(self.__metadata__['end'], graph_db=self._graph_db)
        return self._end_node

    def other_node(self, node):
        """ Return a node object representing the node within this
        relationship which is not the one supplied.
        """
        if self.__metadata__['end'] == node._uri:
            return self.start_node
        else:
            return self.end_node

    @property
    def id(self):
        """ Return the unique id for this relationship.
        """
        return self._id

    def is_type(self, type):
        """ Return :py:const:`True` if this relationship is of the given type.
        """
        return self.type == type

    @property
    def nodes(self):
        """ Return a tuple of the two nodes attached to this relationship.
        """
        return self.start_node, self.end_node

    @property
    def start_node(self):
        """ Return the start node of this relationship.
        """
        if not self._start_node:
            self._start_node = Node(self.__metadata__['start'], graph_db=self._graph_db)
        return self._start_node

    @property
    def type(self):
        """ Return the type of this relationship.
        """
        if not self._type:
            self._type = self.__metadata__['type']
        return self._type


class Index(rest.Resource):
    """ Searchable database index which can contain either nodes or
    relationships.

    .. seealso:: :py:func:`GraphDatabaseService.get_or_create_index`
    """

    def __init__(self, content_type, template_uri, graph_db=None, metadata=None):
        rest.Resource.__init__(
            self, template_uri.rpartition("/{key}/{value}")[0],
            "/index/", metadata=metadata
        )
        self._name = str(self._uri).rpartition("/")[2]
        self._content_type = content_type
        self._template_uri = template_uri
        if graph_db is not None:
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
        self._send(rest.Request(self._graph_db, "POST", str(self._uri), {
            "key": key,
            "value": value,
            "uri": str(entity._uri)
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
        rs = self._send(rest.Request(self._graph_db, "POST", str(self._uri) + "?unique", {
            "key": key,
            "value": value,
            "uri": str(entity._uri)
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
            self._content_type(result['self'], self._graph_db)
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
            assert isinstance(start_node, Node)
            assert isinstance(end_node, Node)
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
                "start": str(abstract[0]._uri),
                "type": abstract[1],
                "end": str(abstract[2]._uri),
                "properties": abstract[3] if len(abstract) > 3 else None
            }
        else:
            raise TypeError(self._content_type)
        return self._send(rest.Request(
            self._graph_db, "POST", str(self._uri) + "?unique", body)
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
        return self._content_type(rs.body["self"], self._graph_db)

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
            return self._content_type(rs.body["self"], self._graph_db)
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
                    self._uri,
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
                    rest.URI(entity, "/index/").reference,
                ))
            batch._submit()
        elif key and entity:
            self._send(rest.Request(
                self._graph_db, "DELETE", "{0}/{1}/{2}".format(
                    self._uri,
                    quote(key, ""),
                    entity._id,
                )
            ))
        elif entity:
            self._send(rest.Request(
                self._graph_db, "DELETE", "{0}/{1}".format(
                    self._uri,
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
            self._content_type(item['self'], self._graph_db)
            for item in self._send(rest.Request(self._graph_db, "GET", "{0}?query={1}".format(
                self._uri, quote(query, "")
            ))).body
        ]


class Path(object):
    """ A representation of a sequence of nodes connected by relationships.
    """

    def __init__(self, node, *rels_and_nodes):
        self._nodes = [node]
        self._nodes.extend(rels_and_nodes[1::2])
        if len(rels_and_nodes) % 2 != 0:
            # If a trailing relationship is supplied, add a dummy end node
            self._nodes.append(None)
        self._relationships = list(rels_and_nodes[0::2])

    def __repr__(self):
        out = ", ".join(repr(item) for item in round_robin(self._nodes, self._relationships))
        return "Path({0})".format(out)

    def __str__(self):
        out = []
        for i, rel in enumerate(self._relationships):
            out.append(str(self._nodes[i]))
            out.append("-")
            out.append(str(rel))
            out.append("->")
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
        def relationship_tuples():
            for i, rel in enumerate(self._relationships):
                yield self._nodes[i], rel, self._nodes[i + 1]
        return iter(relationship_tuples())

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
        return self._nodes

    @property
    def relationships(self):
        """ Return a list of all the relationships which make up this path.
        """
        return self._relationships

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
        left._relationships.append(rel)
        left._nodes.extend(right._nodes)
        left._relationships.extend(right._relationships)
        return left

    def _create(self, graph_db, verb):
        nodes, path, values, params = [], [], [], {}
        def append_node(i, node):
            if isinstance(node, dict):
                path.append("(n{0} {{p{0}}})".format(i))
                params["p{0}".format(i)] = compact(node or {})
            else:
                path.append("(n{0})".format(i))
                if isinstance(node, Node):
                    nodes.append("n{0}=node({{i{0}}})".format(i))
                    params["i{0}".format(i)] = node._id
                elif isinstance(node, int):
                    nodes.append("n{0}=node({{i{0}}})".format(i))
                    params["i{0}".format(i)] = node
                elif isinstance(node, tuple):
                    nodes.append("n{0}=node:{1}(`{2}`={{i{0}}})".format(i, node[0], node[1]))
                    params["i{0}".format(i)] = node[2]
                elif node is not None:
                    raise TypeError("Cannot infer node from {0}".format(type(node)))
            values.append("n{0}".format(i))
        def append_rel(i, rel):
            if isinstance(rel, Relationship):
                path.append("-[r{0}:`{1}`]->".format(i, rel.type))
            elif isinstance(rel, tuple):
                path.append("-[r{0}:`{1}` {{q{0}}}]->".format(i, rel[0]))
                params["q{0}".format(i)] = compact(rel[1] or {})
            else:
                path.append("-[r{0}:`{1}`]->".format(i, rel))
            values.append("r{0}".format(i))
        append_node(0, self.nodes[0])
        for i, (start, rel, end) in enumerate(self):
            append_rel(i, rel)
            append_node(i + 1, end)
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
                "an unsupported property type".format(graph_db._uri)
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
