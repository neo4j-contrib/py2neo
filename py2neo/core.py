#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright 2011-2014, Nigel Small
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

""" Core objects and functionality for py2neo.

authenticate - register authentication details for a host:port
rewrite - register a rewrite hook for a scheme://host:port

Resource - local representation of a remote web resource
ResourceTemplate - template for Resource generation based on a pattern
Service - base class for objects that can be bound to remote resources
ServiceRoot - root resource for a Neo4j server instance
Graph - main graph resource class to bind to a remote graph database service
Schema - schema index and constraint management resource
PropertySet - dict subclass that equates None and missing values for storing properties
LabelSet - set subclass for storing labels
PropertyContainer - base class for Node and Relationship classes
Node - local graph node object that can be bound to a remote Neo4j node
NodePointer - reference to a node object defined elsewhere
Rel - forward relationship without start and end node information
Rev - reverse relationship without start and end node information
Path - local graph path object that represents a remote Neo4j path
Relationship - local graph relationship object that can be bound to a remote Neo4j relationship

"""


from __future__ import division, unicode_literals

import base64
from io import StringIO
import re
from warnings import warn
from weakref import WeakValueDictionary
import webbrowser

from py2neo import __version__
from py2neo.error.client import BindError, JoinError
from py2neo.error.server import GraphError
from py2neo.packages.httpstream import http, Response, ClientError, ServerError, \
    Resource as _Resource, ResourceTemplate as _ResourceTemplate
from py2neo.packages.httpstream.http import JSONResponse
from py2neo.packages.httpstream.numbers import NOT_FOUND
from py2neo.packages.httpstream.packages.urimagic import URI
from py2neo.types import cast_property
from py2neo.util import is_collection, is_integer, round_robin, ustr, version_tuple, \
    raise_from, xstr


__all__ = ["authenticate", "rewrite",
           "Resource", "ResourceTemplate", "Service",
           "ServiceRoot", "Graph", "Schema", "PropertySet", "LabelSet", "PropertyContainer",
           "Node", "NodePointer", "Rel", "Rev", "Path", "Relationship", "Subgraph",
           "ServerPlugin", "UnmanagedExtension"]


DEFAULT_SCHEME = "http"
DEFAULT_HOST = "localhost"
DEFAULT_PORT = 7474
DEFAULT_HOST_PORT = "{0}:{1}".format(DEFAULT_HOST, DEFAULT_PORT)

PRODUCT = ("py2neo", __version__)

NON_ALPHA_NUM = re.compile("[^0-9A-Za-z_]")
SIMPLE_NAME = re.compile(r"[A-Za-z_][0-9A-Za-z_]*")

http.default_encoding = "UTF-8"

_headers = {
    None: [("X-Stream", "true")],
}

_http_rewrites = {}


def _add_header(key, value, host_port=None):
    """ Add an HTTP header to be sent with all requests if no `host_port`
    is provided or only to those matching the value supplied otherwise.
    """
    if host_port in _headers:
        _headers[host_port].append((key, value))
    else:
        _headers[host_port] = [(key, value)]


def _get_headers(host_port):
    """Fetch all HTTP headers relevant to the `host_port` provided.
    """
    uri_headers = {}
    for n, headers in _headers.items():
        if n is None or n == host_port:
            uri_headers.update(headers)
    return uri_headers


def authenticate(host_port, user_name, password):
    """ Set HTTP basic authentication values for specified `host_port`. The
    code below shows a simple example::

        # set up authentication parameters
        neo4j.authenticate("camelot:7474", "arthur", "excalibur")

        # connect to authenticated graph database
        graph = neo4j.Graph("http://camelot:7474/db/data/")

    Note: a `host_port` can be either a server name or a server name and port
    number but must match exactly that used within the Graph
    URI.

    :param host_port: the host and optional port requiring authentication
        (e.g. "bigserver", "camelot:7474")
    :param user_name: the user name to authenticate as
    :param password: the password
    """
    credentials = (user_name + ":" + password).encode("UTF-8")
    value = "Basic " + base64.b64encode(credentials).decode("ASCII")
    _add_header("Authorization", value, host_port=host_port)


def rewrite(from_scheme_host_port, to_scheme_host_port):
    """ Automatically rewrite all URIs directed to the scheme, host and port
    specified in `from_scheme_host_port` to that specified in
    `to_scheme_host_port`.

    As an example::

        # implicitly convert all URIs beginning with <http://localhost:7474>
        # to instead use <https://dbserver:9999>
        neo4j.rewrite(("http", "localhost", 7474), ("https", "dbserver", 9999))

    If `to_scheme_host_port` is :py:const:`None` then any rewrite rule for
    `from_scheme_host_port` is removed.

    This facility is primarily intended for use by database servers behind
    proxies which are unaware of their externally visible network address.
    """
    global _http_rewrites
    if to_scheme_host_port is None:
        try:
            del _http_rewrites[from_scheme_host_port]
        except KeyError:
            pass
    else:
        _http_rewrites[from_scheme_host_port] = to_scheme_host_port


class Resource(_Resource):
    """ Variant of HTTPStream Resource that passes extra headers and product
    detail.
    """

    error_class = GraphError

    def __init__(self, uri, metadata=None):
        uri = URI(uri)
        scheme_host_port = (uri.scheme, uri.host, uri.port)
        if scheme_host_port in _http_rewrites:
            scheme_host_port = _http_rewrites[scheme_host_port]
            # This is fine - it's all my code anyway...
            uri._URI__set_scheme(scheme_host_port[0])
            uri._URI__set_authority("{0}:{1}".format(scheme_host_port[1],
                                                     scheme_host_port[2]))
        if uri.user_info:
            authenticate(uri.host_port, *uri.user_info.partition(":")[0::2])
        self._resource = _Resource.__init__(self, uri)
        #self._subresources = {}
        self.__headers = _get_headers(self.__uri__.host_port)
        self.__base = super(Resource, self)
        if metadata is None:
            self.__initial_metadata = None
        else:
            self.__initial_metadata = dict(metadata)
        self.__last_get_response = None

        uri = uri.string
        service_root_uri = uri[:uri.find("/", uri.find("//") + 2)] + "/"
        if service_root_uri == uri:
            self.__service_root = self
        else:
            self.__service_root = ServiceRoot(service_root_uri)
        self.__relative_uri = NotImplemented

    @property
    def graph(self):
        return self.__service_root.graph

    @property
    def headers(self):
        return self.__headers

    @property
    def metadata(self):
        if self.__last_get_response is None:
            if self.__initial_metadata is not None:
                return self.__initial_metadata
            self.get()
        return self.__last_get_response.content

    @property
    def relative_uri(self):
        if self.__relative_uri is NotImplemented:
            self_uri = self.uri.string
            graph_uri = self.graph.uri.string
            self.__relative_uri = URI(self_uri[len(graph_uri):])
        return self.__relative_uri

    @property
    def service_root(self):
        return self.__service_root

    def get(self, headers=None, redirect_limit=5, **kwargs):
        headers = dict(headers or {})
        headers.update(self.__headers)
        kwargs.update(product=PRODUCT, cache=True)
        try:
            response = self.__base.get(headers=headers, redirect_limit=redirect_limit, **kwargs)
        except (ClientError, ServerError) as error:
            if isinstance(error, JSONResponse):
                content = dict(error.content, request=error.request, response=error)
            else:
                content = {}
            message = content.pop("message", "HTTP GET returned response %s" % error.status_code)
            raise_from(self.error_class(message, **content), error)
        else:
            self.__last_get_response = response
            return response

    def put(self, body=None, headers=None, **kwargs):
        headers = dict(headers or {})
        headers.update(self.__headers)
        kwargs.update(product=PRODUCT)
        try:
            response = self.__base.put(body, headers, **kwargs)
        except (ClientError, ServerError) as error:
            if isinstance(error, JSONResponse):
                content = dict(error.content, request=error.request, response=error)
            else:
                content = {}
            message = content.pop("message", "HTTP PUT returned response %s" % error.status_code)
            raise_from(self.error_class(message, **content), error)
        else:
            return response

    def post(self, body=None, headers=None, **kwargs):
        headers = dict(headers or {})
        headers.update(self.__headers)
        kwargs.update(product=PRODUCT)
        try:
            response = self.__base.post(body, headers, **kwargs)
        except (ClientError, ServerError) as error:
            if isinstance(error, JSONResponse):
                content = dict(error.content, request=error.request, response=error)
            else:
                content = {}
            message = content.pop("message", "HTTP POST returned response %s" % error.status_code)
            raise_from(self.error_class(message, **content), error)
        else:
            return response

    def delete(self, headers=None, **kwargs):
        headers = dict(headers or {})
        headers.update(self.__headers)
        kwargs.update(product=PRODUCT)
        try:
            response = self.__base.delete(headers, **kwargs)
        except (ClientError, ServerError) as error:
            if isinstance(error, JSONResponse):
                content = dict(error.content, request=error.request, response=error)
            else:
                content = {}
            message = content.pop("message", "HTTP DELETE returned response %s" % error.status_code)
            raise_from(self.error_class(message, **content), error)
        else:
            return response


class ResourceTemplate(_ResourceTemplate):

    error_class = GraphError

    def expand(self, **values):
        resource = Resource(self.uri_template.expand(**values))
        resource.error_class = self.error_class
        return resource


class Service(object):
    """ Base class for objects that can be bound to a remote resource.
    """

    error_class = GraphError

    __resource__ = None

    def bind(self, uri, metadata=None):
        """ Bind object to Resource or ResourceTemplate.
        """
        if "{" in uri and "}" in uri:
            if metadata:
                raise ValueError("Initial metadata cannot be passed to a resource template")
            self.__resource__ = ResourceTemplate(uri)
        else:
            self.__resource__ = Resource(uri, metadata)
        self.__resource__.error_class = self.error_class

    @property
    def bound(self):
        """ Returns :const:`True` if bound to a remote resource.
        """
        return self.__resource__ is not None

    @property
    def graph(self):
        return self.service_root.graph

    @property
    def relative_uri(self):
        return self.resource.relative_uri

    @property
    def resource(self):
        """ Returns the :class:`Resource` to which this is bound.
        """
        if self.bound:
            return self.__resource__
        else:
            raise BindError("Local entity is not bound to a remote entity")

    @property
    def service_root(self):
        return self.resource.service_root

    def unbind(self):
        self.__resource__ = None

    @property
    def uri(self):
        if isinstance(self.resource, ResourceTemplate):
            return self.resource.uri_template
        else:
            return self.resource.uri


class ServiceRoot(object):
    """ Neo4j REST API service root resource.
    """

    DEFAULT_URI = "{0}://{1}/".format(DEFAULT_SCHEME, DEFAULT_HOST_PORT)

    __instances = {}

    __graph = None

    def __new__(cls, uri=None):
        if uri is None:
            uri = cls.DEFAULT_URI
        if not uri.endswith("/"):
            uri += "/"
        try:
            inst = cls.__instances[uri]
        except KeyError:
            inst = super(ServiceRoot, cls).__new__(cls)
            inst.__resource = Resource(uri)
            inst.__graph = None
            cls.__instances[uri] = inst
        return inst

    @property
    def graph(self):
        if self.__graph is None:
            self.__graph = Graph(self.resource.metadata["data"])
        return self.__graph

    @property
    def resource(self):
        return self.__resource

    @property
    def uri(self):
        return self.resource.uri


class Graph(Service):
    """ Top-level wrapper around a Neo4j database service identified by
    URI. To connect to a local server on the default URI, simply use::

        >>> from py2neo import Graph
        >>> graph = Graph()

    The server address can also be provided explicitly::

        >>> other_graph = Graph("http://camelot:1138/db/data/")

    If the database server is behind a proxy that requires HTTP
    authorisation,
    this can also be specified within the URI::

        >>> secure_graph = Graph("http://arthur:excalibur@camelot:1138/db/data/")

    Once obtained, the Graph object provides direct or indirect access
    to most of the functionality available within py2neo.

    """

    __instances = {}

    __batch = None
    __cypher = None
    __legacy = None
    __schema = None
    __node_labels = None
    __relationship_types = None

    # Auto-sync will be removed in 2.1
    auto_sync_properties = False

    @staticmethod
    def cast(obj):
        """ Cast an general Python object to a graph-specific entity,
        such as a :class:`.Node` or a :class:`.Relationship`.
        """
        if obj is None:
            return None
        elif isinstance(obj, (Node, NodePointer, Path, Rel, Relationship, Rev)):
            return obj
        elif isinstance(obj, dict):
            return Node.cast(obj)
        elif isinstance(obj, tuple):
            return Relationship.cast(obj)
        else:
            raise TypeError(obj)

    def __new__(cls, uri=None):
        if uri is None:
            uri = ServiceRoot().graph.uri.string
        if not uri.endswith("/"):
            uri += "/"
        key = (cls, uri)
        try:
            inst = cls.__instances[key]
        except KeyError:
            inst = super(Graph, cls).__new__(cls)
            inst.bind(uri)
            cls.__instances[key] = inst
        return inst

    def __repr__(self):
        return "<Graph uri=%r>" % self.uri.string

    def __hash__(self):
        return hash(self.uri)

    def __len__(self):
        return self.size

    def __bool__(self):
        return True

    def __nonzero__(self):
        return True

    def __contains__(self, entity):
        return entity.bound and entity.uri.string.startswith(entity.uri.string)

    @property
    def batch(self):
        """ Batch execution resource for this graph. This attribute will
        generally not be used directly.

        .. seealso::
           :class:`py2neo.batch.BatchResource`
           :class:`py2neo.batch.WriteBatch`

        """
        if self.__batch is None:
            from py2neo.batch import BatchResource
            self.__batch = BatchResource(self.resource.metadata["batch"])
        return self.__batch

    @property
    def cypher(self):
        """ Cypher execution resource for this graph (non-transactional).

        ::

            >>> from py2neo import Graph
            >>> graph = Graph()
            >>> results = graph.cypher.execute("MATCH (n:Person) RETURN n")
            >>> next(results)
            (n7890:Person {name:'Alice'})

        .. seealso::
           :class:`py2neo.cypher.CypherResource`

        """
        if self.__cypher is None:
            from py2neo.cypher import CypherResource
            metadata = self.resource.metadata
            self.__cypher = CypherResource(metadata["cypher"], metadata.get("transaction"))
        return self.__cypher

    def create(self, *entities):
        """ Create multiple nodes, relationships and/or paths in a
        single transaction.

        .. warning::
            This method will *always* return a tuple, even when creating
            only a single entity. To automatically unpack to a single
            item, append a trailing comma to the variable name on the
            left of the assignment operation.

        """
        from py2neo.cypher.create import CreateStatement
        statement = CreateStatement(self)
        for entity in entities:
            statement.create(entity)
        return statement.execute()

    def create_unique(self, *entities):
        """ Create one or more unique paths or relationships in a
        single transaction.
        """
        from py2neo.cypher.create import CreateStatement
        statement = CreateStatement(self)
        for entity in entities:
            statement.create_unique(entity)
        return statement.execute()

    def delete(self, *entities):
        """ Delete one or more nodes, relationships and/or paths.
        """
        from py2neo.cypher.delete import DeleteStatement
        statement = DeleteStatement(self)
        for entity in entities:
            statement.delete(entity)
        return statement.execute()

    def delete_all(self):
        """ Delete all nodes and relationships from the graph.

        .. warning::
            This method will permanently remove **all** nodes and relationships
            from the graph and cannot be undone.
        """
        from py2neo.batch import WriteBatch, CypherJob
        batch = WriteBatch(self)
        batch.append(CypherJob("START r=rel(*) DELETE r"))
        batch.append(CypherJob("START n=node(*) DELETE n"))
        batch.run()

    def find(self, label, property_key=None, property_value=None):
        """ Iterate through a set of labelled nodes, optionally filtering
        by property key and value
        """
        if not label:
            raise ValueError("Empty label")
        from py2neo.cypher.lang import cypher_escape
        if property_key is None:
            statement = "MATCH (n:%s) RETURN n,labels(n)" % cypher_escape(label)
            response = self.cypher.post(statement)
        else:
            statement = "MATCH (n:%s {%s:{v}}) RETURN n,labels(n)" % (
                cypher_escape(label), cypher_escape(property_key))
            response = self.cypher.post(statement, {"v": property_value})
        for record in response.content["data"]:
            dehydrated = record[0]
            dehydrated.setdefault("metadata", {})["labels"] = record[1]
            yield self.hydrate(dehydrated)
        response.close()

    def hydrate(self, data):
        """ Hydrate a dictionary of data into a Node, Relationship or other
        graph object.
        """
        if isinstance(data, dict):
            if "self" in data:
                if "type" in data:
                    return Relationship.hydrate(data)
                else:
                    return Node.hydrate(data)
            elif "nodes" in data and "relationships" in data:
                if "directions" not in data:
                    from py2neo.batch import Job, Target
                    node_uris = data["nodes"]
                    relationship_uris = data["relationships"]
                    jobs = [Job("GET", Target(uri)) for uri in relationship_uris]
                    directions = []
                    for i, result in enumerate(self.batch.submit(jobs)):
                        rel_data = result.content
                        start = rel_data["start"]
                        end = rel_data["end"]
                        if start == node_uris[i] and end == node_uris[i + 1]:
                            directions.append("->")
                        else:
                            directions.append("<-")
                    data["directions"] = directions
                return Path.hydrate(data)
            elif "columns" in data and "data" in data:
                from py2neo.cypher import RecordList
                return RecordList.hydrate(data, self)
            elif "neo4j_version" in data:
                return self
            elif "exception" in data and "stacktrace" in data:
                message = data.pop("message", "The server returned an error")
                raise GraphError(message, **data)
            else:
                warn("Map literals returned over the Neo4j REST interface are ambiguous "
                     "and may be hydrated as graph objects")
                return data
        elif is_collection(data):
            return type(data)(map(self.hydrate, data))
        else:
            return data

    @property
    def legacy(self):
        """ Sub-resource providing access to legacy functionality.
        """
        if self.__legacy is None:
            from py2neo.legacy import LegacyResource
            self.__legacy = LegacyResource(self.uri.string)
        return self.__legacy

    def match(self, start_node=None, rel_type=None, end_node=None, bidirectional=False, limit=None):
        """ Iterate through all relationships matching specified criteria.

        Examples are as follows::

            # all relationships from the graph database
            # ()-[r]-()
            rels = list(graph.match())

            # all relationships outgoing from `alice`
            # (alice)-[r]->()
            rels = list(graph.match(start_node=alice))

            # all relationships incoming to `alice`
            # ()-[r]->(alice)
            rels = list(graph.match(end_node=alice))

            # all relationships attached to `alice`, regardless of direction
            # (alice)-[r]-()
            rels = list(graph.match(start_node=alice, bidirectional=True))

            # all relationships from `alice` to `bob`
            # (alice)-[r]->(bob)
            rels = list(graph.match(start_node=alice, end_node=bob))

            # all relationships outgoing from `alice` of type "FRIEND"
            # (alice)-[r:FRIEND]->()
            rels = list(graph.match(start_node=alice, rel_type="FRIEND"))

            # up to three relationships outgoing from `alice` of type "FRIEND"
            # (alice)-[r:FRIEND]->()
            rels = list(graph.match(start_node=alice, rel_type="FRIEND", limit=3))

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
        :return: matching relationships
        :rtype: generator
        """
        if start_node is None and end_node is None:
            query = "START a=node(*)"
            params = {}
        elif end_node is None:
            query = "START a=node({A})"
            start_node = Node.cast(start_node)
            if not start_node.bound:
                raise TypeError("Nodes for relationship match end points must be bound")
            params = {"A": start_node}
        elif start_node is None:
            query = "START b=node({B})"
            end_node = Node.cast(end_node)
            if not end_node.bound:
                raise TypeError("Nodes for relationship match end points must be bound")
            params = {"B": end_node}
        else:
            query = "START a=node({A}),b=node({B})"
            start_node = Node.cast(start_node)
            end_node = Node.cast(end_node)
            if not start_node.bound or not end_node.bound:
                raise TypeError("Nodes for relationship match end points must be bound")
            params = {"A": start_node, "B": end_node}
        if rel_type is None:
            rel_clause = ""
        elif is_collection(rel_type):
            separator = "|:" if self.neo4j_version >= (2, 0, 0) else "|"
            rel_clause = ":" + separator.join("`{0}`".format(_)
                                              for _ in rel_type)
        else:
            rel_clause = ":`{0}`".format(rel_type)
        if bidirectional:
            query += " MATCH (a)-[r" + rel_clause + "]-(b) RETURN r"
        else:
            query += " MATCH (a)-[r" + rel_clause + "]->(b) RETURN r"
        if limit is not None:
            query += " LIMIT {0}".format(int(limit))
        results = self.cypher.stream(query, params)
        try:
            for result in results:
                yield result.r
        finally:
            results.close()

    def match_one(self, start_node=None, rel_type=None, end_node=None, bidirectional=False):
        """ Fetch a single relationship matching specified criteria.

        :param start_node: concrete start :py:class:`Node` to match or
            :py:const:`None` if any
        :param rel_type: type of relationships to match or :py:const:`None` if
            any
        :param end_node: concrete end :py:class:`Node` to match or
            :py:const:`None` if any
        :param bidirectional: :py:const:`True` if reversed relationships should
            also be included
        :return: a matching :py:class:`Relationship` or :py:const:`None`

        .. seealso::
           :py:func:`Graph.match <py2neo.neo4j.Graph.match>`
        """
        rels = list(self.match(start_node, rel_type, end_node,
                               bidirectional, 1))
        if rels:
            return rels[0]
        else:
            return None

    def merge(self, label, property_key=None, property_value=None):
        """ Match or create a node by label and optional property and return
        all matching nodes.
        """
        if not label:
            raise ValueError("Empty label")
        from py2neo.cypher.lang import cypher_escape
        if property_key is None:
            statement = "MERGE (n:%s) RETURN n,labels(n)" % cypher_escape(label)
            response = self.cypher.post(statement)
        else:
            statement = "MERGE (n:%s {%s:{v}}) RETURN n,labels(n)" % (
                cypher_escape(label), cypher_escape(property_key))
            response = self.cypher.post(statement, {"v": property_value})
        for record in response.content["data"]:
            dehydrated = record[0]
            dehydrated.setdefault("metadata", {})["labels"] = record[1]
            yield self.hydrate(dehydrated)
        response.close()

    def merge_one(self, label, property_key=None, property_value=None):
        """ Match or create a node by label and optional property and return a
        single matching node. This method is intended to be used with a unique
        constraint and does not fail if more than one matching node is found.
        """
        for node in self.merge(label, property_key, property_value):
            return node

    @property
    def neo4j_version(self):
        """ The database software version as a 4-tuple of (``int``, ``int``,
        ``int``, ``str``).
        """
        return version_tuple(self.resource.metadata["neo4j_version"])

    def node(self, id_):
        """ Fetch a node by ID. This method creates an object representing the
        remote node with the ID specified but fetches no data from the server.
        For this reason, there is no guarantee that the entity returned
        actually exists.
        """
        resource = self.resource.resolve("node/%s" % id_)
        uri_string = resource.uri.string
        try:
            return Node.cache[uri_string]
        except KeyError:
            data = {"self": uri_string}
            return Node.cache.setdefault(uri_string, Node.hydrate(data))

    @property
    def node_labels(self):
        """ The set of node labels currently defined within the graph.
        """
        if not self.supports_node_labels:
            raise NotImplementedError("Node labels not available for this Neo4j server version")
        if self.__node_labels is None:
            self.__node_labels = Resource(self.uri.string + "labels")
        return frozenset(self.__node_labels.get().content)

    def open_browser(self):
        """ Open a page in the default system web browser pointing at
        the Neo4j browser application for this Graph.
        """
        webbrowser.open(self.service_root.resource.uri.string)

    @property
    def order(self):
        """ The number of nodes in this graph.
        """
        return self.cypher.execute_one("START n=node(*) RETURN count(n)")

    def pull(self, *entities):
        """ Update one or more local entities by pulling data from
        their remote counterparts.
        """
        if entities:
            from py2neo.batch.pull import PullBatch
            batch = PullBatch(self)
            for entity in entities:
                batch.append(entity)
            batch.pull()

    def push(self, *entities):
        """ Update one or more remote entities by pushing data from
        their local counterparts.
        """
        if entities:
            from py2neo.batch.push import PushBatch
            batch = PushBatch(self)
            for entity in entities:
                batch.append(entity)
            batch.push()

    def relationship(self, id_):
        """ Fetch a relationship by ID.
        """
        resource = self.resource.resolve("relationship/" + str(id_))
        uri_string = resource.uri.string
        try:
            return Relationship.cache[uri_string]
        except KeyError:
            try:
                return Relationship.cache.setdefault(
                    uri_string, Relationship.hydrate(resource.get().content))
            except ClientError:
                raise ValueError("Relationship with ID %s not found" % id_)

    @property
    def relationship_types(self):
        """ The set of relationship types currently defined within the graph.
        """
        if self.__relationship_types is None:
            self.__relationship_types = Resource(self.uri.string + "relationship/types")
        return frozenset(self.__relationship_types.get().content)

    @property
    def schema(self):
        """ The Schema resource for this graph.

        .. seealso::
            :py:func:`Schema <py2neo.neo4j.Schema>`
        """
        if self.__schema is None:
            self.__schema = Schema(self.uri.string + "schema")
        return self.__schema

    @property
    def size(self):
        """ The number of relationships in this graph.
        """
        return self.cypher.execute_one("START r=rel(*) RETURN count(r)")

    @property
    def supports_cypher_transactions(self):
        """ Indicates whether the server supports explicit Cypher transactions.
        """
        return "transaction" in self.resource.metadata

    @property
    def supports_foreach_pipe(self):
        """ Indicates whether the server supports pipe syntax for FOREACH.
        """
        return self.neo4j_version >= (2, 0)

    @property
    def supports_node_labels(self):
        """ Indicates whether the server supports node labels.
        """
        return self.neo4j_version >= (2, 0)

    @property
    def supports_optional_match(self):
        """ Indicates whether the server supports Cypher OPTIONAL MATCH
        clauses.
        """
        return self.neo4j_version >= (2, 0)

    @property
    def supports_schema_indexes(self):
        """ Indicates whether the server supports schema indexes.
        """
        return self.neo4j_version >= (2, 0)


class Schema(Service):

    __instances = {}

    def __new__(cls, uri):
        try:
            inst = cls.__instances[uri]
        except KeyError:
            inst = super(Schema, cls).__new__(cls)
            inst.bind(uri)
            if not inst.graph.supports_schema_indexes:
                raise NotImplementedError("Schema index support requires version 2.0 or above")
            inst._index_template = ResourceTemplate(uri + "/index/{label}")
            inst._index_key_template = ResourceTemplate(uri + "/index/{label}/{property_key}")
            inst._uniqueness_constraint_template = \
                ResourceTemplate(uri + "/constraint/{label}/uniqueness")
            inst._uniqueness_constraint_key_template = \
                ResourceTemplate(uri + "/constraint/{label}/uniqueness/{property_key}")
            cls.__instances[uri] = inst
        return inst

    def create_index(self, label, property_key):
        """ Index a property key for a label.
        """
        self._index_template.expand(label=label).post({"property_keys": [property_key]})

    def create_unique_constraint(self, label, property_key):
        """ Create an uniqueness constraint for a label.
        """
        self._uniqueness_constraint_template.expand(label=label).post(
            {"property_keys": [property_key]})

    def drop_index(self, label, property_key):
        """ Remove label index for a given property key.
        """
        try:
            self._index_key_template.expand(label=label, property_key=property_key).delete()
        except GraphError as error:
            cause = error.__cause__
            if isinstance(cause, Response):
                if cause.status_code == NOT_FOUND:
                    raise GraphError("No such schema index (label=%r, key=%r)" % (
                        label, property_key))
            raise

    def drop_unique_constraint(self, label, property_key):
        """ Remove uniqueness constraint for a given property key.
        """
        try:
            self._uniqueness_constraint_key_template.expand(
                label=label, property_key=property_key).delete()
        except GraphError as error:
            cause = error.__cause__
            if isinstance(cause, Response):
                if cause.status_code == NOT_FOUND:
                    raise GraphError("No such unique constraint (label=%r, key=%r)" % (
                        label, property_key))
            raise

    def get_indexes(self, label):
        """ Fetch a list of indexed property keys for a label.
        """
        return [
            indexed["property_keys"][0]
            for indexed in self._index_template.expand(label=label).get().content
        ]

    def get_unique_constraints(self, label):
        """ Fetch a list of unique constraints for a label.
        """
        return [
            unique["property_keys"][0]
            for unique in self._uniqueness_constraint_template.expand(label=label).get().content
        ]


class PropertySet(Service, dict):
    """ A dict subclass that equates None with a non-existent key and can be
    bound to a remote *properties* resource.
    """

    def __init__(self, iterable=None, **kwargs):
        Service.__init__(self)
        dict.__init__(self)
        self.update(iterable, **kwargs)

    def __eq__(self, other):
        if not isinstance(other, PropertySet):
            other = PropertySet(other)
        return dict.__eq__(self, other)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        x = 0
        for key, value in self.items():
            if isinstance(value, list):
                x ^= hash((key, tuple(value)))
            else:
                x ^= hash((key, value))
        return x

    def __getitem__(self, key):
        return dict.get(self, key)

    def __setitem__(self, key, value):
        if value is None:
            try:
                dict.__delitem__(self, key)
            except KeyError:
                pass
        else:
            dict.__setitem__(self, key, cast_property(value))

    def pull(self):
        """ Copy the set of remote properties onto the local set.
        """
        self.resource.get()
        properties = self.resource.metadata
        self.replace(properties or {})

    def push(self):
        """ Copy the set of local properties onto the remote set.
        """
        self.resource.put(self)

    def replace(self, iterable=None, **kwargs):
        self.clear()
        self.update(iterable, **kwargs)

    def setdefault(self, key, default=None):
        if key in self:
            value = self[key]
        elif default is None:
            value = None
        else:
            value = dict.setdefault(self, key, default)
        return value

    def update(self, iterable=None, **kwargs):
        if iterable:
            try:
                for key in iterable.keys():
                    self[key] = iterable[key]
            except (AttributeError, TypeError):
                for key, value in iterable:
                    self[key] = value
        for key in kwargs:
            self[key] = kwargs[key]


class LabelSet(Service, set):
    """ A set subclass that can be bound to a remote *labels* resource.
    """

    def __init__(self, iterable=None):
        Service.__init__(self)
        set.__init__(self)
        if iterable:
            self.update(iterable)

    def __eq__(self, other):
        if not isinstance(other, LabelSet):
            other = LabelSet(other)
        return set.__eq__(self, other)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        value = 0
        for label in self:
            value ^= hash(label)
        return value

    def pull(self):
        """ Copy the set of remote labels onto the local set.
        """
        self.resource.get()
        labels = self.resource.metadata
        self.replace(labels or [])

    def push(self):
        """ Copy the set of local labels onto the remote set.
        """
        self.resource.put(self)

    def replace(self, iterable):
        self.clear()
        self.update(iterable)


class PropertyContainer(Service):
    """ Base class for objects that contain a set of properties,
    i.e. :py:class:`Node` and :py:class:`Relationship`.
    """

    def __init__(self, **properties):
        Service.__init__(self)
        self.__properties = PropertySet(properties)
        # Auto-sync will be removed in 2.1
        self.auto_sync_properties = Graph.auto_sync_properties

    def __eq__(self, other):
        return self.properties == other.properties

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash(self.__properties)

    def __contains__(self, key):
        self.__pull_if_bound()
        return key in self.properties

    def __getitem__(self, key):
        self.__pull_if_bound()
        return self.properties.__getitem__(key)

    def __setitem__(self, key, value):
        self.properties.__setitem__(key, value)
        self.__push_if_bound()

    def __delitem__(self, key):
        self.properties.__delitem__(key)
        self.__push_if_bound()

    def bind(self, uri, metadata=None):
        Service.bind(self, uri, metadata)
        self.__properties.bind(uri + "/properties")

    @property
    def properties(self):
        """ The set of properties attached to this object.
        """
        return self.__properties

    def pull(self):
        self.resource.get()
        properties = self.resource.metadata["data"]
        self.__properties.replace(properties or {})

    def push(self):
        self.__properties.push()

    def unbind(self):
        Service.unbind(self)
        self.__properties.unbind()

    def __pull_if_bound(self):
        # remove in 2.1
        if self.auto_sync_properties:
            try:
                self.properties.pull()
            except BindError:
                pass

    def __push_if_bound(self):
        # remove in 2.1
        if self.auto_sync_properties:
            try:
                self.properties.push()
            except BindError:
                pass


class Node(PropertyContainer):
    """ A Node instance represents a graph node and may exist purely
    client-side or may be bound to a corresponding server node. Node
    labels are fully integrated within py2neo 2.0 and therefore may
    be provided along with properties on construction::

        >>> from py2neo import Node
        >>> alice = Node("Person", name="Alice")
        >>> banana = Node("Fruit", "Food", colour="yellow", tasty=True)

    All positional arguments are interpreted as labels and all
    keyword arguments as properties. It is possible to construct Node
    instances from other data types (such as a dictionary) by using
    the :meth:`.cast` method::

        >>> bob = Node.cast({"name": "Bob Robertson", "age": 44})

    Labels and properties can be accessed and modified using the
    :attr:`.labels` and :attr:`.properties` attributes respectively.
    The *labels* attribute is an instance of :class:`.LabelSet` which
    extends the built-in *set* class. Similarly, *properties* is an instance of
    :class:`py2neo.PropertySet` which extends *dict*.

        >>> alice.properties["name"]
        'Alice'
        >>> alice.labels
        {'Person'}
        >>> alice.labels.add("Employee")
        >>> alice.properties["employee_no"] = 3456
        >>> alice
        (:Employee:Person {employee_no:3456,name:"Alice"})

    One of the core differences between a *PropertySet* and a standard dictionary is in how it handles
    :const:`None` and missing values. As with Neo4j server nodes, missing values are treated as
    equivalent to:const:`None` and vice versa.

    To bind a new Node instance to a server node, use the :func:`py2neo.Graph.create` method::

        >>> graph.create(alice, bob, {"name": "Carol", "employee_no": 9998})
        ((n234:Employee:Person {employee_no:3456,name:"Alice"}),
         (n235 {age:44,name:"Bob Robertson"}),
         (n236 {employee_no:9998,name:"Carol"}))

    The *create* method returns Node instances for each argument supplied. When the argument is itself
    a Node, that same instance is bound and returned; in other cases, a new Node is created.

    In older versions of py2neo, Node properties would be automatically synchronised when modified.
    In some cases, this behaviour could lead to performance degradation through an excess of network
    traffic. Py2neo 2.0 allows explicit control over this synchronisation (at the expense of a few
    extra lines of code) by using the **push** and **pull** methods:

    .. code-block:: python
       :emphasize-lines: 6-7

       >>> from py2neo import watch
       >>> watch("httpstream")
       >>> bob.labels.add("Employee")
       >>> bob.properties["employee_no"] = 42
       >>> bob.push()
       POST http://localhost:7474/db/data/batch [181]
       200 OK [127]

    The **watch** function shown above can be used to monitor HTTP traffic between py2neo and the Neo4j
    server. It adds a logging handler that dumps log records to standard output and - in this case -
    shows that only one HTTP request is made to update both the label and the property.
    """

    cache = WeakValueDictionary()

    @staticmethod
    def cast(*args, **kwargs):
        """ Cast the arguments provided to a :py:class:`neo4j.Node`. The
        following general combinations are possible:

        >>> Node.cast(None)
        >>> Node.cast()
        ()
        >>> Node.cast("Person")
        (:Person)
        >>> Node.cast(name="Alice")
        ({name:"Alice"})
        >>> Node.cast("Person", name="Alice")
        (:Person {name:"Alice"})


        - ``node()``
        - ``node(node_instance)``
        - ``node(property_dict)``
        - ``node(**properties)``
        - ``node(int)`` -> NodePointer(int)
        - ``node(None)`` -> None

        If :py:const:`None` is passed as the only argument, :py:const:`None` is
        returned instead of a ``Node`` instance.

        Examples::

            node()
            node(Node("http://localhost:7474/db/data/node/1"))
            node({"name": "Alice"})
            node(name="Alice")

        Other representations::

            {"name": "Alice"}

        """
        if len(args) == 1 and not kwargs:
            from py2neo.batch import Job
            arg = args[0]
            if arg is None:
                return None
            elif isinstance(arg, (Node, NodePointer, Job)):
                return arg
            elif is_integer(arg):
                return NodePointer(arg)

        inst = Node()

        def apply(x):
            if isinstance(x, dict):
                inst.properties.update(x)
            elif is_collection(x):
                for item in x:
                    apply(item)
            else:
                inst.labels.add(ustr(x))

        for arg in args:
            apply(arg)
        inst.properties.update(kwargs)
        return inst

    @classmethod
    def hydrate(cls, data, inst=None):
        """ Create a new Node instance from a serialised representation held
        within a dictionary. It is expected there is at least a "self" key
        pointing to a URI for this Node; there may also optionally be
        properties passed in the "data" value.
        """
        self = data["self"]
        if inst is None:
            inst = cls.cache.setdefault(self, cls())
        cls.cache[self] = inst
        inst.bind(self, data)
        inst.__stale.clear()
        if "data" in data:
            properties = data["data"]
            properties.update(inst.properties)
            inst._PropertyContainer__properties.replace(properties)
        else:
            inst.__stale.add("properties")
        if "metadata" in data:
            metadata = data["metadata"]
            labels = set(metadata["labels"])
            labels.update(inst.labels)
            inst.__labels.replace(labels)
        else:
            inst.__stale.add("labels")
        return inst

    @classmethod
    def __joinable(cls, obj):
        from py2neo.batch import Job
        return obj is None or isinstance(obj, (Node, NodePointer, Job))

    @classmethod
    def join(cls, n, m):
        """ Attempt to coalesce two equivalent nodes into a single node.
        """
        if not cls.__joinable(n) or not cls.__joinable(m):
            raise TypeError("Can only join Node, NodePointer, Job or None")
        if n is None:
            return m
        elif m is None or n is m:
            return n
        elif isinstance(n, NodePointer) or isinstance(m, NodePointer):
            if isinstance(n, NodePointer) and isinstance(m, NodePointer) and n.address == m.address:
                return n
        elif n.bound and m.bound:
            if n.resource == m.resource:
                return n
        raise JoinError("Cannot join nodes {} and {}".format(n, m))

    def __init__(self, *labels, **properties):
        PropertyContainer.__init__(self, **properties)
        self.__labels = LabelSet(labels)
        self.__stale = set()

    def __repr__(self):
        s = [self.__class__.__name__]
        if self.bound:
            s.append("graph=%r" % self.graph.uri.string)
            s.append("ref=%r" % self.ref)
            if "labels" in self.__stale:
                s.append("labels=?")
            else:
                s.append("labels=%r" % set(self.labels))
            if "properties" in self.__stale:
                s.append("properties=?")
            else:
                s.append("properties=%r" % self.properties)
        else:
            s.append("labels=%r" % set(self.labels))
            s.append("properties=%r" % self.properties)
        return "<" + " ".join(s) + ">"

    def __str__(self):
        return xstr(self.__unicode__())

    def __unicode__(self):
        from py2neo.cypher import CypherWriter
        string = StringIO()
        writer = CypherWriter(string)
        if self.bound:
            writer.write_node(self, "n" + ustr(self._id))
        else:
            writer.write_node(self)
        return string.getvalue()

    def __eq__(self, other):
        if other is None:
            return False
        other = Node.cast(other)
        if self.bound and other.bound:
            return self.resource == other.resource
        else:
            return (LabelSet.__eq__(self.labels, other.labels) and
                    PropertyContainer.__eq__(self, other))

    def __hash__(self):
        value = super(Node, self).__hash__() ^ hash(self.labels)
        if self.bound:
            value ^= hash(self.resource.uri)
        return value

    def __add__(self, other):
        return Path(self, other)

    @property
    def _id(self):
        """ Return the internal ID for this entity.

        :return: integer ID of this entity within the database.
        """
        return int(self.uri.path.segments[-1])

    @property
    def ref(self):
        return "node/%s" % self._id

    def bind(self, uri, metadata=None):
        PropertyContainer.bind(self, uri, metadata)
        if self.graph.supports_node_labels:
            self.__labels.bind(uri + "/labels")
        else:
            from py2neo.legacy.core import LegacyNode
            self.__class__ = LegacyNode
        self.cache[uri] = self

    @property
    def degree(self):
        statement = "START n=node({n}) MATCH (n)-[r]-() RETURN count(r)"
        return self.graph.cypher.execute_one(statement, {"n": self})

    @property
    def exists(self):
        """ Detects whether this Node still exists in the database.
        """
        try:
            self.resource.get()
        except GraphError as error:
            if error.__cause__ and error.__cause__.status_code == NOT_FOUND:
                return False
            else:
                raise
        else:
            return True

    @property
    def labels(self):
        """ The set of labels attached to this Node.
        """
        if self.bound and "labels" in self.__stale:
            self.refresh()
        return self.__labels

    def match(self, rel_type=None, other_node=None, limit=None):
        """ Iterate through matching relationships attached to this node,
        regardless of direction.

        :param rel_type: type of relationships to match or :py:const:`None` if
            any
        :param other_node: concrete :py:class:`Node` to match for other end of
            relationship or :py:const:`None` if any
        :param limit: maximum number of relationships to match or
            :py:const:`None` if no limit
        :return: matching relationships
        :rtype: generator

        .. seealso::
           :py:func:`Graph.match <py2neo.neo4j.Graph.match>`
        """
        return self.graph.match(self, rel_type, other_node, True, limit)

    def match_incoming(self, rel_type=None, start_node=None, limit=None):
        """ Iterate through matching relationships where this node is the end
        node.

        :param rel_type: type of relationships to match or :py:const:`None` if
            any
        :param start_node: concrete start :py:class:`Node` to match or
            :py:const:`None` if any
        :param limit: maximum number of relationships to match or
            :py:const:`None` if no limit
        :return: matching relationships
        :rtype: generator

        .. seealso::
           :py:func:`Graph.match <py2neo.neo4j.Graph.match>`
        """
        return self.graph.match(start_node, rel_type, self, False, limit)

    def match_outgoing(self, rel_type=None, end_node=None, limit=None):
        """ Iterate through matching relationships where this node is the start
        node.

        :param rel_type: type of relationships to match or :py:const:`None` if
            any
        :param end_node: concrete end :py:class:`Node` to match or
            :py:const:`None` if any
        :param limit: maximum number of relationships to match or
            :py:const:`None` if no limit
        :return: matching relationships
        :rtype: generator

        .. seealso::
           :py:func:`Graph.match <py2neo.neo4j.Graph.match>`
        """
        return self.graph.match(self, rel_type, end_node, False, limit)

    @property
    def properties(self):
        """ The set of properties attached to this Node.
        """
        if self.bound and "properties" in self.__stale:
            self.refresh()
        return super(Node, self).properties

    def pull(self):
        super(Node, self).properties.clear()
        self.__labels.clear()
        self.refresh()

    def push(self):
        from py2neo.batch.push import PushBatch
        batch = PushBatch(self.graph)
        batch.append(self)
        batch.push()

    def refresh(self):
        """ Non-destructive pull.
        """
        query = "START a=node({a}) RETURN a,labels(a)"
        content = self.graph.cypher.post(query, {"a": self._id}).content
        dehydrated, label_metadata = content["data"][0]
        dehydrated.setdefault("metadata", {})["labels"] = label_metadata
        Node.hydrate(dehydrated, self)

    def unbind(self):
        try:
            del self.cache[self.uri]
        except KeyError:
            pass
        PropertyContainer.unbind(self)
        self.__labels.unbind()


class NodePointer(object):

    def __init__(self, address):
        self.address = address

    def __repr__(self):
        return "<NodePointer address=%s>" % self.address

    def __str__(self):
        return xstr(self.__unicode__())

    def __unicode__(self):
        return "{%s}" % self.address

    def __eq__(self, other):
        return self.address == other.address

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash(self.address)


class Rel(PropertyContainer):
    """ A relationship with no start or end nodes.
    """

    cache = WeakValueDictionary()
    pair = None
    pair_class = object

    @staticmethod
    def cast(*args, **kwargs):
        """ Cast the arguments provided to a Rel object.

        >>> Rel.cast('KNOWS')
        -[:KNOWS]->
        >>> Rel.cast(('KNOWS',))
        -[:KNOWS]->
        >>> Rel.cast('KNOWS', {'since': 1999})
        -[:KNOWS {since:1999}]->
        >>> Rel.cast(('KNOWS', {'since': 1999}))
        -[:KNOWS {since:1999}]->
        >>> Rel.cast('KNOWS', since=1999)
        -[:KNOWS {since:1999}]->

        """

        if len(args) == 1 and not kwargs:
            from py2neo.batch import Job
            arg = args[0]
            if arg is None:
                return None
            elif isinstance(arg, (Rel, Job)):
                return arg
            elif isinstance(arg, Relationship):
                return arg.rel

        inst = Rel()

        def apply(x):
            if isinstance(x, dict):
                inst.properties.update(x)
            elif is_collection(x):
                for item in x:
                    apply(item)
            else:
                inst.type = ustr(x)

        for arg in args:
            apply(arg)
        inst.properties.update(kwargs)
        return inst

    @classmethod
    def hydrate(cls, data, inst=None):
        """ Create a new Rel instance from a serialised representation held
        within a dictionary. It is expected there is at least a "self" key
        pointing to a URI for this Rel; there may also optionally be a "type"
        and properties passed in the "data" value.
        """
        self = data["self"]
        if inst is None:
            inst = cls.cache.setdefault(self, cls())
        cls.cache[self] = inst
        inst.bind(self, data)
        inst.__type = data.get("type")
        pair = inst.pair
        if pair is not None:
            pair._Rel__type = inst.__type
        if "data" in data:
            properties = data["data"]
            properties.update(inst.properties)
            inst._PropertyContainer__properties.replace(properties)
            inst.__stale.clear()
        else:
            inst.__stale.clear()
            inst.__stale.add("properties")
        return inst

    def __init__(self, *type_, **properties):
        if len(type_) > 1:
            raise ValueError("Only one relationship type can be specified")
        PropertyContainer.__init__(self, **properties)
        self.__type = type_[0] if type_ else None
        self.__stale = set()

    def __repr__(self):
        s = [self.__class__.__name__]
        if self.bound:
            s.append("graph=%r" % self.graph.uri.string)
            s.append("ref=%r" % self.ref)
            if "type" in self.__stale:
                s.append("type=?")
            else:
                s.append("type=%r" % self.type)
            if "properties" in self.__stale:
                s.append("properties=?")
            else:
                s.append("properties=%r" % self.properties)
        else:
            s.append("type=%r" % type)
            s.append("properties=%r" % self.properties)
        return "<" + " ".join(s) + ">"

    def __str__(self):
        return xstr(self.__unicode__())

    def __unicode__(self):
        from py2neo.cypher import CypherWriter
        string = StringIO()
        writer = CypherWriter(string)
        if self.bound:
            writer.write_rel(self, "r" + ustr(self._id))
        else:
            writer.write_rel(self)
        return string.getvalue()

    def __eq__(self, other):
        if other is None:
            return False
        other = Rel.cast(other)
        if self.bound and other.bound:
            return self.resource == other.resource
        else:
            return self.type == other.type and self.properties == other.properties

    def __hash__(self):
        value = super(Rel, self).__hash__() ^ hash(self.type)
        if self.bound:
            value ^= hash(self.resource.uri)
        return value

    def __pos__(self):
        return self

    def __neg__(self):
        if self.pair is None:
            self.pair = self.pair_class()
            self.pair.__resource__ = self.__resource__
            self.pair._PropertyContainer__properties = self._PropertyContainer__properties
            self.pair._Rel__type = self.__type
            self.pair._Rel__stale = self.__stale
            self.pair.pair = self
        return self.pair

    def __abs__(self):
        return self

    @property
    def _id(self):
        """ Return the internal ID for this Rel.

        :return: integer ID of this entity within the database.
        """
        return int(self.uri.path.segments[-1])

    @property
    def ref(self):
        return "relationship/%s" % self._id

    def bind(self, uri, metadata=None):
        PropertyContainer.bind(self, uri, metadata)
        self.cache[uri] = self
        pair = self.pair
        if pair is not None:
            PropertyContainer.bind(pair, uri, metadata)
            # make sure we're using exactly the same resource object
            # (maybe could write a Service.multi_bind classmethod
            pair.__resource__ = self.__resource__
            pair.cache[uri] = pair

    @property
    def exists(self):
        """ Detects whether this Rel still exists in the database.
        """
        try:
            self.resource.get()
        except GraphError as error:
            if error.__cause__ and error.__cause__.status_code == NOT_FOUND:
                return False
            else:
                raise
        else:
            return True

    @property
    def properties(self):
        """ The set of properties attached to this Rel.
        """
        if self.bound and "properties" in self.__stale:
            self.refresh()
        return super(Rel, self).properties

    def pull(self):
        super(Rel, self).properties.clear()
        self.refresh()

    def refresh(self):
        """ Non-destructive pull.
        """
        super(Rel, self).pull()
        pulled_type = self.resource.metadata["type"]
        self.__type = pulled_type
        pair = self.pair
        if pair is not None:
            pair._Rel__type = pulled_type
        self.__stale.clear()

    @property
    def type(self):
        if self.bound and self.__type is None:
            self.pull()
        return self.__type

    @type.setter
    def type(self, name):
        if self.bound:
            raise AttributeError("The type of a bound Rel is immutable")
        self.__type = name
        pair = self.pair
        if pair is not None:
            pair._Rel__type = name

    def unbind(self):
        try:
            del self.cache[self.uri]
        except KeyError:
            pass
        PropertyContainer.unbind(self)
        pair = self.pair
        if pair is not None:
            try:
                del pair.cache[pair.uri]
            except KeyError:
                pass
            PropertyContainer.unbind(pair)


class Rev(Rel):

    pair_class = Rel

    def __abs__(self):
        return self.__neg__()

    def __hash__(self):
        return -(super(Rev, self).__hash__())


Rel.pair_class = Rev


class Path(object):
    """ A chain of relationships.

        >>> from py2neo import Node, Path, Rev
        >>> alice, bob, carol = Node(name="Alice"), Node(name="Bob"), Node(name="Carol")
        >>> abc = Path(alice, "KNOWS", bob, Rev("KNOWS"), carol)
        >>> abc
        ({name:"Alice"})-[:KNOWS]->({name:"Bob"})<-[:KNOWS]-({name:"Carol"})
        >>> abc.nodes
        (({name:"Alice"}), ({name:"Bob"}), ({name:"Carol"}))
        >>> abc.rels
        (-[:KNOWS]->, <-[:KNOWS]-)
        >>> abc.relationships
        (({name:"Alice"})-[:KNOWS]->({name:"Bob"}), ({name:"Carol"})-[:KNOWS]->({name:"Bob"}))
        >>> dave, eve = Node(name="Dave"), Node(name="Eve")
        >>> de = Path(dave, "KNOWS", eve)
        >>> de
        ({name:"Dave"})-[:KNOWS]->({name:"Eve"})
        >>> abcde = Path(abc, "KNOWS", de)
        >>> abcde
        ({name:"Alice"})-[:KNOWS]->({name:"Bob"})<-[:KNOWS]-({name:"Carol"})-[:KNOWS]->({name:"Dave"})-[:KNOWS]->({name:"Eve"})

    """

    @classmethod
    def hydrate(cls, data, inst=None):
        node_uris = data["nodes"]
        relationship_uris = data["relationships"]
        rel_rev = [Rel if direction == "->" else Rev for direction in data["directions"]]
        if inst is None:
            nodes = [Node.hydrate({"self": uri}) for uri in node_uris]
            rels = [rel_rev[i].hydrate({"self": uri}) for i, uri in enumerate(relationship_uris)]
            inst = Path(*round_robin(nodes, rels))
        else:
            for i, node in enumerate(inst.nodes):
                uri = node_uris[i]
                Node.hydrate({"self": uri}, node)
            for i, rel in enumerate(inst.rels):
                uri = relationship_uris[i]
                rel_rev[i].hydrate({"self": uri}, rel)
        inst.__metadata = data
        return inst

    def __init__(self, *entities):
        nodes = []
        rels = []

        def join_path(path, index):
            if len(nodes) == len(rels):
                nodes.extend(path.nodes)
                rels.extend(path.rels)
            else:
                # try joining forward
                try:
                    nodes[-1] = Node.join(nodes[-1], path.start_node)
                except JoinError:
                    # try joining backward
                    try:
                        nodes[-1] = Node.join(nodes[-1], path.end_node)
                    except JoinError:
                        raise JoinError("Path at position %s cannot be joined" % index)
                    else:
                        nodes.extend(path.nodes[-2::-1])
                        rels.extend(-r for r in path.rels[::-1])
                else:
                    nodes.extend(path.nodes[1:])
                    rels.extend(path.rels)

        def join_rel(rel, index):
            if len(nodes) == len(rels):
                raise JoinError("Rel at position %s cannot be joined" % index)
            else:
                rels.append(rel)

        def join_node(node):
            if len(nodes) == len(rels):
                nodes.append(node)
            else:
                nodes[-1] = Node.join(nodes[-1], node)

        for i, entity in enumerate(entities):
            if isinstance(entity, Path):
                join_path(entity, i)
            elif isinstance(entity, Rel):
                join_rel(entity, i)
            elif isinstance(entity, (Node, NodePointer)):
                join_node(entity)
            elif len(nodes) == len(rels):
                join_node(Node.cast(entity))
            else:
                join_rel(Rel.cast(entity), i)
        join_node(None)

        self.__nodes = tuple(nodes)
        self.__rels = tuple(rels)
        self.__relationships = None
        self.__order = len(self.__nodes)
        self.__size = len(self.__rels)
        self.__metadata = None

    def __repr__(self):
        s = [self.__class__.__name__]
        if self.bound:
            s.append("graph=%r" % self.graph.uri.string)
            s.append("start=%r" % self.start_node.ref)
            s.append("end=%r" % self.end_node.ref)
        s.append("order=%r" % self.order)
        s.append("size=%r" % self.size)
        return "<" + " ".join(s) + ">"

    def __str__(self):
        return xstr(self.__unicode__())

    def __unicode__(self):
        from py2neo.cypher import CypherWriter
        string = StringIO()
        writer = CypherWriter(string)
        writer.write_path(self)
        return string.getvalue()

    def __eq__(self, other):
        try:
            return self.nodes == other.nodes and self.rels == other.rels
        except AttributeError:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        value = 0
        for entity in self.rels + self.nodes:
            value ^= hash(entity)
        return value

    def __bool__(self):
        return bool(self.rels)

    def __nonzero__(self):
        return bool(self.rels)

    def __len__(self):
        return self.size

    def __getitem__(self, item):
        try:
            if isinstance(item, slice):
                path = Path()
                p, q = item.start, item.stop
                if q is not None:
                    q += 1
                path.__nodes = self.nodes[p:q]
                path.__rels = self.rels[item]
                return path
            else:
                if item >= 0:
                    start_node = self.nodes[item]
                    end_node = self.nodes[item + 1]
                else:
                    start_node = self.nodes[item - 1]
                    end_node = self.nodes[item]
                return Relationship(start_node, self.rels[item], end_node)
        except IndexError:
            raise IndexError("Path segment index out of range")

    def __iter__(self):
        return iter(self.relationships)

    def __reversed__(self):
        return iter(reversed(self.relationships))

    def __add__(self, other):
        return Path(self, other)

    @property
    def bound(self):
        try:
            _ = self.service_root
        except BindError:
            return False
        else:
            return True

    @property
    def end_node(self):
        return self.__nodes[-1]

    @property
    def exists(self):
        return all(entity.exists for entity in self.nodes + self.rels)

    @property
    def graph(self):
        return self.service_root.graph

    @property
    def nodes(self):
        """ Return a tuple of all the nodes which make up this path.
        """
        return self.__nodes

    @property
    def order(self):
        """ The number of nodes within this path.
        """
        return self.__order

    def pull(self):
        from py2neo.batch.pull import PullBatch
        batch = PullBatch(self.graph)
        for relationship in self:
            batch.append(relationship)
        batch.pull()

    def push(self):
        from py2neo.batch.push import PushBatch
        batch = PushBatch(self.graph)
        for relationship in self:
            batch.append(relationship)
        batch.push()

    @property
    def relationships(self):
        """ Return a list of all the relationships which make up this path.
        """
        if self.__relationships is None:
            self.__relationships = tuple(
                Relationship(self.nodes[i], rel, self.nodes[i + 1])
                for i, rel in enumerate(self.rels)
            )
        return self.__relationships

    @property
    def rels(self):
        """ Return a tuple of all the rels which make up this path.
        """
        return self.__rels

    @property
    def service_root(self):
        for relationship in self:
            try:
                return relationship.service_root
            except BindError:
                pass
        raise BindError("Local path is not bound to a remote path")

    @property
    def size(self):
        """ The number of relationships within this path.
        """
        return self.__size

    @property
    def start_node(self):
        return self.__nodes[0]

    def unbind(self):
        for entity in self.rels + self.nodes:
            try:
                entity.unbind()
            except BindError:
                pass


class Relationship(Path):
    """ A relationship within a graph, identified by a URI.

    :param uri: URI identifying this relationship
    """

    cache = WeakValueDictionary()

    @staticmethod
    def cast(*args, **kwargs):
        """ Cast the arguments provided to a :py:class:`neo4j.Relationship`. The
        following general combinations are possible:

        - ``rel(relationship_instance)``
        - ``rel((start_node, type, end_node))``
        - ``rel((start_node, type, end_node, properties))``
        - ``rel((start_node, (type, properties), end_node))``
        - ``rel(start_node, (type, properties), end_node)``
        - ``rel(start_node, type, end_node, properties)``
        - ``rel(start_node, type, end_node, **properties)``

        Examples::

            rel(Relationship("http://localhost:7474/db/data/relationship/1"))
            rel((alice, "KNOWS", bob))
            rel((alice, "KNOWS", bob, {"since": 1999}))
            rel((alice, ("KNOWS", {"since": 1999}), bob))
            rel(alice, ("KNOWS", {"since": 1999}), bob)
            rel(alice, "KNOWS", bob, {"since": 1999})
            rel(alice, "KNOWS", bob, since=1999)

        Other representations::

            (alice, "KNOWS", bob)
            (alice, "KNOWS", bob, {"since": 1999})
            (alice, ("KNOWS", {"since": 1999}), bob)

        """
        if len(args) == 1 and not kwargs:
            arg = args[0]
            if isinstance(arg, Relationship):
                return arg
            elif isinstance(arg, tuple):
                if len(arg) == 3:
                    return Relationship(*arg)
                elif len(arg) == 4:
                    return Relationship(arg[0], arg[1], arg[2], **arg[3])
                else:
                    raise TypeError("Cannot cast relationship from {0}".format(arg))
            else:
                raise TypeError("Cannot cast relationship from {0}".format(arg))
        elif len(args) == 3:
            rel = Relationship(*args)
            rel.properties.update(kwargs)
            return rel
        elif len(args) == 4:
            props = args[3]
            props.update(kwargs)
            return Relationship(*args[0:3], **props)
        else:
            raise TypeError("Cannot cast relationship from {0}".format((args, kwargs)))

    @classmethod
    def hydrate(cls, data, inst=None):
        """ Create a new Relationship instance from a serialised representation
        held within a dictionary.
        """
        self = data["self"]
        if inst is None:
            inst = cls.cache.setdefault(self, cls(Node.hydrate({"self": data["start"]}),
                                                  Rel.hydrate(data),
                                                  Node.hydrate({"self": data["end"]})))
        else:
            Node.hydrate({"self": data["start"]}, inst.start_node)
            Node.hydrate({"self": data["end"]}, inst.end_node)
            Rel.hydrate(data, inst.rel)
        cls.cache[self] = inst
        return inst

    def __init__(self, start_node, rel, end_node, **properties):
        cast_rel = Rel.cast(rel)
        if isinstance(cast_rel, Rev):  # always forwards
            Path.__init__(self, end_node, -cast_rel, start_node)
        else:
            Path.__init__(self, start_node, cast_rel, end_node)
        self.rel.properties.update(properties)

    def __repr__(self):
        s = [self.__class__.__name__]
        if self.bound:
            s.append("graph=%r" % self.graph.uri.string)
            s.append("ref=%r" % self.ref)
            s.append("start=%r" % self.start_node.ref)
            s.append("end=%r" % self.end_node.ref)
            if "type" in self.rel._Rel__stale:
                s.append("type=?")
            else:
                s.append("type=%r" % self.type)
            if "properties" in self.rel._Rel__stale:
                s.append("properties=?")
            else:
                s.append("properties=%r" % self.properties)
        else:
            s.append("type=%r" % self.type)
            s.append("properties=%r" % self.properties)
        return "<" + " ".join(s) + ">"

    def __str__(self):
        return xstr(self.__unicode__())

    def __unicode__(self):
        from py2neo.cypher import CypherWriter
        string = StringIO()
        writer = CypherWriter(string)
        if self.bound:
            writer.write_relationship(self, "r" + ustr(self._id))
        else:
            writer.write_relationship(self)
        return string.getvalue()

    def __len__(self):
        return 1

    def __contains__(self, key):
        return self.rel.__contains__(key)

    def __getitem__(self, key):
        return self.rel.__getitem__(key)

    def __setitem__(self, key, value):
        self.rel.__setitem__(key, value)

    def __delitem__(self, key):
        self.rel.__delitem__(key)

    @property
    def _id(self):
        return self.rel._id

    @property
    def ref(self):
        return "relationship/%s" % self._id

    def bind(self, uri, metadata=None):
        """ Bind to a remote relationship. The relationship start and end
        nodes will also become bound to their corresponding remote nodes.
        """
        self.rel.bind(uri, metadata)
        self.cache[uri] = self
        for i, key, node in [(0, "start", self.start_node), (-1, "end", self.end_node)]:
            uri = self.resource.metadata[key]
            if isinstance(node, Node):
                node.bind(uri)
            else:
                nodes = list(self._Path__nodes)
                node = Node.cache.setdefault(uri, Node())
                if not node.bound:
                    node.bind(uri)
                nodes[i] = node
                self._Path__nodes = tuple(nodes)

    @property
    def bound(self):
        """ Flag to indicate if this relationship is bound to a remote
        relationship.
        """
        return self.rel.bound

    @property
    def exists(self):
        """ Flag to indicate if this relationship exists in the
        database, if bound.
        """
        return self.rel.exists

    @property
    def graph(self):
        return self.service_root.graph

    @property
    def properties(self):
        return self.rel.properties

    def pull(self):
        self.rel.pull()

    def push(self):
        self.rel.push()

    @property
    def rel(self):
        """ The :class:`py2neo.Rel` object within this relationship.
        """
        return self.rels[0]

    @property
    def relative_uri(self):
        """ The URI of this relationship, relative to the graph
        database root, if bound.
        """
        return self.rel.relative_uri

    @property
    def resource(self):
        """ The resource object wrapped by this relationship, if
        bound.
        """
        return self.rel.resource

    @property
    def service_root(self):
        try:
            return self.rel.service_root
        except BindError:
            try:
                return self.start_node.service_root
            except BindError:
                return self.end_node.service_root

    @property
    def type(self):
        """ The type of this relationship.
        """
        return self.rel.type

    @type.setter
    def type(self, name):
        if self.rel.bound:
            raise AttributeError("The type of a bound Relationship is immutable")
        self.rel.type = name

    def unbind(self):
        """ Unbind this relationship and its start and end
        nodes, if bound.
        """
        try:
            del self.cache[self.uri]
        except KeyError:
            pass
        self.rel.unbind()
        for node in [self.start_node, self.end_node]:
            if isinstance(node, Node):
                try:
                    node.unbind()
                except BindError:
                    pass

    @property
    def uri(self):
        """ The URI of this relationship, if bound.
        """
        return self.rel.uri


class Subgraph(object):

    def __init__(self, *entities):
        self.__nodes = set()
        self.__relationships = set()
        for entity in entities:
            entity = Graph.cast(entity)
            if isinstance(entity, Node):
                self.__nodes.add(entity)
            elif isinstance(entity, Relationship):
                self.__nodes.add(entity.start_node)
                self.__nodes.add(entity.end_node)
                self.__relationships.add(entity)
            elif isinstance(entity, (Path, Subgraph)):
                for node in entity.nodes:
                    self.__nodes.add(node)
                for relationship in entity.relationships:
                    self.__relationships.add(relationship)
            elif entity is not None:
                raise ValueError("Cannot add %s to Subgraph" % entity.__class__.__name__)

    def __repr__(self):
        return "<Subgraph order=%s size=%s>" % (self.order, self.size)

    def __eq__(self, other):
        return self.nodes == other.nodes and self.relationships == other.relationships

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        value = 0
        for entity in self.__nodes | self.__relationships:
            value ^= hash(entity)
        return value

    def __bool__(self):
        return bool(self.__relationships)

    def __nonzero__(self):
        return bool(self.__relationships)

    def __len__(self):
        return self.size

    def __iter__(self):
        return iter(self.__relationships)

    def __contains__(self, item):
        if isinstance(item, Node):
            return item in self.__nodes
        elif isinstance(item, Path):
            for relationship in item:
                if relationship not in self.__relationships:
                    return False
            return True
        else:
            raise False

    @property
    def bound(self):
        try:
            _ = self.service_root
        except BindError:
            return False
        else:
            return True

    @property
    def exists(self):
        return all(entity.exists for entity in self.__nodes | self.__relationships)

    @property
    def graph(self):
        return self.service_root.graph

    @property
    def nodes(self):
        return frozenset(self.__nodes)

    @property
    def order(self):
        return len(self.__nodes)

    @property
    def relationships(self):
        return frozenset(self.__relationships)

    @property
    def service_root(self):
        for relationship in self:
            try:
                return relationship.service_root
            except BindError:
                pass
        raise BindError("Local path is not bound to a remote path")

    @property
    def size(self):
        return len(self.__relationships)

    def unbind(self):
        for entity in self.__nodes | self.__relationships:
            try:
                entity.unbind()
            except BindError:
                pass


class ServerPlugin(object):

    def __init__(self, graph, name):
        self.graph = graph
        self.name = name
        extensions = self.graph.resource.metadata["extensions"]
        try:
            self.resources = {key: Resource(value) for key, value in extensions[self.name].items()}
        except KeyError:
            raise LookupError("No plugin named %r found on graph <%s>" % (self.name, graph.uri))


class UnmanagedExtension(object):

    def __init__(self, graph, path):
        self.graph = graph
        self.resource = Resource(graph.service_root.uri.resolve(path))
        try:
            self.resource.get()
        except GraphError:
            raise NotImplementedError("No extension found at path %r on "
                                      "graph <%s>" % (path, graph.uri))


from py2neo.deprecated import *
