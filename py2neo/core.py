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
Bindable - base class for objects that can be bound to remote resources
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
import json
import re
from warnings import warn
from weakref import WeakValueDictionary

from py2neo import __version__
from py2neo.error import GraphError, BindError, JoinError
from py2neo.packages.httpstream import http, ClientError, ServerError, \
    Resource as _Resource, ResourceTemplate as _ResourceTemplate
from py2neo.packages.httpstream.http import JSONResponse
from py2neo.packages.httpstream.numbers import NOT_FOUND, CONFLICT
from py2neo.packages.httpstream.packages.urimagic import percent_encode, URI, URITemplate
from py2neo.packages.jsonstream import assembled, grouped
from py2neo.types import cast_property
from py2neo.util import is_collection, is_integer, round_robin, ustr, version_tuple


__all__ = ["authenticate", "rewrite", "Resource", "ResourceTemplate", "Bindable",
           "ServiceRoot", "Graph", "Schema", "PropertySet", "LabelSet", "PropertyContainer",
           "Node", "NodePointer", "Rel", "Rev", "Path", "Relationship"]


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
            response = self.__base.get(headers, redirect_limit, **kwargs)
        except (ClientError, ServerError) as error:
            if isinstance(error, JSONResponse):
                content = error.content
                content["request"] = error.request
                content["response"] = error
                raise self.error_class.hydrate(content)
            else:
                raise
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
                content = error.content
                content["request"] = error.request
                content["response"] = error
                raise self.error_class.hydrate(content)
            else:
                raise
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
                content = error.content
                content["request"] = error.request
                content["response"] = error
                raise self.error_class.hydrate(content)
            else:
                raise
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
                content = error.content
                content["request"] = error.request
                content["response"] = error
                raise self.error_class.hydrate(content)
            else:
                raise
        else:
            return response


class ResourceTemplate(_ResourceTemplate):

    error_class = GraphError

    def expand(self, **values):
        resource = Resource(self.uri_template.expand(**values))
        resource.error_class = self.error_class
        return resource


class Bindable(object):
    """ Base class for objects that can be bound to a remote resource.
    """

    error_class = GraphError

    __resource = None

    def __init__(self, uri=None, metadata=None):
        if uri and not self.bound:
            self.bind(uri, metadata)

    def bind(self, uri, metadata=None):
        """ Bind object to Resource or ResourceTemplate.
        """
        if "{" in uri:
            if metadata:
                raise ValueError("Initial metadata cannot be passed to a resource template")
            self.__resource = ResourceTemplate(uri)
        else:
            self.__resource = Resource(uri, metadata)
        self.__resource.error_class = self.error_class

    @property
    def bound(self):
        """ Returns :const:`True` if bound to a remote resource.
        """
        return self.__resource is not None

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
            return self.__resource
        else:
            raise BindError("Local entity is not bound to a remote entity")

    @property
    def service_root(self):
        return self.resource.service_root

    def unbind(self):
        self.__resource = None

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


class Graph(Bindable):
    """ An instance of a `Neo4j <http://neo4j.org/>`_ database identified by
    its base URI. Generally speaking, this is the only URI which a system
    attaching to this service should need to be directly aware of; all further
    entity URIs will be discovered automatically from within response content
    when possible (see `Hypermedia <http://en.wikipedia.org/wiki/Hypermedia>`_)
    or will be derived from existing URIs.

    The following code illustrates how to connect to a database server and
    display its version number::

        from py2neo import Graph
        
        graph = Graph()
        print(graph.neo4j_version)

    :param uri: the base URI of the database (defaults to <http://localhost:7474/db/data/>)
    """

    __instances = {}

    __batch = None
    __cypher = None
    __schema = None
    __node_labels = None
    __relationship_types = None

    # Auto-sync will be removed in 2.1
    auto_sync_properties = False

    @staticmethod
    def cast(obj):
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

    def __len__(self):
        """ Return the size of this graph (i.e. the number of relationships).
        """
        return self.size

    def __bool__(self):
        return True

    def __nonzero__(self):
        return True

    def __contains__(self, entity):
        return entity.bound and entity.uri.string.startswith(entity.uri.string)

    @property
    def batch(self):
        if self.__batch is None:
            from py2neo.batch import BatchResource
            self.__batch = BatchResource(self.uri.string + "batch")
        return self.__batch

    @property
    def cypher(self):
        if self.__cypher is None:
            from py2neo.cypher import CypherResource
            self.__cypher = CypherResource(self.uri.string + "cypher")
        return self.__cypher

    def create(self, *entities):
        """ Create multiple nodes and/or relationships as part of a single
        batch.

        The abstracts provided may use any accepted notation, as described in
        the section on py2neo fundamentals.
        For a node, simply pass a dictionary of properties; for a relationship, pass a tuple of
        (start, type, end) or (start, type, end, data) where start and end
        may be :py:class:`Node` instances or zero-based integral references
        to other node entities within this batch::

            # create a single node
            alice, = graph.create({"name": "Alice"})

            # create multiple nodes
            people = graph.create(
                {"name": "Alice", "age": 33}, {"name": "Bob", "age": 44},
                {"name": "Carol", "age": 55}, {"name": "Dave", "age": 66},
            )

            # create two nodes with a connecting relationship
            alice, bob, ab = graph.create(
                {"name": "Alice"}, {"name": "Bob"},
                (0, "KNOWS", 1, {"since": 2006})
            )

            # create a node plus a relationship to pre-existing node
            bob, ab = graph.create({"name": "Bob"}, (alice, "PERSON", 0))

        :return: list of :py:class:`Node` and/or :py:class:`Relationship`
            instances

        .. warning::
            This method will *always* return a list, even when only creating
            a single node or relationship. To automatically unpack a list
            containing a single item, append a trailing comma to the variable
            name on the left of the assignment operation.

        """
        entities = tuple(map(Graph.cast, entities))
        names = []

        supports_node_labels = self.supports_node_labels

        start_clause = []
        create_clause = []
        return_clause = []
        params = {}

        def _(*args):
            return "".join("_" + ustr(arg) for arg in args)

        def create_node(name, node):
            if node.bound:
                start_clause.append("{0}=node({{{0}}})".format(name))
                params[name] = node._id
            else:
                labels = ""
                if supports_node_labels:
                    labels = "".join(":`" + label.replace("`", "``") + "`"
                                     for label in node.labels)
                if node.properties:
                    template = "({0}{1} {{{0}}})"
                    params[name] = node.properties
                else:
                    template = "({0}{1})"
                create_clause.append(template.format(name, labels))
            return_clause.append(name)
            return [name], []

        def create_rel(name, rel, *node_names):
            if rel.bound:
                start_clause.append("{0}=relationship({{{0}}})".format(name))
                params[name] = rel._id
            else:
                if rel.properties:
                    template = "({0})-[{1}:`{2}` {{{1}}}]->({3})"
                    params[name] = rel.properties
                else:
                    template = "({0})-[{1}:`{2}`]->({3})"
                create_clause.append(template.format(node_names[0], name,
                                              rel.type.replace("`", "``"), node_names[1]))
            return_clause.append(name)
            return [], [name]

        def create_path(name, path):
            node_names = [None] * len(path.nodes)
            rel_names = [None] * len(path.rels)
            for i, node in enumerate(path.nodes):
                if isinstance(node, NodePointer):
                    node_names[i] = _(node.address)
                    # Switch out node with object from elsewhere in entity list
                    nodes = list(path.nodes)
                    node = entities[node.address]
                    if not isinstance(node, Node):
                        raise ValueError("Pointer does not refer to a node")
                    nodes[i] = node
                    path.__nodes = tuple(nodes)
                else:
                    node_names[i] = name + "n" + ustr(i)
                    create_node(node_names[i], node)
            for i, rel in enumerate(path.rels):
                rel_names[i] = name + "r" + ustr(i)
                create_rel(rel_names[i], rel, node_names[i], node_names[i + 1])
            return node_names, rel_names

        for i, entity in enumerate(entities):
            name = _(i)
            if isinstance(entity, Node):
                names.append(create_node(name, entity))
            elif isinstance(entity, Path):
                names.append(create_path(name, entity))
            else:
                raise TypeError("Cannot create entity of type {}".format(type(entity).__name__))

        clauses = []
        if start_clause:
            clauses.append("START " + ",".join(start_clause))
        if create_clause:
            clauses.append("CREATE " + ",".join(create_clause))
        if return_clause:
            clauses.append("RETURN " + ",".join(return_clause))
        if not clauses:
            return []

        statement = "\n".join(clauses)
        raw = self.cypher.post(statement, params).content
        columns = raw["columns"]
        data = raw["data"]

        dehydrated = dict(zip(columns, data[0]))
        for i, entity in enumerate(entities):
            node_names, rel_names = names[i]
            if isinstance(entity, Node):
                metadata = dehydrated[node_names[0]]
                entity.bind(metadata["self"], metadata)
            elif isinstance(entity, Relationship):
                metadata = dehydrated[rel_names[0]]
                entity.bind(metadata["self"], metadata)
            elif isinstance(entity, Path):
                for j, node in enumerate(entity.nodes):
                    metadata = dehydrated[node_names[j]]
                    node.bind(metadata["self"], metadata)
                for j, rel in enumerate(entity.rels):
                    metadata = dehydrated[rel_names[j]]
                    rel.bind(metadata["self"], metadata)

        return entities

    def delete(self, *entities):
        """ Delete one or more Nodes, Relationships and/or Paths. Note that
        deleting a relationship or path will remove only the relationships,
        not the nodes. These must be deleted explicitly *after* the relationships
        have been deleted.
        """
        if entities:
            from py2neo.batch import WriteBatch
            batch = WriteBatch(self)
            for entity in entities:
                if isinstance(entity, Path):
                    for rel in entity:
                        batch.delete(rel)
                elif entity is not None:
                    batch.delete(entity)
            batch.run()

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
        uri = self.uri.resolve("/".join(["label", label, "nodes"]))
        if property_key:
            uri = uri.resolve("?" + percent_encode({property_key: json.dumps(property_value, ensure_ascii=False)}))
        try:
            for i, result in grouped(Resource(uri).get()):
                yield Node.hydrate(assembled(result))
        except GraphError as err:
            if err.response.status_code == NOT_FOUND:
                pass
            else:
                raise

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
                from py2neo.cypher import CypherResults
                return CypherResults.hydrate(data, self)
            elif "neo4j_version" in data:
                return self
            elif "exception" in data and "stacktrace" in data:
                raise GraphError.hydrate(data)
            else:
                warn("Map literals returned over the Neo4j REST interface are ambiguous "
                     "and may be hydrated as graph objects")
                return data
        elif is_collection(data):
            return type(data)(map(self.hydrate, data))
        else:
            return data

    def match(self, start_node=None, rel_type=None, end_node=None,
              bidirectional=False, limit=None):
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
            params = {"A": start_node._id}
        elif start_node is None:
            query = "START b=node({B})"
            end_node = Node.cast(end_node)
            if not end_node.bound:
                raise TypeError("Nodes for relationship match end points must be bound")
            params = {"B": end_node._id}
        else:
            query = "START a=node({A}),b=node({B})"
            start_node = Node.cast(start_node)
            end_node = Node.cast(end_node)
            if not start_node.bound or not end_node.bound:
                raise TypeError("Nodes for relationship match end points must be bound")
            params = {"A": start_node._id, "B": end_node._id}
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
                yield result[0]
        finally:
            results.close()

    def match_one(self, start_node=None, rel_type=None, end_node=None,
                  bidirectional=False):
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

    def merge(self, *entities):
        """ Merge a number of nodes, relationships and/or paths into the
        graph using Cypher MERGE/CREATE UNIQUE.
        """
        # TODO (can't use batch)
        pass

    @property
    def neo4j_version(self):
        """ The database software version as a 4-tuple of (``int``, ``int``,
        ``int``, ``str``).
        """
        return version_tuple(self.resource.metadata["neo4j_version"])

    def node(self, id_):
        """ Fetch a node by ID.
        """
        resource = self.resource.resolve("node/%s" % id_)
        uri_string = resource.uri.string
        try:
            return Node.cache[uri_string]
        except KeyError:
            try:
                return Node.cache.setdefault(uri_string, Node.hydrate(resource.get().content))
            except ClientError:
                raise ValueError("Node with ID {} not found".format(id_))

    @property
    def node_labels(self):
        """ The set of node labels currently defined within the graph.
        """
        if not self.supports_node_labels:
            raise NotImplementedError("Node labels not available for this Neo4j server version")
        if self.__node_labels is None:
            self.__node_labels = Resource(self.uri.string + "labels")
        return frozenset(self.__node_labels.get().content)

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
                return Relationship.cache.setdefault(uri_string,
                                                     Relationship.hydrate(resource.get().content))
            except ClientError:
                raise ValueError("Relationship with ID {} not found".format(id_))

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

    @property
    def supports_cypher_transactions(self):
        """ Indicates whether the server supports explicit Cypher transactions.
        """
        return "transaction" in self.resource.metadata


class Schema(Bindable):

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
        except ClientError as error:
            if error.status_code == NOT_FOUND:
                raise GraphError("No such schema index (label=%r, key=%r)" % (label, property_key))
            else:
                raise

    def drop_unique_constraint(self, label, property_key):
        """ Remove uniqueness constraint for a given property key.
        """
        try:
            self._uniqueness_constraint_key_template.expand(
                label=label, property_key=property_key).delete()
        except ClientError as error:
            if error.status_code == NOT_FOUND:
                raise GraphError("No such unique constraint (label=%r, key=%r)" %
                                 (label, property_key))
            else:
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


class PropertySet(Bindable, dict):
    """ A dict subclass that equates None with a non-existent key and can be
    bound to a remote *properties* resource.
    """

    def __init__(self, iterable=None, **kwargs):
        Bindable.__init__(self)
        dict.__init__(self)
        self.update(iterable, **kwargs)

    def __eq__(self, other):
        if not isinstance(other, PropertySet):
            other = PropertySet(other)
        return dict.__eq__(self, other)

    def __ne__(self, other):
        return not self.__eq__(other)

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


class LabelSet(Bindable, set):
    """ A set subclass that can be bound to a remote *labels* resource.
    """

    def __init__(self, iterable=None):
        Bindable.__init__(self)
        set.__init__(self)
        if iterable:
            self.update(iterable)

    def __eq__(self, other):
        if not isinstance(other, LabelSet):
            other = LabelSet(other)
        return set.__eq__(self, other)

    def __ne__(self, other):
        return not self.__eq__(other)

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


class PropertyContainer(Bindable):
    """ Base class for objects that contain a set of properties,
    i.e. :py:class:`Node` and :py:class:`Relationship`.
    """

    def __init__(self, **properties):
        Bindable.__init__(self)
        self.__properties = PropertySet(properties)
        # Auto-sync will be removed in 2.1
        self.auto_sync_properties = Graph.auto_sync_properties

    def __eq__(self, other):
        return self.properties == other.properties

    def __ne__(self, other):
        return not self.__eq__(other)

    def __len__(self):
        return len(self.properties)

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
        Bindable.bind(self, uri, metadata)
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
        Bindable.unbind(self)
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
    """ A node within a graph, identified by a URI. For example:

        >>> from py2neo import Node
        >>> alice = Node("Person", name="Alice")
        >>> alice
        (:Person {name:"Alice"})

    Typically, concrete nodes will not be constructed directly in this way
    by client applications. Instead, methods such as
    :py:func:`Graph.create` build node objects indirectly as
    required. Once created, nodes can be treated like any other container type
    so as to manage properties::

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
        properties = data.get("data")
        labels = data.get("label_data")
        if inst is None:
            inst = cls.cache.setdefault(self, cls())
        cls.cache[self] = inst
        inst.bind(self, data)
        inst.__stale.clear()
        if properties is None:
            inst.__stale.add("properties")
        else:
            inst._PropertyContainer__properties.replace(properties)
        if labels is None:
            inst.__stale.add("labels")
        else:
            inst.__labels.replace(labels)
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
        from py2neo.cypher import Representation
        r = Representation()
        if self.bound:
            r.write_node(self, "n" + ustr(self._id))
        else:
            r.write_node(self)
        return repr(r)

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
        if self.bound:
            hashable = self.resource.uri
        else:
            hashable = (tuple(sorted(self.properties.items())), tuple(sorted(self.labels)))
        return hash(hashable)

    def __add__(self, other):
        return Path(self, other)

    @property
    def _id(self):
        """ Return the internal ID for this entity.

        :return: integer ID of this entity within the database.
        """
        return int(self.uri.path.segments[-1])

    def bind(self, uri, metadata=None):
        PropertyContainer.bind(self, uri, metadata)
        if self.graph.supports_node_labels:
            self.__labels.bind(uri + "/labels")
        else:
            from py2neo.legacy.core import LegacyNode
            self.__class__ = LegacyNode
        self.cache[uri] = self

    @property
    def exists(self):
        """ Detects whether this Node still exists in the database.
        """
        try:
            self.resource.get()
        except GraphError as err:
            if err.response.status_code == NOT_FOUND:
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
            self.pull()
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
            self.pull()
        return super(Node, self).properties

    def pull(self):
        query = "START a=node({a}) RETURN a,labels(a)"
        content = self.graph.cypher.post(query, {"a": self._id}).content
        dehydrated, label_data = content["data"][0]
        dehydrated["label_data"] = label_data
        Node.hydrate(dehydrated, self)

    def push(self):
        from py2neo.batch.push import PushBatch
        batch = PushBatch(self.graph)
        batch.append(self)
        batch.push()

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
        return "{%s}" % self.address

    def __eq__(self, other):
        return self.address == other.address

    def __ne__(self, other):
        return not self.__eq__(other)


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
        type_ = data.get("type")
        properties = data.get("data")
        if inst is None:
            inst = cls.cache.setdefault(self, cls())
        cls.cache[self] = inst
        inst.bind(self, data)
        inst.__type = type_
        pair = inst.pair
        if pair is not None:
            pair._Rel__type = type_
        if properties is None:
            inst.__stale.clear()
            inst.__stale.add("properties")
        else:
            inst._PropertyContainer__properties.replace(properties)
            inst.__stale.clear()
        return inst

    def __init__(self, *type_, **properties):
        if len(type_) > 1:
            raise ValueError("Only one relationship type can be specified")
        PropertyContainer.__init__(self, **properties)
        self.__type = type_[0] if type_ else None
        self.__stale = set()

    def __repr__(self):
        from py2neo.cypher import Representation
        r = Representation()
        if self.bound:
            r.write_rel(self, "r" + ustr(self._id))
        else:
            r.write_rel(self)
        return repr(r)

    def __eq__(self, other):
        if other is None:
            return False
        other = Rel.cast(other)
        if self.bound and other.bound:
            return self.resource == other.resource
        else:
            return self.type == other.type and self.properties == other.properties

    def __pos__(self):
        return self

    def __neg__(self):
        if self.pair is None:
            self.pair = self.pair_class()
            self.pair._Bindable__resource = self._Bindable__resource
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

    def bind(self, uri, metadata=None):
        PropertyContainer.bind(self, uri, metadata)
        self.cache[uri] = self
        pair = self.pair
        if pair is not None:
            PropertyContainer.bind(pair, uri, metadata)
            # make sure we're using exactly the same resource object
            # (maybe could write a Bindable.multi_bind classmethod
            pair._Bindable__resource = self.resource
            pair.cache[uri] = pair

    @property
    def exists(self):
        """ Detects whether this Rel still exists in the database.
        """
        try:
            self.resource.get()
        except GraphError as err:
            if err.response.status_code == NOT_FOUND:
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
            self.pull()
        return super(Rel, self).properties

    def pull(self):
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
        from py2neo.cypher import Representation
        r = Representation()
        r.write_path(self)
        return repr(r)

    def __eq__(self, other):
        return self.nodes == other.nodes and self.rels == other.rels

    def __ne__(self, other):
        return not self.__eq__(other)

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
    def end_node(self):
        return self.__nodes[-1]

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
    def rels(self):
        """ Return a tuple of all the rels which make up this path.
        """
        return self.__rels

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
        from py2neo.cypher import Representation
        r = Representation()
        if self.bound:
            r.write_relationship(self, "r" + ustr(self._id))
        else:
            r.write_relationship(self)
        return repr(r)

    def __len__(self):
        return self.rel.__len__()

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


from py2neo.deprecated import *
