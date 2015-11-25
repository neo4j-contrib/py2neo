#!/usr/bin/env python
# -*- encoding: utf-8 -*-

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


from __future__ import division, unicode_literals

import base64
from io import StringIO
import re
from warnings import warn
import webbrowser

from py2neo import __version__
from py2neo.compat import integer, string, ustr, xstr
from py2neo.env import NEO4J_AUTH, NEO4J_URI
from py2neo.error import BindError, GraphError, JoinError, Unauthorized
from py2neo.packages.httpstream import http, ClientError, ServerError, \
    Resource as _Resource, ResourceTemplate as _ResourceTemplate
from py2neo.packages.httpstream.http import JSONResponse, user_agent
from py2neo.packages.httpstream.numbers import UNAUTHORIZED
from py2neo.packages.httpstream.packages.urimagic import URI
from py2neo.primitive import \
    Node as PrimitiveNode, \
    Relationship as PrimitiveRelationship, \
    Path as PrimitivePath
from py2neo.types import cast_property
from py2neo.util import is_collection, round_robin, version_tuple, \
    raise_from, ThreadLocalWeakValueDictionary, deprecated


__all__ = ["Graph", "Node", "Relationship", "Path", "NodePointer", "Subgraph",
           "ServiceRoot",
           "authenticate", "familiar", "rewrite",
           "Bindable", "Resource", "ResourceTemplate"]


PRODUCT = ("py2neo", __version__)

NON_ALPHA_NUM = re.compile("[^0-9A-Za-z_]")
SIMPLE_NAME = re.compile(r"[A-Za-z_][0-9A-Za-z_]*")

http.default_encoding = "UTF-8"

_headers = {
    None: [
        ("User-Agent", user_agent(PRODUCT)),
        ("X-Stream", "true"),
    ],
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
    """ Set HTTP basic authentication values for specified `host_port` for use
    with both Neo4j 2.2 built-in authentication as well as if a database server
    is behind (for example) an Apache proxy. The code below shows a simple example::

        from py2neo import authenticate, Graph

        # set up authentication parameters
        authenticate("camelot:7474", "arthur", "excalibur")

        # connect to authenticated graph database
        graph = Graph("http://camelot:7474/db/data/")

    Note: a `host_port` can be either a server name or a server name and port
    number but must match exactly that used within the Graph
    URI.

    :arg host_port: the host and optional port requiring authentication
        (e.g. "bigserver", "camelot:7474")
    :arg user_name: the user name to authenticate as
    :arg password: the password
    """
    credentials = (user_name + ":" + password).encode("UTF-8")
    value = 'Basic ' + base64.b64encode(credentials).decode("ASCII")
    _add_header("Authorization", value, host_port=host_port)


def familiar(*objects):
    """ Check all objects belong to the same remote service.

    :arg objects: Bound objects to compare.
    :return: :const:`True` if all objects belong to the same remote service,
             :const:`False` otherwise.
    """
    service_roots = set()
    for obj in objects:
        if not obj.bound:
            raise ValueError("Can only determine familiarity of bound objects")
        service_roots.add(obj.service_root)
        if len(service_roots) > 1:
            return False
    return True


def rewrite(from_scheme_host_port, to_scheme_host_port):
    """ Automatically rewrite all URIs directed to the scheme, host and port
    specified in `from_scheme_host_port` to that specified in
    `to_scheme_host_port`.

    As an example::

        from py2neo import rewrite
        # implicitly convert all URIs beginning with <http://localhost:7474>
        # to instead use <https://dbserver:9999>
        rewrite(("http", "localhost", 7474), ("https", "dbserver", 9999))

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


def node_like(obj):
    from py2neo.batch import Job
    return obj is None or isinstance(obj, (Node, NodePointer, Job))


def coalesce(n, m):
    # Attempt to unify two nodes together into a single node.
    if not node_like(n) or not node_like(m):
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


class Resource(_Resource):
    """ Base class for all local resources mapped to remote counterparts.
    """

    #: The class of error raised by failure responses from this resource.
    error_class = GraphError

    def __init__(self, uri, metadata=None, headers=None):
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
        self._headers = dict(headers or {})
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
        self.__ref = NotImplemented

    @property
    def graph(self):
        """ The parent graph of this resource.

        :rtype: :class:`.Graph`
        """
        return self.__service_root.graph

    @property
    def headers(self):
        """ The HTTP headers sent with this resource.
        """
        headers = _get_headers(self.__uri__.host_port)
        headers.update(self._headers)
        return headers

    @property
    def metadata(self):
        """ Metadata received in the last HTTP response.
        """
        if self.__last_get_response is None:
            if self.__initial_metadata is not None:
                return self.__initial_metadata
            self.get()
        return self.__last_get_response.content

    def resolve(self, reference, strict=True):
        """ Resolve a URI reference against the URI for this resource,
        returning a new resource represented by the new target URI.

        :arg reference: Relative URI to resolve.
        :arg strict: Strict mode flag.
        :rtype: :class:`.Resource`
        """
        return Resource(_Resource.resolve(self, reference, strict).uri)

    @property
    def service_root(self):
        """ The root service associated with this resource.

        :return: :class:`.ServiceRoot`
        """
        return self.__service_root

    def get(self, headers=None, redirect_limit=5, **kwargs):
        """ Perform an HTTP GET to this resource.

        :arg headers: Extra headers to pass in the request.
        :arg redirect_limit: Maximum number of times to follow redirects.
        :arg kwargs: Other arguments to pass to the underlying `httpstream` method.
        :rtype: :class:`httpstream.Response`
        :raises: :class:`py2neo.GraphError`
        """
        headers = dict(self.headers, **(headers or {}))
        kwargs.update(cache=True)
        try:
            response = self.__base.get(headers=headers, redirect_limit=redirect_limit, **kwargs)
        except (ClientError, ServerError) as error:
            if error.status_code == UNAUTHORIZED:
                raise Unauthorized(self.uri.string)
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
        """ Perform an HTTP PUT to this resource.

        :arg body: The payload of this request.
        :arg headers: Extra headers to pass in the request.
        :arg kwargs: Other arguments to pass to the underlying `httpstream` method.
        :rtype: :class:`httpstream.Response`
        :raises: :class:`py2neo.GraphError`
        """
        headers = dict(self.headers, **(headers or {}))
        try:
            response = self.__base.put(body, headers, **kwargs)
        except (ClientError, ServerError) as error:
            if error.status_code == UNAUTHORIZED:
                raise Unauthorized(self.uri.string)
            if isinstance(error, JSONResponse):
                content = dict(error.content, request=error.request, response=error)
            else:
                content = {}
            message = content.pop("message", "HTTP PUT returned response %s" % error.status_code)
            raise_from(self.error_class(message, **content), error)
        else:
            return response

    def post(self, body=None, headers=None, **kwargs):
        """ Perform an HTTP POST to this resource.

        :arg body: The payload of this request.
        :arg headers: Extra headers to pass in the request.
        :arg kwargs: Other arguments to pass to the underlying `httpstream` method.
        :rtype: :class:`httpstream.Response`
        :raises: :class:`py2neo.GraphError`
        """
        headers = dict(self.headers, **(headers or {}))
        try:
            response = self.__base.post(body, headers, **kwargs)
        except (ClientError, ServerError) as error:
            if error.status_code == UNAUTHORIZED:
                raise Unauthorized(self.uri.string)
            if isinstance(error, JSONResponse):
                content = dict(error.content, request=error.request, response=error)
            else:
                content = {}
            message = content.pop("message", "HTTP POST returned response %s" % error.status_code)
            raise_from(self.error_class(message, **content), error)
        else:
            return response

    def delete(self, headers=None, **kwargs):
        """ Perform an HTTP DELETE to this resource.

        :arg headers: Extra headers to pass in the request.
        :arg kwargs: Other arguments to pass to the underlying `httpstream` method.
        :rtype: :class:`httpstream.Response`
        :raises: :class:`py2neo.GraphError`
        """
        headers = dict(self.headers, **(headers or {}))
        try:
            response = self.__base.delete(headers, **kwargs)
        except (ClientError, ServerError) as error:
            if error.status_code == UNAUTHORIZED:
                raise Unauthorized(self.uri.string)
            if isinstance(error, JSONResponse):
                content = dict(error.content, request=error.request, response=error)
            else:
                content = {}
            message = content.pop("message", "HTTP DELETE returned response %s" % error.status_code)
            raise_from(self.error_class(message, **content), error)
        else:
            return response


class ResourceTemplate(_ResourceTemplate):
    """ A factory class for producing :class:`.Resource` objects dynamically
    based on a template URI.
    """

    #: The class of error raised by failure responses from resources produced by this template.
    error_class = GraphError

    def expand(self, **values):
        """ Produce a resource instance by substituting values into the
        stored template URI.

        :arg values: A set of named values to plug into the template URI.
        :rtype: :class:`.Resource`
        """
        resource = Resource(self.uri_template.expand(**values))
        resource.error_class = self.error_class
        return resource


class Bindable(object):
    """ Base class for objects that can be optionally bound to a remote resource. This
    class is essentially a container for a :class:`.Resource` instance.
    """

    #: The class of error raised by failure responses from the contained resource.
    error_class = GraphError

    __resource__ = None

    def __eq__(self, other):
        try:
            return self.bound and other.bound and self.uri == other.uri
        except AttributeError:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def bind(self, uri, metadata=None):
        """ Associate this «class.lower» with a remote resource.

        :arg uri: The URI identifying the remote resource to which to bind.
        :arg metadata: Dictionary of initial metadata to attach to the contained resource.

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
        """ :const:`True` if this object is bound to a remote resource,
        :const:`False` otherwise.
        """
        return self.__resource__ is not None

    @property
    def graph(self):
        """ The graph associated with the remote resource.

        :rtype: :class:`.Graph`
        """
        return self.service_root.graph

    @property
    def cypher(self):
        """ The Cypher engine associated with the remote resource.

        :rtype: :class:`.CypherEngine`
        """
        return self.service_root.graph.cypher

    @property
    def ref(self):
        """ The URI of the remote resource relative to its graph.

        :rtype: string
        """
        return self.resource.ref

    @property
    def resource(self):
        """ The remote resource to which this object is bound.

        :rtype: :class:`.Resource`
        :raises: :class:`py2neo.BindError`
        """
        if self.bound:
            return self.__resource__
        else:
            raise BindError("Local entity is not bound to a remote entity")

    @property
    def service_root(self):
        """ The root service associated with the remote resource.

        :return: :class:`.ServiceRoot`
        """
        return self.resource.service_root

    def unbind(self):
        """ Detach this object from any remote resource.
        """
        self.__resource__ = None

    @property
    def uri(self):
        """ The full URI of the remote resource.
        """
        resource = self.resource
        try:
            return resource.uri
        except AttributeError:
            return resource.uri_template


class ServiceRoot(object):
    """ Wrapper for the base REST resource exposed by a running Neo4j
    server, corresponding to the ``/`` URI. If no URI is supplied to
    the constructor, a value is taken from the ``NEO4J_URI`` environment
    variable (if set) otherwise a default of ``http://localhost:7474/``
    is used.
    """

    __instances = {}

    __authentication = None
    __graph = None

    def __new__(cls, uri=None):
        if uri is None:
            uri = NEO4J_URI
        uri = ustr(uri)
        if not uri.endswith("/"):
            uri += "/"
        try:
            inst = cls.__instances[uri]
        except KeyError:
            if NEO4J_AUTH:
                user_name, password = NEO4J_AUTH.partition(":")[0::2]
                authenticate(URI(uri).host_port, user_name, password)
            inst = super(ServiceRoot, cls).__new__(cls)
            inst.__resource = Resource(uri)
            inst.__graph = None
            cls.__instances[uri] = inst
        return inst

    def __repr__(self):
        return "<ServiceRoot uri=%r>" % self.uri.string
    
    def __eq__(self, other):
        try:
            return self.uri == other.uri
        except AttributeError:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash(self.uri)

    @property
    def graph(self):
        """ The graph exposed by this service.

        :rtype: :class:`.Graph`
        """
        if self.__graph is None:
            try:
                # The graph URI used to be determined via
                # discovery but another HTTP call sometimes
                # caused problems in the middle of other
                # operations (such as hydration) when using
                # concurrent code. Therefore, the URI is now
                # constructed manually.
                #
                # uri = self.resource.metadata["data"]
                uri = self.uri.string + "db/data/"
            except KeyError:
                raise GraphError("No graph available for service <%s>" % self.uri)
            else:
                self.__graph = Graph(uri)
        return self.__graph

    @property
    def resource(self):
        """ The contained resource object for this instance.

        :rtype: :class:`py2neo.Resource`
        """
        return self.__resource

    @property
    def uri(self):
        """ The full URI of the contained resource.
        """
        return self.resource.uri


class Graph(Bindable):
    """ The `Graph` class provides a wrapper around the
    `REST API <http://docs.neo4j.org/chunked/stable/rest-api.html>`_ exposed
    by a running Neo4j database server and is identified by the base URI of
    the graph database. If no URI is specified, a default value is taken from
    the ``NEO4J_URI`` environment variable. If this is not set, a default of
    `http://localhost:7474/db/data/` is assumed. Therefore, the simplest way
    to connect to a running service is to use::

        >>> from py2neo import Graph
        >>> graph = Graph()

    An explicitly specified graph database URI can also be passed to the
    constructor as a string::

        >>> other_graph = Graph("http://camelot:1138/db/data/")

    If the database server requires authorisation, the credentials can also
    be specified within the URI::

        >>> secure_graph = Graph("http://arthur:excalibur@camelot:1138/db/data/")

    Once obtained, the `Graph` instance provides direct or indirect access
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
        elif isinstance(obj, (Node, NodePointer, Path, Relationship, Subgraph)):
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
        """ A :class:`py2neo.batch.BatchResource` instance attached to this
        graph. This resource exposes methods for submitting iterable
        collections of :class:`py2neo.batch.Job` objects to the server and
        will often be used indirectly via classes such as
        :class:`py2neo.batch.PullBatch` or :class:`py2neo.batch.PushBatch`.

        :rtype: :class:`py2neo.batch.BatchResource`

        """
        if self.__batch is None:
            from py2neo.batch import BatchResource
            self.__batch = BatchResource(self.resource.metadata["batch"])
        return self.__batch

    @property
    def cypher(self):
        """ The Cypher execution resource for this graph providing access to
        all Cypher functionality for the underlying database, both simple
        and transactional.

        ::

            >>> from py2neo import Graph
            >>> graph = Graph()
            >>> graph.cypher.execute("CREATE (a:Person {name:{N}})", {"N": "Alice"})

        :rtype: :class:`py2neo.cypher.CypherEngine`

        """
        if self.__cypher is None:
            from py2neo.cypher import CypherEngine
            metadata = self.resource.metadata
            self.__cypher = CypherEngine(metadata.get("transaction"))
        return self.__cypher

    def create(self, *entities):
        """ Create one or more remote nodes, relationships or paths in a
        single transaction. The entity values provided must be either
        existing entity objects (such as nodes or relationships) or values
        that can be cast to them.

        For example, to create a remote node from a local :class:`Node` object::

            from py2neo import Graph, Node
            graph = Graph()
            alice = Node("Person", name="Alice")
            graph.create(alice)

        Then, create a second node and a relationship connecting both nodes::

            german, speaks = graph.create({"name": "German"}, (alice, "SPEAKS", 0))

        This second example shows how :class:`dict` and :class:`tuple` objects
        can also be used to create nodes and relationships respectively. The
        zero value in the relationship tuple references the zeroth item created
        within that transaction, i.e. the "German" node.

        .. note::
            If an object is passed to this method that is already bound to
            a remote entity, that argument will be ignored and nothing will
            be created.

        :arg entities: One or more existing graph entities or values that
                       can be cast to entities.
        :return: A tuple of all entities created (or ignored) of the same
                 length and order as the arguments passed in.
        :rtype: tuple

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
        """ Create one or more unique paths or relationships in a single
        transaction. This is similar to :meth:`create` but uses a Cypher
        `CREATE UNIQUE <http://docs.neo4j.org/chunked/stable/query-create-unique.html>`_
        clause to ensure that only relationships that do not already exist are created.
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
        self.cypher.execute("MATCH (a) OPTIONAL MATCH (a)-[r]->() DELETE r, a")

    def exists(self, *entities):
        """ Determine whether a number of graph entities all exist within the database.
        """
        tx = self.cypher.begin()
        for entity in entities:
            if isinstance(entity, Node):
                tx.append("MATCH (a) WHERE id(a) = {x} RETURN count(a)", x=entity)
            elif isinstance(entity, Relationship):
                tx.append("MATCH ()-[r]->() WHERE id(r) = {x} RETURN count(r)", x=entity)
            elif isinstance(entity, Path):
                for node in entity.nodes():
                    tx.append("MATCH (a) WHERE id(a) = {x} RETURN count(a)", x=node)
                for rel in entity.relationships():
                    tx.append("MATCH ()-[r]->() WHERE id(r) = {x} RETURN count(r)", x=rel)
            elif isinstance(entity, Subgraph):
                for node in entity.nodes:
                    tx.append("MATCH (a) WHERE id(a) = {x} RETURN count(a)", x=node)
                for rel in entity.relationships:
                    tx.append("MATCH ()-[r]->() WHERE id(r) = {x} RETURN count(r)", x=rel)
            else:
                raise TypeError("Cannot determine existence of non-entity")
        count = len(tx.statements)
        return sum(result[0][0] for result in tx.commit()) == count

    def find(self, label, property_key=None, property_value=None, limit=None):
        """ Iterate through a set of labelled nodes, optionally filtering
        by property key and value
        """
        if not label:
            raise ValueError("Empty label")
        from py2neo.cypher.lang import cypher_escape
        if property_key is None:
            statement = "MATCH (n:%s) RETURN n,labels(n)" % cypher_escape(label)
            parameters = {}
        else:
            statement = "MATCH (n:%s {%s:{V}}) RETURN n,labels(n)" % (
                cypher_escape(label), cypher_escape(property_key))
            parameters = {"V": property_value}
        if limit:
            statement += " LIMIT %s" % limit
        response = self.cypher.post(statement, parameters)
        for record in response["data"]:
            dehydrated = record[0]
            dehydrated.setdefault("metadata", {})["labels"] = record[1]
            yield self.hydrate(dehydrated)

    def find_one(self, label, property_key=None, property_value=None):
        """ Find a single node by label and optional property. This method is
        intended to be used with a unique constraint and does not fail if more
        than one matching node is found.
        """
        for node in self.find(label, property_key, property_value, limit=1):
            return node

    def hydrate(self, data):
        """ Hydrate a dictionary of data to produce a :class:`.Node`,
        :class:`.Relationship` or other graph object instance. The
        data structure and values expected are those produced by the
        `REST API <http://neo4j.com/docs/stable/rest-api.html>`__.

        :arg data: dictionary of data to hydrate
        
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
            elif "exception" in data and ("stacktrace" in data or "stackTrace" in data):
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

    def match(self, start_node=None, rel_type=None, end_node=None, bidirectional=False, limit=None):
        """ Return an iterator for all relationships matching the
        specified criteria.

        For example, to find all of Alice's friends::

            for rel in graph.match(start_node=alice, rel_type="FRIEND"):
                print(rel.end_node.properties["name"])

        :arg start_node: :attr:`~py2neo.Node.bound` start :class:`~py2neo.Node` to match or
                           :const:`None` if any
        :arg rel_type: type of relationships to match or :const:`None` if any
        :arg end_node: :attr:`~py2neo.Node.bound` end :class:`~py2neo.Node` to match or
                         :const:`None` if any
        :arg bidirectional: :const:`True` if reversed relationships should also be included
        :arg limit: maximum number of relationships to match or :const:`None` if no limit
        :return: matching relationships
        :rtype: generator
        """
        if start_node is None and end_node is None:
            statement = "MATCH (a)"
            parameters = {}
        elif end_node is None:
            statement = "MATCH (a) WHERE id(a)={A}"
            start_node = Node.cast(start_node)
            if not start_node.bound:
                raise TypeError("Nodes for relationship match end points must be bound")
            parameters = {"A": start_node}
        elif start_node is None:
            statement = "MATCH (b) WHERE id(b)={B}"
            end_node = Node.cast(end_node)
            if not end_node.bound:
                raise TypeError("Nodes for relationship match end points must be bound")
            parameters = {"B": end_node}
        else:
            statement = "MATCH (a) WHERE id(a)={A} MATCH (b) WHERE id(b)={B}"
            start_node = Node.cast(start_node)
            end_node = Node.cast(end_node)
            if not start_node.bound or not end_node.bound:
                raise TypeError("Nodes for relationship match end points must be bound")
            parameters = {"A": start_node, "B": end_node}
        if rel_type is None:
            rel_clause = ""
        elif is_collection(rel_type):
            rel_clause = ":" + "|:".join("`{0}`".format(_) for _ in rel_type)
        else:
            rel_clause = ":`{0}`".format(rel_type)
        if bidirectional:
            statement += " MATCH (a)-[r" + rel_clause + "]-(b) RETURN r"
        else:
            statement += " MATCH (a)-[r" + rel_clause + "]->(b) RETURN r"
        if limit is not None:
            statement += " LIMIT {0}".format(int(limit))
        results = self.cypher.execute(statement, parameters)
        for result in results:
            yield result.r

    def match_one(self, start_node=None, rel_type=None, end_node=None, bidirectional=False):
        """ Return a single relationship matching the
        specified criteria. See :meth:`~py2neo.Graph.match` for
        argument details.
        """
        rels = list(self.match(start_node, rel_type, end_node,
                               bidirectional, 1))
        if rels:
            return rels[0]
        else:
            return None

    def merge(self, label, property_key=None, property_value=None, limit=None):
        """ Match or create a node by label and optional property and return
        all matching nodes.
        """
        if not label:
            raise ValueError("Empty label")
        from py2neo.cypher.lang import cypher_escape
        if property_key is None:
            statement = "MERGE (n:%s) RETURN n,labels(n)" % cypher_escape(label)
            parameters = {}
        elif not isinstance(property_key, string):
            raise TypeError("Property key must be textual")
        elif property_value is None:
            raise ValueError("Both key and value must be specified for a property")
        else:
            statement = "MERGE (n:%s {%s:{V}}) RETURN n,labels(n)" % (
                cypher_escape(label), cypher_escape(property_key))
            parameters = {"V": cast_property(property_value)}
        if limit:
            statement += " LIMIT %s" % limit
        response = self.cypher.post(statement, parameters)
        for record in response["data"]:
            dehydrated = record[0]
            dehydrated.setdefault("metadata", {})["labels"] = record[1]
            yield self.hydrate(dehydrated)

    def merge_one(self, label, property_key=None, property_value=None):
        """ Match or create a node by label and optional property and return a
        single matching node. This method is intended to be used with a unique
        constraint and does not fail if more than one matching node is found.

            >>> graph = Graph()
            >>> person = graph.merge_one("Person", "email", "bob@example.com")

        """
        for node in self.merge(label, property_key, property_value, limit=1):
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
        if self.__node_labels is None:
            self.__node_labels = Resource(self.uri.string + "labels")
        return frozenset(self.__node_labels.get().content)

    def open_browser(self):
        """ Open a page in the default system web browser pointing at
        the Neo4j browser application for this graph.
        """
        webbrowser.open(self.service_root.resource.uri.string)

    @property
    def order(self):
        """ The number of nodes in this graph.
        """
        statement = "MATCH (n) RETURN count(n)"
        return self.cypher.evaluate(statement)

    def pull(self, *entities):
        """ Pull data to one or more entities from their remote counterparts.
        """
        if entities:
            from py2neo.batch.pull import PullBatch
            batch = PullBatch(self)
            for entity in entities:
                batch.append(entity)
            batch.pull()

    def push(self, *entities):
        """ Push data from one or more entities to their remote counterparts.
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
            except GraphError as error:
                if error.exception == "RelationshipNotFoundException":
                    raise ValueError("Relationship with ID %s not found" % id_)
                else:
                    raise

    @property
    def relationship_types(self):
        """ The set of relationship types currently defined within the graph.
        """
        if self.__relationship_types is None:
            self.__relationship_types = Resource(self.uri.string + "relationship/types")
        return frozenset(self.__relationship_types.get().content)

    @property
    def schema(self):
        """ The schema resource for this graph.

        :rtype: :class:`SchemaResource <py2neo.schema.SchemaResource>`
        """
        if self.__schema is None:
            from py2neo.schema import SchemaResource
            self.__schema = SchemaResource(self.uri.string + "schema")
        return self.__schema

    @property
    def size(self):
        """ The number of relationships in this graph.
        """
        statement = "MATCH ()-[r]->() RETURN count(r)"
        return self.cypher.evaluate(statement)


class Node(Bindable, PrimitiveNode):
    """ A graph node that may optionally be bound to a remote counterpart
    in a Neo4j database. Nodes may contain a set of named :attr:`~py2neo.Node.properties` and
    may have one or more :attr:`labels <py2neo.Node.labels>` applied to them::

        >>> from py2neo import Node
        >>> alice = Node("Person", name="Alice")
        >>> banana = Node("Fruit", "Food", colour="yellow", tasty=True)

    All positional arguments passed to the constructor are interpreted
    as labels and all keyword arguments as properties. It is also possible to
    construct Node instances from other data types (such as a dictionary)
    by using the :meth:`.Node.cast` class method::

        >>> bob = Node.cast({"name": "Bob Robertson", "age": 44})

    Labels and properties can be accessed and modified using the
    :attr:`labels <py2neo.Node.labels>` and :attr:`~py2neo.Node.properties`
    attributes respectively. The former is an instance of :class:`.LabelSet`,
    which extends the built-in :class:`set` class, and the latter is an
    instance of :class:`.PropertySet` which extends :class:`dict`.

        >>> alice["name"]
        'Alice'
        >>> alice.labels()
        {'Person'}
        >>> alice.add_label("Employee")
        >>> alice["employee_no"] = 3456
        >>> alice
        <Node labels={'Employee', 'Person'} properties={'employee_no': 3456, 'name': 'Alice'}>

    One of the core differences between a :class:`.PropertySet` and a standard
    dictionary is in how it handles :const:`None` and missing values. As with actual Neo4j
    properties, missing values and those equal to :const:`None` are equivalent.
    """

    cache = ThreadLocalWeakValueDictionary()

    __id = None

    @staticmethod
    def cast(*args, **kwargs):
        """ Cast the arguments provided to a :class:`.Node` (or
        :class:`.NodePointer`). The following combinations of
        arguments are possible::

            >>> Node.cast(None)
            >>> Node.cast()
            <Node labels=set() properties={}>
            >>> Node.cast("Person")
            <Node labels={'Person'} properties={}>
            >>> Node.cast(name="Alice")
            <Node labels=set() properties={'name': 'Alice'}>
            >>> Node.cast("Person", name="Alice")
            <Node labels={'Person'} properties={'name': 'Alice'}>
            >>> Node.cast(123)
            <NodePointer address=123>
            >>> Node.cast({"name": "Alice"})
            <Node labels=set() properties={'name': 'Alice'}>
            >>> node = Node("Person", name="Alice")
            >>> Node.cast(node)
            <Node labels={'Person'} properties={'name': 'Alice'}>

        """
        if len(args) == 1 and not kwargs:
            from py2neo.batch import Job
            arg = args[0]
            if arg is None:
                return None
            elif isinstance(arg, (Node, NodePointer, Job)):
                return arg
            elif isinstance(arg, integer):
                return NodePointer(arg)

        inst = Node()

        def apply(x):
            if isinstance(x, dict):
                inst.update(x)
            elif is_collection(x):
                for item in x:
                    apply(item)
            elif isinstance(x, string):
                inst.add_label(ustr(x))
            else:
                raise TypeError("Cannot cast %s to Node" % repr(tuple(map(type, args))))

        for arg in args:
            apply(arg)
        inst.update(kwargs)
        return inst

    @classmethod
    def hydrate(cls, data, inst=None):
        """ Hydrate a dictionary of data to produce a :class:`.Node` instance.
        The data structure and values expected are those produced by the
        `REST API <http://neo4j.com/docs/stable/rest-api-nodes.html#rest-api-get-node>`__
        although only the ``self`` value is required.

        :arg data: dictionary of data to hydrate
        :arg inst: an existing :class:`.Node` instance to overwrite with new values

        """
        self = data["self"]
        if inst is None:
            new_inst = cls()
            new_inst.__stale.update({"labels", "properties"})
            inst = cls.cache.setdefault(self, new_inst)
            # The check below is a workaround for http://bugs.python.org/issue19542
            # See also: https://github.com/nigelsmall/py2neo/issues/391
            if inst is None:
                inst = cls.cache[self] = new_inst
        cls.cache[self] = inst
        inst.bind(self, data)
        if "data" in data:
            inst.__stale.discard("properties")
            properties = data["data"]
            properties.update(inst)
            inst.clear()
            inst.update(properties)
        if "metadata" in data:
            inst.__stale.discard("labels")
            metadata = data["metadata"]
            labels = set(metadata["labels"])
            labels.update(inst.labels())
            inst.clear_labels()
            inst.update_labels(labels)
        return inst

    def __init__(self, *labels, **properties):
        PrimitiveNode.__init__(self, *labels, **properties)
        self.__stale = set()

    def __repr__(self):
        s = [self.__class__.__name__]
        if self.bound:
            s.append("graph=%r" % self.graph.uri.string)
            s.append("ref=%r" % self.ref)
            if "labels" in self.__stale:
                s.append("labels=?")
            else:
                s.append("labels=%r" % set(self.labels()))
            if "properties" in self.__stale:
                s.append("properties=?")
            else:
                s.append("properties=%r" % dict(self))
        else:
            s.append("labels=%r" % set(self.labels()))
            s.append("properties=%r" % dict(self))
        return "<" + " ".join(s) + ">"

    def __str__(self):
        return xstr(self.__unicode__())

    def __unicode__(self):
        from py2neo.cypher import CypherWriter
        s = StringIO()
        writer = CypherWriter(s)
        if self.bound:
            writer.write_node(self, "n" + ustr(self._id))
        else:
            writer.write_node(self)
        return s.getvalue()

    def __eq__(self, other):
        if other is None:
            return False
        other = Node.cast(other)
        if self.bound and other.bound:
            return self.resource == other.resource
        else:
            return PrimitiveNode.__eq__(self, other)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        if self.bound:
            return hash(self.resource.uri)
        else:
            return PrimitiveNode.__hash__(self)

    def __add__(self, other):
        return Path(self, other)

    def __getitem__(self, item):
        if self.bound and "properties" in self.__stale:
            self.refresh()
        return PrimitiveNode.__getitem__(self, item)

    @property
    def _id(self):
        """ The internal ID of this node within the database.
        """
        if self.__id is None:
            self.__id = int(self.uri.path.segments[-1])
        return self.__id

    @property
    def ref(self):
        """ The URI of this node relative to its graph.

        :rtype: string
        """
        return "node/%s" % self._id

    def bind(self, uri, metadata=None):
        """ Associate this node with a remote node.

        :arg uri: The URI identifying the remote node to which to bind.
        :arg metadata: Dictionary of initial metadata to attach to the contained resource.

        """
        Bindable.bind(self, uri, metadata)
        self.cache[uri] = self

    def degree(self):
        """ The number of relationships attached to this node.
        """
        return self.cypher.evaluate("MATCH (a)-[r]-() WHERE id(a)={n} RETURN count(r)", n=self)

    @deprecated("Node.exists() is deprecated, use graph.exists(node) instead")
    def exists(self):
        """ :const:`True` if this node exists in the database,
        :const:`False` otherwise.
        """
        return self.graph.exists(self)

    def labels(self):
        """ The set of labels attached to this node.
        """
        if self.bound and "labels" in self.__stale:
            self.refresh()
        return PrimitiveNode.labels(self)

    @deprecated("Node.match() is deprecated, use graph.match(node, ...) instead")
    def match(self, rel_type=None, other_node=None, limit=None):
        """ Return an iterator for all relationships attached to this node
        that match the specified criteria. See :meth:`.Graph.match` for
        argument details.
        """
        return self.graph.match(self, rel_type, other_node, True, limit)

    @deprecated("Node.match_incoming() is deprecated, use graph.match(node, ...) instead")
    def match_incoming(self, rel_type=None, start_node=None, limit=None):
        """ Return an iterator for all incoming relationships to this node
        that match the specified criteria. See :meth:`.Graph.match` for
        argument details.
        """
        return self.graph.match(start_node, rel_type, self, False, limit)

    @deprecated("Node.match_outgoing() is deprecated, use graph.match(node, ...) instead")
    def match_outgoing(self, rel_type=None, end_node=None, limit=None):
        """ Return an iterator for all outgoing relationships from this node
        that match the specified criteria. See :meth:`.Graph.match` for
        argument details.
        """
        return self.graph.match(self, rel_type, end_node, False, limit)

    @property
    @deprecated("Node.properties is deprecated, use dict(node) instead")
    def properties(self):
        """ The set of properties attached to this node. Properties
        can also be read from and written to any :class:`Node`
        by using the index syntax directly. This means
        the following statements are equivalent::

            node.properties["name"] = "Alice"
            node["name"] = "Alice"

        """
        if self.bound and "properties" in self.__stale:
            self.refresh()
        return dict(self)

    @deprecated("Node.pull() is deprecated, use graph.pull(node) instead")
    def pull(self):
        """ Pull data to this node from its remote counterpart. Consider
        using :meth:`.Graph.pull` instead for batches of nodes.
        """
        self.graph.pull(self)

    @deprecated("Node.push() is deprecated, use graph.push(node) instead")
    def push(self):
        """ Push data from this node to its remote counterpart. Consider
        using :meth:`.Graph.push` instead for batches of nodes.
        """
        self.graph.push(self)

    def refresh(self):
        # Non-destructive pull.
        # Note: this may fail if attempted against an entity mid-transaction
        # that has not yet been committed.
        query = "MATCH (a) WHERE id(a)={a} RETURN a,labels(a)"
        content = self.cypher.post(query, {"a": self._id})
        try:
            dehydrated, label_metadata = content["data"][0]
        except IndexError:
            raise GraphError("Node with ID %s not found" % self._id)
        else:
            dehydrated.setdefault("metadata", {})["labels"] = label_metadata
            Node.hydrate(dehydrated, self)

    def unbind(self):
        """ Detach this node from any remote counterpart.
        """
        try:
            del self.cache[self.uri]
        except KeyError:
            pass
        Bindable.unbind(self)
        self.__id = None


class NodePointer(object):
    """ Pointer to a :class:`Node` object. This can be used in a batch
    context to point to a node not yet created.
    """

    #: The address or index to which this pointer points.
    address = None

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


class Path(PrimitivePath):
    """ A sequence of nodes connected by relationships that may
    optionally be bound to remote counterparts in a Neo4j database.

        >>> from py2neo import Node, Path
        >>> alice, bob, carol = Node(name="Alice"), Node(name="Bob"), Node(name="Carol")
        >>> abc = Path(alice, "KNOWS", bob, Relationship(carol, "KNOWS", bob), carol)
        >>> abc
        <Path order=3 size=2>
        >>> abc.nodes
        (<Node labels=set() properties={'name': 'Alice'}>,
         <Node labels=set() properties={'name': 'Bob'}>,
         <Node labels=set() properties={'name': 'Carol'}>)
        >>> abc.relationships
        (<Relationship type='KNOWS' properties={}>,
         <Relationship type='KNOWS' properties={}>)
        >>> dave, eve = Node(name="Dave"), Node(name="Eve")
        >>> de = Path(dave, "KNOWS", eve)
        >>> de
        <Path order=2 size=1>
        >>> abcde = Path(abc, "KNOWS", de)
        >>> abcde
        <Path order=5 size=4>
        >>> for relationship in abcde.relationships():
        ...     print(relationship)
        ({name:"Alice"})-[:KNOWS]->({name:"Bob"})
        ({name:"Carol"})-[:KNOWS]->({name:"Bob"})
        ({name:"Carol"})-[:KNOWS]->({name:"Dave"})
        ({name:"Dave"})-[:KNOWS]->({name:"Eve"})

    """

    @classmethod
    def hydrate(cls, data, inst=None):
        """ Hydrate a dictionary of data to produce a :class:`.Path` instance.
        The data structure and values expected are those produced by the
        `REST API <http://neo4j.com/docs/stable/rest-api-graph-algos.html#rest-api-find-one-of-the-shortest-paths>`__.

        :arg data: dictionary of data to hydrate
        :arg inst: an existing :class:`.Path` instance to overwrite with new values

        """
        node_uris = data["nodes"]
        relationship_uris = data["relationships"]
        offsets = [(0, 1) if direction == "->" else (1, 0) for direction in data["directions"]]
        if inst is None:
            nodes = [Node.hydrate({"self": uri}) for uri in node_uris]
            relationships = [Relationship.hydrate({"self": uri,
                                                   "start": node_uris[i + offsets[i][0]],
                                                   "end": node_uris[i + offsets[i][1]]})
                             for i, uri in enumerate(relationship_uris)]
            inst = Path(*round_robin(nodes, relationships))
        else:
            for i, node in enumerate(inst.nodes()):
                uri = node_uris[i]
                Node.hydrate({"self": uri}, node)
            for i, relationship in enumerate(inst.relationships()):
                uri = relationship_uris[i]
                Relationship.hydrate({"self": uri,
                                      "start": node_uris[i + offsets[i][0]],
                                      "end": node_uris[i + offsets[i][1]]}, relationship)
        inst.__metadata = data
        return inst

    def __init__(self, *entities):
        entities = list(entities)
        for i, entity in enumerate(entities):
            if entity is None:
                entities[i] = Node()
            elif isinstance(entity, dict):
                entities[i] = Node(**entity)
        for i, entity in enumerate(entities):
            try:
                start_node = entities[i - 1].end_node()
                end_node = entities[i + 1].start_node()
            except (IndexError, AttributeError):
                pass
            else:
                if isinstance(entity, string):
                    entities[i] = Relationship(start_node, entity, end_node)
                elif isinstance(entity, tuple) and len(entity) == 2:
                    t, properties = entity
                    entities[i] = Relationship(start_node, t, end_node, **properties)
        PrimitivePath.__init__(self, *entities)

    def __repr__(self):
        s = [self.__class__.__name__]
        if self.bound:
            s.append("graph=%r" % self.graph.uri.string)
            s.append("start=%r" % self.start_node().ref)
            s.append("end=%r" % self.end_node().ref)
        s.append("order=%r" % self.order())
        s.append("size=%r" % self.size())
        return "<" + " ".join(s) + ">"

    def __str__(self):
        return xstr(self.__unicode__())

    def __unicode__(self):
        from py2neo.cypher import CypherWriter
        s = StringIO()
        writer = CypherWriter(s)
        writer.write_path(self)
        return s.getvalue()

    def append(self, *others):
        """ Join another path or relationship to the end of this path to form a new path.

        :arg others: Entities to join to the end of this path
        :rtype: :class:`.Path`
        """
        return Path(self, *others)

    @property
    def bound(self):
        """ :const:`True` if this path is bound to a remote counterpart,
        :const:`False` otherwise.
        """
        try:
            _ = self.service_root
        except BindError:
            return False
        else:
            return True

    @property
    @deprecated("Path.exists() is deprecated, use graph.exists(path) instead")
    def exists(self):
        """ :const:`True` if this path exists in the database,
        :const:`False` otherwise.
        """
        return self.graph.exists(*(self.nodes() + self.relationships()))

    @property
    def graph(self):
        """ The parent graph of this path.

        :rtype: :class:`.Graph`
        """
        return self.service_root.graph

    def pull(self):
        """ Pull data to all entities in this path from their remote counterparts.
        """
        self.graph.pull(self)

    def push(self):
        """ Push data from all entities in this path to their remote counterparts.
        """
        self.graph.push(self)

    @property
    def service_root(self):
        """ The root service associated with this path.

        :return: :class:`.ServiceRoot`
        """
        for relationship in self:
            try:
                return relationship.service_root
            except BindError:
                pass
        raise BindError("Local path is not bound to a remote path")

    def unbind(self):
        """ Detach all entities in this path
        from any remote counterparts.
        """
        for entity in self.relationships() + self.nodes():
            try:
                entity.unbind()
            except BindError:
                pass


class Relationship(Bindable, PrimitiveRelationship):
    """ A graph relationship that may optionally be bound to a remote counterpart
    in a Neo4j database. Relationships require a triple of start node, relationship
    type and end node and may also optionally be given one or more properties::

        >>> from py2neo import Node, Relationship
        >>> alice = Node("Person", name="Alice")
        >>> bob = Node("Person", name="Bob")
        >>> alice_knows_bob = Relationship(alice, "KNOWS", bob, since=1999)

    """

    cache = ThreadLocalWeakValueDictionary()

    __id = None

    @staticmethod
    def cast(*args, **kwargs):
        """ Cast the arguments provided to a :class:`.Relationship`. The
        following combinations of arguments are possible::

            >>> Relationship.cast(Node(), "KNOWS", Node())
            <Relationship type='KNOWS' properties={}>
            >>> Relationship.cast((Node(), "KNOWS", Node()))
            <Relationship type='KNOWS' properties={}>
            >>> Relationship.cast(Node(), "KNOWS", Node(), since=1999)
            <Relationship type='KNOWS' properties={'since': 1999}>
            >>> Relationship.cast(Node(), "KNOWS", Node(), {"since": 1999})
            <Relationship type='KNOWS' properties={'since': 1999}>
            >>> Relationship.cast((Node(), "KNOWS", Node(), {"since": 1999}))
            <Relationship type='KNOWS' properties={'since': 1999}>
            >>> Relationship.cast(Node(), ("KNOWS", {"since": 1999}), Node())
            <Relationship type='KNOWS' properties={'since': 1999}>
            >>> Relationship.cast((Node(), ("KNOWS", {"since": 1999}), Node()))
            <Relationship type='KNOWS' properties={'since': 1999}>

        """

        def get_type(r):
            if isinstance(r, string):
                return r
            elif hasattr(r, "type"):
                if callable(r.type):
                    return r.type()
                else:
                    return r.type
            elif isinstance(r, tuple) and len(r) == 2 and isinstance(r[0], string):
                return r[0]
            else:
                raise ValueError("Cannot determine relationship type from %r" % r)

        def get_properties(r):
            if isinstance(r, string):
                return {}
            elif hasattr(r, "type") and callable(r.type):
                return dict(r)
            elif hasattr(r, "properties"):
                return r.properties
            elif isinstance(r, tuple) and len(r) == 2 and isinstance(r[0], string):
                return dict(r[1])
            else:
                raise ValueError("Cannot determine properties from %r" % r)

        if len(args) == 1 and not kwargs:
            arg = args[0]
            if isinstance(arg, Relationship):
                return arg
            elif isinstance(arg, tuple):
                if len(arg) == 3:
                    start_node, t, end_node = arg
                    properties = get_properties(t)
                elif len(arg) == 4:
                    start_node, t, end_node, properties = arg
                    properties = dict(get_properties(t), **properties)
                else:
                    raise TypeError("Cannot cast relationship from {0}".format(arg))
            else:
                raise TypeError("Cannot cast relationship from {0}".format(arg))
        elif len(args) == 3:
            start_node, t, end_node = args
            properties = dict(get_properties(t), **kwargs)
        elif len(args) == 4:
            start_node, t, end_node, properties = args
            properties = dict(get_properties(t), **properties)
            properties.update(kwargs)
        else:
            raise TypeError("Cannot cast relationship from {0}".format((args, kwargs)))
        return Relationship(start_node, get_type(t), end_node, **properties)

    @classmethod
    def hydrate(cls, data, inst=None):
        """ Hydrate a dictionary of data to produce a :class:`.Relationship` instance.
        The data structure and values expected are those produced by the
        `REST API <http://neo4j.com/docs/stable/rest-api-relationships.html#rest-api-get-relationship-by-id>`__.

        :arg data: dictionary of data to hydrate
        :arg inst: an existing :class:`.Relationship` instance to overwrite with new values

        """
        self = data["self"]
        if inst is None:
            new_inst = cls(Node.hydrate({"self": data["start"]}),
                           data.get("type"),
                           Node.hydrate({"self": data["end"]}),
                           **data.get("data", {}))
            inst = cls.cache.setdefault(self, new_inst)
            # The check below is a workaround for http://bugs.python.org/issue19542
            # See also: https://github.com/nigelsmall/py2neo/issues/391
            if inst is None:
                inst = cls.cache[self] = new_inst
        else:
            Node.hydrate({"self": data["start"]}, inst.start_node())
            Node.hydrate({"self": data["end"]}, inst.end_node())
            inst._type = data.get("type")
            if "data" in data:
                inst.clear()
                inst.update(data["data"])
            else:
                inst.__stale.add("properties")
        cls.cache[self] = inst
        inst.bind(self, data)
        return inst

    def __init__(self, *nodes, **properties):
        n = []
        p = {}
        for value in nodes:
            if isinstance(value, string):
                n.append(value)
            elif isinstance(value, tuple) and len(value) == 2 and isinstance(value[0], string):
                t, props = value
                n.append(t)
                p.update(props)
            else:
                n.append(Node.cast(value))
        p.update(properties)
        PrimitiveRelationship.__init__(self, *n, **p)
        self.__stale = set()

    def __repr__(self):
        s = [self.__class__.__name__]
        if self.bound:
            s.append("graph=%r" % self.graph.uri.string)
            s.append("ref=%r" % self.ref)
            s.append("start=%r" % self.start_node().ref)
            s.append("end=%r" % self.end_node().ref)
            if self._type is None:
                s.append("type=?")
            else:
                s.append("type=%r" % self._type)
            if "properties" in self.__stale:
                s.append("properties=?")
            else:
                s.append("properties=%r" % dict(self))
        else:
            s.append("type=%r" % self._type)
            s.append("properties=%r" % dict(self))
        return "<" + " ".join(s) + ">"

    def __str__(self):
        return xstr(self.__unicode__())

    def __unicode__(self):
        from py2neo.cypher import CypherWriter
        s = StringIO()
        writer = CypherWriter(s)
        if self.bound:
            writer.write_full_relationship(self, "r" + ustr(self._id))
        else:
            writer.write_full_relationship(self)
        return s.getvalue()

    def __eq__(self, other):
        if other is None:
            return False
        other = Relationship.cast(other)
        if self.bound and other.bound:
            return self.resource == other.resource
        else:
            return PrimitiveRelationship.__eq__(self, other)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        if self.bound:
            return hash(self.resource.uri)
        else:
            return PrimitiveRelationship.__hash__(self)

    @property
    def _id(self):
        """ The internal ID of this relationship within the database.
        """
        if self.__id is None:
            self.__id = int(self.uri.path.segments[-1])
        return self.__id

    def bind(self, uri, metadata=None):
        """ Associate this relationship with a remote relationship. The start and
        end nodes will also be associated with their corresponding remote nodes.

        :arg uri: The URI identifying the remote relationship to which to bind.
        :arg metadata: Dictionary of initial metadata to attach to the contained resource.

        """
        Bindable.bind(self, uri, metadata)
        self.cache[uri] = self
        for i, key, node in [(0, "start", self.start_node), (-1, "end", self.end_node)]:
            uri = self.resource.metadata[key]
            if isinstance(node, Node):
                node.bind(uri)
            else:
                nodes = list(self._nodes)
                node = Node.cache.setdefault(uri, Node())
                if not node.bound:
                    node.bind(uri)
                nodes[i] = node
                self._nodes = tuple(nodes)

    @deprecated("Relationship.exists() is deprecated, use graph.exists(relationship) instead")
    def exists(self):
        """ :const:`True` if this relationship exists in the database,
        :const:`False` otherwise.
        """
        return self.graph.exists(self)

    @property
    def graph(self):
        """ The parent graph of this relationship.

        :rtype: :class:`.Graph`
        """
        return self.service_root.graph

    @property
    @deprecated("Relationship.properties is deprecated, use dict(relationship) instead")
    def properties(self):
        """ The set of properties attached to this relationship. Properties
        can also be read from and written to any :class:`Relationship`
        by using the index syntax directly. This means
        the following statements are equivalent::

            relationship.properties["since"] = 1999
            relationship["since"] = 1999

        """
        if self.bound and "properties" in self.__stale:
            self.graph.pull(self)
        return dict(self)

    @deprecated("Relationship.pull() is deprecated, use graph.pull(relationship) instead")
    def pull(self):
        """ Pull data to this relationship from its remote counterpart.
        """
        self.graph.pull(self)

    @deprecated("Relationship.push() is deprecated, use graph.push(relationship) instead")
    def push(self):
        """ Push data from this relationship to its remote counterpart.
        """
        self.graph.push(self)

    @property
    def ref(self):
        """ The URI of this relationship relative to its graph.

        :rtype: string
        """
        return "relationship/%s" % self._id

    def type(self):
        """ The type of this relationship.
        """
        if self.bound and self._type is None:
            self.graph.pull(self)
        return self._type

    def unbind(self):
        """ Detach this relationship and its start and end
        nodes from any remote counterparts.
        """
        try:
            del self.cache[self.uri]
        except KeyError:
            pass
        Bindable.unbind(self)
        for node in [self.start_node, self.end_node]:
            if isinstance(node, Node):
                try:
                    node.unbind()
                except BindError:
                    pass
        self.__id = None


class Subgraph(object):
    """ A general collection of :class:`.Node` and :class:`.Relationship` objects.
    """

    def __init__(self, *entities):
        self.__nodes = set()
        self.__relationships = set()
        for entity in entities:
            self.add(entity)

    def __repr__(self):
        return "<Subgraph order=%s size=%s>" % (self.order, self.size)

    def __eq__(self, other):
        try:
            return self.nodes == other.nodes and self.relationships == other.relationships
        except AttributeError:
            return False

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

    def __contains__(self, entity):
        if isinstance(entity, Node):
            return entity in self.__nodes
        elif isinstance(entity, Relationship):
            return (entity.start_node() in self.__nodes and
                    entity.end_node() in self.__nodes and
                    entity in self.__relationships)
        else:
            try:
                return (all(node in self for node in entity.nodes) and
                        all(relationship in self for relationship in entity.relationships))
            except AttributeError:
                return False

    def add(self, entity):
        """ Add an entity to the subgraph.

        :arg entity: Entity to add
        """
        entity = Graph.cast(entity)
        if isinstance(entity, Node):
            self.__nodes.add(entity)
        elif isinstance(entity, Relationship):
            self.__nodes.add(entity.start_node())
            self.__nodes.add(entity.end_node())
            self.__relationships.add(entity)
        else:
            for node in entity.nodes:
                self.__nodes.add(node)
            for relationship in entity.relationships:
                self.__relationships.add(relationship)

    @property
    def bound(self):
        """ :const:`True` if all entities in this subgraph are bound to remote counterparts,
        :const:`False` otherwise.
        """
        try:
            _ = self.service_root
        except BindError:
            return False
        else:
            return True

    @property
    @deprecated("Subgraph.exists() is deprecated, use graph.exists(subgraph) instead")
    def exists(self):
        """ :const:`True` if this subgraph exists in the database,
        :const:`False` otherwise.
        """
        return self.graph.exists(*(self.__nodes | self.__relationships))

    @property
    def graph(self):
        """ The parent graph of this subgraph.

        :rtype: :class:`.Graph`
        """
        return self.service_root.graph

    @property
    def nodes(self):
        """ The set of all nodes in this subgraph.
        """
        return frozenset(self.__nodes)

    @property
    def order(self):
        """ The number of nodes in this subgraph.
        """
        return len(self.__nodes)

    @property
    def relationships(self):
        """ The set of all relationships in this subgraph.
        """
        return frozenset(self.__relationships)

    @property
    def service_root(self):
        """ The root service associated with this subgraph.

        :return: :class:`.ServiceRoot`
        """
        for relationship in self:
            try:
                return relationship.service_root
            except BindError:
                pass
        raise BindError("Local path is not bound to a remote path")

    @property
    def size(self):
        """ The number of relationships in this subgraph.
        """
        return len(self.__relationships)

    def unbind(self):
        """ Detach all entities in this subgraph
        from any remote counterparts.
        """
        for entity in self.__nodes | self.__relationships:
            try:
                entity.unbind()
            except BindError:
                pass
