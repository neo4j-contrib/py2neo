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

""" The neo4j module provides the main `Neo4j <http://neo4j.org/>`_ client
functionality and will be the starting point for most applications. The main
classes provided are:

- :py:class:`Graph` - an instance of a Neo4j database server,
  providing a number of graph-global methods for handling nodes and
  relationships
- :py:class:`Node` - a representation of a database node
- :py:class:`Relationship` - a representation of a relationship between two
  database nodes
- :py:class:`Path` - a sequence of alternating nodes and relationships
- :py:class:`ReadBatch` - a batch of read requests to be carried out within a
  single transaction
- :py:class:`WriteBatch` - a batch of write requests to be carried out within
  a single transaction
"""


from __future__ import division, unicode_literals

from collections import namedtuple
from datetime import datetime
import base64
import json
import logging
import re
from weakref import WeakValueDictionary

from .packages.httpstream import (http,
                                  Resource as _Resource,
                                  ResourceTemplate as _ResourceTemplate,
                                  ClientError as _ClientError,
                                  ServerError as _ServerError)
from .packages.jsonstream import assembled, grouped
from .packages.httpstream.numbers import CREATED, NOT_FOUND, CONFLICT, BAD_REQUEST
from .packages.urimagic import percent_encode, URI, URITemplate

from . import __version__
from .exceptions import *

from .util import *


__all__ = ["Cacheable", "Graph", "GraphDatabaseService", "Node", "Path", "Rel",
           "Relationship", "Resource", "ResourceTemplate", "Rev", "_hydrated",
           "ReadBatch", "WriteBatch", "BatchRequestList", "_cast", "_rel",
           "Index", "LegacyReadBatch", "LegacyWriteBatch"]


DEFAULT_SCHEME = "http"
DEFAULT_HOST = "localhost"
DEFAULT_PORT = 7474
DEFAULT_NETLOC = "{0}:{1}".format(DEFAULT_HOST, DEFAULT_PORT)
DEFAULT_URI = "{0}://{1}".format(DEFAULT_SCHEME, DEFAULT_NETLOC)

PRODUCT = ("py2neo", __version__)

NON_ALPHA_NUM = re.compile("[^0-9A-Za-z_]")
SIMPLE_NAME = re.compile(r"[A-Za-z_][0-9A-Za-z_]*")

http.default_encoding = "UTF-8"

batch_log = logging.getLogger(__name__ + ".batch")
cypher_log = logging.getLogger(__name__ + ".cypher")

_headers = {
    None: [("X-Stream", "true")]
}

_http_rewrites = {}

auto_sync = True


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


def familiar(*resources):
    """ Return :py:const:`True` if all resources share a common service root.

    :param resources:
    :return:
    """
    if len(resources) < 2:
        return True
    return all(_.service_root == resources[0].service_root for _ in resources)


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


# TODO: remove and replace with Graph.hydrate
def _hydrated(data, hydration_cache=None):
    """ Takes input iterable, assembles and resolves any Resource objects,
    returning the result.
    """
    if hydration_cache is None:
        hydration_cache = {}
    if isinstance(data, dict):
        if has_all(data, Relationship.signature):
            self_uri = data["self"]
            try:
                return hydration_cache[self_uri]
            except KeyError:
                hydrated = Relationship._hydrated(data)
                hydration_cache[self_uri] = hydrated
                return hydrated
        elif has_all(data, Node.signature):
            self_uri = data["self"]
            try:
                return hydration_cache[self_uri]
            except KeyError:
                hydrated = Node._hydrated(data)
                hydration_cache[self_uri] = hydrated
                return hydrated
        elif has_all(data, Path.signature):
            return Path._hydrated(data)
        else:
            raise ValueError("Cannot determine object type", data)
    elif is_collection(data):
        return type(data)([_hydrated(datum, hydration_cache) for datum in data])
    else:
        return data


def _rel(*args, **kwargs):
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
                return Rel.cast(arg[1]).between(arg[0], arg[2])
            elif len(arg) == 4:
                return Relationship.abstract(arg[0], arg[1], arg[2], **arg[3])
            else:
                raise TypeError("Cannot cast relationship from {0}".format(arg))
        else:
            raise TypeError("Cannot cast relationship from {0}".format(arg))
    elif len(args) == 3:
        rel = Rel.cast(args[1])
        rel.properties.update(kwargs)
        return rel.between(args[0], args[2])
    elif len(args) == 4:
        props = args[3]
        props.update(kwargs)
        return Relationship.abstract(*args[0:3], **props)
    else:
        raise TypeError("Cannot cast relationship from {0}".format((args, kwargs)))


class UnboundError(Exception):
    pass


# TODO: rename to Resource
class Neo4jResource(_Resource):
    """ Variant of HTTPStream Resource that passes extra headers and product
    detail.
    """

    def __init__(self, uri):
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
        self.__base = super(Neo4jResource, self)
        self.__last_get_response = None

    @property
    def headers(self):
        return self.__headers

    @property
    def metadata(self):
        if self.__last_get_response is None:
            self.get()
        return self.__last_get_response.content

    def get(self, headers=None, redirect_limit=5, **kwargs):
        headers = dict(headers or {})
        headers.update(self.__headers)
        kwargs.update(product=PRODUCT, cache=True)
        # TODO: clean up exception handling - decorator? do we need both client/server types at this level?
        try:
            self.__last_get_response = self.__base.get(headers, redirect_limit, **kwargs)
        except _ClientError as err:
            raise ClientError(err)
        except _ServerError as err:
            raise ServerError(err)
        else:
            return self.__last_get_response

    def put(self, body=None, headers=None, **kwargs):
        headers = dict(headers or {})
        headers.update(self.__headers)
        kwargs.update(product=PRODUCT)
        try:
            response = self.__base.put(body, headers, **kwargs)
        except _ClientError as err:
            raise ClientError(err)
        except _ServerError as err:
            raise ServerError(err)
        else:
            return response

    def post(self, body=None, headers=None, **kwargs):
        headers = dict(headers or {})
        headers.update(self.__headers)
        kwargs.update(product=PRODUCT)
        try:
            response = self.__base.post(body, headers, **kwargs)
        except _ClientError as err:
            raise ClientError(err)
        except _ServerError as err:
            raise ServerError(err)
        else:
            return response

    def delete(self, headers=None, **kwargs):
        headers = dict(headers or {})
        headers.update(self.__headers)
        kwargs.update(product=PRODUCT)
        try:
            response = self.__base.delete(headers, **kwargs)
        except _ClientError as err:
            raise ClientError(err)
        except _ServerError as err:
            raise ServerError(err)
        else:
            return response


# TODO: rename to ResourceTemplate
class Neo4jResourceTemplate(_ResourceTemplate):

    def expand(self, **values):
        return Neo4jResource(self.uri_template.expand(**values))


class Bindable(object):
    """ Mixin for objects that can be bound to a remote resource.
    """

    def __init__(self):
        self.__service_root = None
        self.__graph = None
        self.__resource = None

    def __assert_bound(self):
        if not self.bound:
            raise UnboundError("Local object is not bound to a "
                               "remote resource")

    @property
    def service_root(self):
        self.__assert_bound()
        return self.__service_root

    @property
    def graph(self):
        self.__assert_bound()
        return self.__graph

    @property
    def resource(self):
        """ Returns the :class:`Resource` to which this is bound.
        """
        self.__assert_bound()
        return self.__resource

    @property
    def bound(self):
        """ Returns :const:`True` if bound to a remote resource.
        """
        return self.__resource is not None

    def bind(self, uri):
        """ Bind object to Resource or ResourceTemplate.
        """
        uri = ustr(uri)
        base = uri[:uri.find("/", uri.find("//") + 2)]
        self.__service_root = ServiceRoot.get_instance(base + "/")
        self.__graph = self.__service_root.graph
        if "{" in uri:
            self.__resource = Neo4jResourceTemplate(uri)
        else:
            self.__resource = Neo4jResource(uri)

    def unbind(self):
        self.__resource = None
        self.__graph = None
        self.__service_root = None

    # deprecated
    @property
    def is_abstract(self):
        return not self.bound


# TODO: remove this class
class Resource(object):
    """ Basic RESTful web resource with JSON metadata. Wraps an
    `httpstream.Resource`.
    """

    def __init__(self, uri):
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
        self._resource = _Resource(uri)
        self._metadata = None
        self._subresources = {}
        self._headers = _get_headers(self.__uri__.host_port)
        self._product = PRODUCT

    def __repr__(self):
        """ Return a valid Python representation of this object.
        """
        return repr(self._resource)

    def __eq__(self, other):
        """ Determine equality of two objects based on URI.
        """
        return self._resource == other._resource

    def __ne__(self, other):
        """ Determine inequality of two objects based on URI.
        """
        return self._resource != other._resource

    @property
    def __uri__(self):
        return self._resource.__uri__

    @property
    def __metadata__(self):
        if not self._metadata:
            self.refresh()
        return self._metadata

    @property
    def is_abstract(self):
        """ Indicates whether this entity is abstract (i.e. not bound
        to a concrete entity within the database)
        """
        return not bool(self.__uri__)

    @property
    def service_root(self):
        return ServiceRoot.get_instance(URI(self._resource).resolve("/"))

    @property
    def graph(self):
        return self.service_root.graph

    def refresh(self):
        """ Refresh resource metadata.
        """
        if not self.is_abstract:
            self._metadata = ResourceMetadata(self._get().content)

    def _get(self):
        try:
            return self._resource.get(headers=self._headers,
                                      product=self._product)
        except _ClientError as e:
            raise ClientError(e)
        except _ServerError as e:
            raise ServerError(e)

    def _put(self, body=None):
        try:
            return self._resource.put(body=body,
                                      headers=self._headers,
                                      product=self._product)
        except _ClientError as e:
            raise ClientError(e)
        except _ServerError as e:
            raise ServerError(e)

    def _post(self, body=None):
        try:
            return self._resource.post(body=body,
                                       headers=self._headers,
                                       product=self._product)
        except _ClientError as e:
            raise ClientError(e)
        except _ServerError as e:
            raise ServerError(e)

    def _delete(self):
        try:
            return self._resource.delete(headers=self._headers,
                                         product=self._product)
        except _ClientError as e:
            raise ClientError(e)
        except _ServerError as e:
            raise ServerError(e)

    def _subresource(self, key, cls=None):
        if key not in self._subresources:
            try:
                uri = URI(self.__metadata__[key])
            except KeyError:
                raise KeyError("Key {0} not found in resource "
                               "metadata".format(repr(key)), self.__metadata__)
            if not cls:
                cls = Resource
            self._subresources[key] = cls(uri)
        return self._subresources[key]


# TODO: remove this class
class ResourceMetadata(object):

    def __init__(self, metadata):
        self._metadata = dict(metadata)

    def __contains__(self, key):
        return key in self._metadata

    def __getitem__(self, key):
        return self._metadata[key]

    def __iter__(self):
        return iter(self._metadata.items())


# TODO: remove this class
class ResourceTemplate(_ResourceTemplate):

    def expand(self, **values):
        return Resource(_ResourceTemplate.expand(self, **values).uri)


class Cacheable(object):

    __instances = {}

    @classmethod
    def get_instance(cls, uri):
        """ Fetch a cached instance if one is available, otherwise create,
        cache and return a new instance.

        :param uri: URI of the cached resource
        :return: a resource instance
        """
        if uri not in cls.__instances:
            cls.__instances[uri] = cls(uri)
        return cls.__instances[uri]


class ServiceRoot(Cacheable, Resource):
    """ Neo4j REST API service root resource.
    """

    def __init__(self, uri=None):
        Resource.__init__(self, uri or DEFAULT_URI)
        self._load2neo = None
        self._load2neo_checked = False

    @property
    def graph(self):
        return Graph.get_instance(self.__metadata__["data"])

    @property
    def load2neo(self):
        if not self._load2neo_checked:
            self._load2neo = Resource(URI(self).resolve("/load2neo"))
            try:
                self._load2neo.refresh()
            except ClientError:
                self._load2neo = None
            finally:
                self._load2neo_checked = True
        if self._load2neo is None:
            raise NotImplementedError("Load2neo extension not available")
        else:
            return self._load2neo

    @property
    def monitor(self):
        manager = Resource(self.__metadata__["management"])
        return Monitor(manager.__metadata__["services"]["monitor"])


# TODO: move to admin plugin
class Monitor(Cacheable, Resource):

    def __init__(self, uri=None):
        if uri is None:
            uri = ServiceRoot().monitor.__uri__
        Resource.__init__(self, uri)

    def fetch_latest_stats(self):
        """ Fetch the latest server statistics as a list of 2-tuples, each
        holding a `datetime` object and a named tuple of node, relationship and
        property counts.
        """
        counts = namedtuple("Stats", ("node_count",
                                      "relationship_count",
                                      "property_count"))
        uri = self.__metadata__["resources"]["latest_data"]
        latest_data = Resource(uri)._get().content
        timestamps = latest_data["timestamps"]
        data = latest_data["data"]
        data = zip(
            (datetime.fromtimestamp(t) for t in timestamps),
            (counts(*x) for x in zip(
                (numberise(n) for n in data["node_count"]),
                (numberise(n) for n in data["relationship_count"]),
                (numberise(n) for n in data["property_count"]),
            )),
        )
        return data


class Graph(Cacheable, Resource):
    """ An instance of a `Neo4j <http://neo4j.org/>`_ database identified by
    its base URI. Generally speaking, this is the only URI which a system
    attaching to this service should need to be directly aware of; all further
    entity URIs will be discovered automatically from within response content
    when possible (see `Hypermedia <http://en.wikipedia.org/wiki/Hypermedia>`_)
    or will be derived from existing URIs.

    The following code illustrates how to connect to a database server and
    display its version number::

        from py2neo import neo4j
        
        graph = neo4j.Graph()
        print(graph.neo4j_version)

    :param uri: the base URI of the database (defaults to <http://localhost:7474/db/data/>)
    """

    def __init__(self, uri=None):
        if uri is None:
            uri = ServiceRoot().graph.__uri__
        Resource.__init__(self, uri)
        self.__node_cache = WeakValueDictionary()
        self.__rel_cache = WeakValueDictionary()

    def __len__(self):
        """ Return the size of this graph (i.e. the number of relationships).
        """
        return self.size

    @property
    def _load2neo(self):
        return self.service_root.load2neo

    def clear(self):
        """ Clear all nodes and relationships from the graph.

        .. warning::
            This method will permanently remove **all** nodes and relationships
            from the graph and cannot be undone.
        """
        batch = WriteBatch(self)
        batch.append_cypher("START r=rel(*) DELETE r")
        batch.append_cypher("START n=node(*) DELETE n")
        batch.run()

    def create(self, *abstracts):
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
            if entity is not None:
                batch.delete(entity)
        batch.run()

    def find(self, label, property_key=None, property_value=None):
        """ Iterate through a set of labelled nodes, optionally filtering
        by property key and value
        """
        uri = URI(self).resolve("/".join(["label", label, "nodes"]))
        if property_key:
            uri = uri.resolve("?" + percent_encode({property_key: json.dumps(property_value, ensure_ascii=False)}))
        try:
            for i, result in grouped(Resource(uri)._get()):
                yield _hydrated(assembled(result))
        except ClientError as err:
            if err.status_code != NOT_FOUND:
                raise

    def get_properties(self, *entities):
        """ Fetch properties for multiple nodes and/or relationships as part
        of a single batch; returns a list of dictionaries in the same order
        as the supplied entities.
        """
        if not entities:
            return []
        if len(entities) == 1:
            return [entities[0].get_properties()]
        batch = BatchRequestList(self)
        for entity in entities:
            batch.append_get(batch._uri_for(entity, "properties"))
        responses = batch._execute()
        try:
            return [BatchResponse(rs, raw=True).body or {}
                    for rs in responses.content]
        finally:
            responses.close()

    def load_geoff(self, geoff):
        """ Load Geoff data via the load2neo extension.

        ::

            >>> from py2neo import neo4j
            >>> graph = neo4j.Graph()
            >>> graph.load_geoff("(alice)<-[:KNOWS]->(bob)")
            [{u'alice': Node('http://localhost:7474/db/data/node/1'),
              u'bob': Node('http://localhost:7474/db/data/node/2')}]

        :param geoff: geoff data to load
        :return: list of node mappings
        """
        loader = Resource(self._load2neo.__metadata__["geoff_loader"])
        return [
            dict((key, self.node(value)) for key, value in line[0].items())
            for line in loader._post(geoff).tsj
        ]

    @property
    def load2neo_version(self):
        """ The load2neo extension version, if available.
        """
        return version_tuple(self._load2neo.__metadata__["load2neo_version"])

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
            rel_clause = ""
        elif is_collection(rel_type):
            if self.neo4j_version >= (2, 0, 0):
                # yuk, version sniffing :-(
                separator = "|:"
            else:
                separator = "|"
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
        results = CypherQuery(self, query).stream(**params)
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

    @property
    def neo4j_version(self):
        """ The database software version as a 4-tuple of (``int``, ``int``,
        ``int``, ``str``).
        """
        return version_tuple(self.__metadata__["neo4j_version"])

    def node(self, id_):
        """ Fetch a node by ID.
        """
        node = Node()
        node.bind(URI(self).resolve("node/" + str(id_)))

    @property
    def node_labels(self):
        """ The set of node labels currently defined within the graph.
        """
        resource = Resource(URI(self).resolve("labels"))
        try:
            return set(_hydrated(assembled(resource._get())))
        except ClientError as err:
            if err.status_code == NOT_FOUND:
                raise NotImplementedError("Node labels not available for this "
                                          "Neo4j server version")
            else:
                raise

    @property
    def order(self):
        """ The number of nodes in this graph.
        """
        return CypherQuery(self, "START n=node(*) "
                                 "RETURN count(n)").execute_one()

    def relationship(self, id_):
        """ Fetch a relationship by ID.
        """
        return Relationship(URI(self).resolve("relationship/" + str(id_)))

    @property
    def relationship_types(self):
        """ The set of relationship types currently defined within the graph.
        """
        resource = self._subresource("relationship_types")
        return set(_hydrated(assembled(resource._get())))

    @property
    def schema(self):
        """ The Schema resource for this graph.

        .. seealso::
            :py:func:`Schema <py2neo.neo4j.Schema>`
        """
        return Schema.get_instance(URI(self).resolve("schema"))

    @property
    def size(self):
        """ The number of relationships in this graph.
        """
        return CypherQuery(self, "START r=rel(*) "
                                 "RETURN count(r)").execute_one()

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
        return "transaction" in self.__metadata__

    def relative_uri(self, uri):
        # "http://localhost:7474/db/data/", "node/1"
        # TODO: confirm is URI
        if uri.startswith(self.__uri__.string):
            return uri[len(self.__uri__.string):]
        else:
            # TODO: specialist error
            raise ValueError(uri + " does not belong to this graph")

    def hydrate(self, data):
        if isinstance(data, dict):
            if "self" in data:
                # entity (node or rel)
                tag, i = self.relative_uri(data["self"]).partition("/")[0::2]
                if tag == "":
                    return self  # uri refers to graph
                elif tag == "node":
                    return self.__node_cache.setdefault(int(i), Node.hydrate(data))
                elif tag == "relationship":
                    return self.__rel_cache.setdefault(int(i), Relationship.hydrate(data))
                else:
                    raise ValueError("Cannot hydrate entity of type '{}'".format(tag))
            else:
                # path
                return Path.hydrate(data)
        elif is_collection(data):
            return type(data)(map(self.hydrate, data))
        else:
            return data


class CypherQuery(object):
    """ A reusable Cypher query. To create a new query object, a graph and the
    query text need to be supplied::

        >>> from py2neo import neo4j
        >>> graph = neo4j.Graph()
        >>> query = neo4j.CypherQuery(graph, "CREATE (a) RETURN a")

    """

    def __init__(self, graph, query):
        self._cypher = Resource(graph.__metadata__["cypher"])
        self._query = query

    def __str__(self):
        return self._query

    @property
    def string(self):
        """ The text of the query.
        """
        return self._query

    def _execute(self, **params):
        if __debug__:
            cypher_log.debug("Query: " + repr(self._query))
            if params:
                cypher_log.debug("Params: " + repr(params))
        try:
            return self._cypher._post({
                "query": self._query,
                "params": dict(params or {}),
            })
        except ClientError as e:
            if e.exception:
                # A CustomCypherError is a dynamically created subclass of
                # CypherError with the same name as the underlying server
                # exception
                CustomCypherError = type(str(e.exception), (CypherError,), {})
                raise CustomCypherError(e)
            else:
                raise CypherError(e)

    def run(self, **params):
        """ Execute the query and discard any results.

        :param params:
        """
        self._execute(**params).close()

    def execute(self, **params):
        """ Execute the query and return the results.

        :param params:
        :return:
        :rtype: :py:class:`CypherResults <py2neo.neo4j.CypherResults>`
        """
        return CypherResults(self._execute(**params))

    def execute_one(self, **params):
        """ Execute the query and return the first value from the first row.

        :param params:
        :return:
        """
        try:
            return self.execute(**params).data[0][0]
        except IndexError:
            return None

    def stream(self, **params):
        """ Execute the query and return a result iterator.

        :param params:
        :return:
        :rtype: :py:class:`IterableCypherResults <py2neo.neo4j.IterableCypherResults>`
        """
        return IterableCypherResults(self._execute(**params))


class CypherResults(object):
    """ A static set of results from a Cypher query.
    """

    signature = ("columns", "data")

    @classmethod
    def _hydrated(cls, data, hydration_cache=None):
        """ Takes assembled data...
        """
        producer = RecordProducer(data["columns"])
        return [
            producer.produce(_hydrated(row, hydration_cache))
            for row in data["data"]
        ]

    def __init__(self, response):
        content = response.content
        self._columns = tuple(content["columns"])
        self._producer = RecordProducer(self._columns)
        self._data = [
            self._producer.produce(_hydrated(row))
            for row in content["data"]
        ]

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

    def __len__(self):
        return len(self._data)

    def __getitem__(self, item):
        return self._data[item]

    @property
    def columns(self):
        """ Column names.
        """
        return self._columns

    @property
    def data(self):
        """ List of result records.
        """
        return self._data

    def __iter__(self):
        return iter(self._data)


class IterableCypherResults(object):
    """ An iterable set of results from a Cypher query.

    ::

        query = graph.cypher.query("START n=node(*) RETURN n LIMIT 10")
        for record in query.stream():
            print record[0]

    Each record returned is cast into a :py:class:`namedtuple` with names
    derived from the resulting column names.

    .. note ::
        Results are available as returned from the server and are decoded
        incrementally. This means that there is no need to wait for the
        entire response to be received before processing can occur.
    """

    def __init__(self, response):
        self._response = response
        self._redo_buffer = []
        self._buffered = self._buffered_results()
        self._columns = None
        self._fetch_columns()
        self._producer = RecordProducer(self._columns)

    def _fetch_columns(self):
        redo = []
        section = []
        for key, value in self._buffered:
            if key and key[0] == "columns":
                section.append((key, value))
            else:
                redo.append((key, value))
                if key and key[0] == "data":
                    break
        self._redo_buffer.extend(redo)
        self._columns = tuple(assembled(section)["columns"])

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def _buffered_results(self):
        for result in self._response:
            while self._redo_buffer:
                yield self._redo_buffer.pop(0)
            yield result

    def __iter__(self):
        hydration_cache = {}
        for key, section in grouped(self._buffered):
            if key[0] == "data":
                for i, row in grouped(section):
                    yield self._producer.produce(_hydrated(assembled(row),
                                                           hydration_cache))

    @property
    def columns(self):
        """ Column names.
        """
        return self._columns

    def close(self):
        """ Close results and free resources.
        """
        self._response.close()


class Schema(Cacheable, Resource):

    def __init__(self, *args, **kwargs):
        Resource.__init__(self, *args, **kwargs)
        if not self.service_root.graph.supports_schema_indexes:
            raise NotImplementedError("Schema index support requires "
                                      "version 2.0 or above")
        self._index_template = \
            URITemplate(str(URI(self)) + "/index/{label}")
        self._index_key_template = \
            URITemplate(str(URI(self)) + "/index/{label}/{property_key}")
        self._uniqueness_constraint_template = \
            URITemplate(str(URI(self)) + "/constraint/{label}/uniqueness")
        self._uniqueness_constraint_key_template = \
            URITemplate(str(URI(self)) + "/constraint/{label}/uniqueness/{property_key}")

    def get_indexed_property_keys(self, label):
        """ Fetch a list of indexed property keys for a label.

        :param label:
        :return:
        """
        if not label:
            raise ValueError("Label cannot be empty")
        resource = Resource(self._index_template.expand(label=label))
        try:
            response = resource._get()
        except ClientError as err:
            if err.status_code == NOT_FOUND:
                return []
            else:
                raise
        else:
            return [
                indexed["property_keys"][0]
                for indexed in response.content
            ]

    def get_unique_constraints(self, label):
        """ Fetch a list of uniqueness constraints for a label.

        :param label:
        :return:
        """
        if not label:
            raise ValueError("Label cannot be empty")
        resource = Resource(self._uniqueness_constraint_template.expand(label=label))
        try:
            response = resource._get()
        except ClientError as err:
            if err.status_code == NOT_FOUND:
                return []
            else:
                raise
        else:
            return [
                unique["property_keys"][0]
                for unique in response.content
            ]

    def create_index(self, label, property_key):
        """ Index a property key for a label.

        :param label:
        :param property_key:
        :return:
        """
        if not label or not property_key:
            raise ValueError("Neither label nor property key can be empty")
        resource = Resource(self._index_template.expand(label=label))
        property_key = bytearray(property_key, "utf-8").decode("utf-8")
        try:
            resource._post({"property_keys": [property_key]})
        except ClientError as err:
            if err.status_code == CONFLICT:
                raise ValueError(err.cause.message)
            else:
                raise

    def add_unique_constraint(self, label, property_key):
        """ Create an uniqueness constraint for a label.

         :param label:
         :param property_key:
         :return:
        """

        if not label or not property_key:
            raise ValueError("Neither label nor property key can be empty")
        resource = Resource(self._uniqueness_constraint_template.expand(label=label))
        try:
            resource._post({"property_keys": [ustr(property_key)]})
        except ClientError as err:
            if err.status_code == CONFLICT:
                raise ValueError(err.cause.message)
            else:
                raise

    def drop_index(self, label, property_key):
        """ Remove label index for a given property key.

        :param label:
        :param property_key:
        :return:
        """
        if not label or not property_key:
            raise ValueError("Neither label nor property key can be empty")
        uri = self._index_key_template.expand(label=label,
                                              property_key=property_key)
        resource = Resource(uri)
        try:
            resource._delete()
        except ClientError as err:
            if err.status_code == NOT_FOUND:
                raise LookupError("Property key not found")
            else:
                raise

    def remove_unique_constraint(self, label, property_key):
        """ Remove uniqueness constraint for a given property key.

         :param label:
         :param property_key:
         :return:
        """
        if not label or not property_key:
            raise ValueError("Neither label nor property key can be empty")
        uri = self._uniqueness_constraint_key_template.expand(label=label,
                                                              property_key=property_key)
        resource = Resource(uri)
        try:
            resource._delete()
        except ClientError as err:
            if err.status_code == NOT_FOUND:
                raise LookupError("Property key not found")
            else:
                raise


class PropertySet(Bindable, dict):
    """ A dict subclass that equates None with a non-existent key and can be
    bound to a remote *properties* resource.
    """

    def __init__(self, iterable=None, **kwargs):
        Bindable.__init__(self)
        dict.__init__(self)
        self.update(iterable, **kwargs)

    def __getitem__(self, key):
        return dict.get(self, key)

    def __setitem__(self, key, value):
        if value is None:
            try:
                dict.__delitem__(self, key)
            except KeyError:
                pass
        else:
            dict.__setitem__(self, key, value)

    def __eq__(self, other):
        if not isinstance(other, PropertySet):
            other = PropertySet(other)
        return dict.__eq__(self, other)

    def __ne__(self, other):
        return not self.__eq__(other)

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

    def pull(self):
        """ Copy the set of remote properties onto the local set.
        """
        self.resource.get()
        self.clear()
        properties = self.resource.metadata
        if properties:
            self.update(properties)

    def push(self):
        """ Copy the set of local properties onto the remote set.
        """
        self.resource.put(self)

    def __json__(self):
        return json.dumps(self, separators=",:", sort_keys=True)


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
        self.clear()
        labels = self.resource.metadata
        if labels:
            self.update(labels)

    def push(self):
        """ Copy the set of local labels onto the remote set.
        """
        self.resource.put(self)


class PropertyContainer(Bindable):
    """ Base class for objects that contain a set of properties,
    i.e. :py:class:`Node` and :py:class:`Relationship`.
    """

    def __init__(self, **properties):
        Bindable.__init__(self)
        self.__properties = PropertySet(properties)

    def __eq__(self, other):
        return self.properties == other.properties

    def __ne__(self, other):
        return not self.__eq__(other)

    def __contains__(self, key):
        # TODO 2.0: remove auto-pull
        if self.bound:
            self.properties.pull()
        return key in self.properties

    def __getitem__(self, key):
        # TODO 2.0: remove auto-pull
        if self.bound:
            self.properties.pull()
        return self.properties.__getitem__(key)

    def __setitem__(self, key, value):
        self.properties.__setitem__(key, value)
        # TODO 2.0: remove auto-push
        if self.bound:
            self.properties.push()

    def __delitem__(self, key):
        self.properties.__delitem__(key)
        # TODO 2.0: remove auto-push
        if self.bound:
            self.properties.push()

    @property
    def properties(self):
        """ The set of properties attached to this object.
        """
        return self.__properties

    def bind(self, uri):
        super(PropertyContainer, self).bind(uri)
        self.__properties.bind(self.resource.metadata["properties"])

    def unbind(self):
        super(PropertyContainer, self).unbind()
        self.__properties.unbind()

    def pull(self):
        self.resource.get()
        self.__properties.clear()
        properties = self.resource.metadata["data"]
        if properties:
            self.__properties.update(properties)

    def push(self):
        self.__properties.push()

    @deprecated("Use `properties` attribute instead")
    def get_cached_properties(self):
        """ Fetch last known properties without calling the server.

        :return: dictionary of properties
        """
        return self.properties

    @deprecated("Use `pull` method on `properties` attribute instead")
    def get_properties(self):
        """ Fetch all properties.

        :return: dictionary of properties
        """
        if self.bound:
            self.properties.pull()
        return self.properties

    @deprecated("Use `push` method on `properties` attribute instead")
    def set_properties(self, properties):
        """ Replace all properties with those supplied.

        :param properties: dictionary of new properties
        """
        self.properties.clear()
        self.properties.update(properties)
        if self.bound:
            self.properties.push()

    @deprecated("Use `push` method on `properties` attribute instead")
    def delete_properties(self):
        """ Delete all properties.
        """
        self.properties.clear()
        try:
            self.properties.push()
        except UnboundError:
            pass


# TODO: delete this class
class _Entity(Resource):
    """ Base class from which :py:class:`Node` and :py:class:`Relationship`
    classes inherit. Provides property management functionality by defining
    standard Python container handler methods.
    """

    def __init__(self, uri):
        Resource.__init__(self, uri)
        self._properties = {}

    def __contains__(self, key):
        return key in self.get_properties()

    def __delitem__(self, key):
        self.update_properties({key: None})

    def __getitem__(self, key):
        return self.get_properties().get(key, None)

    def __iter__(self):
        return self.get_properties().__iter__()

    def __len__(self):
        return len(self.get_properties())

    def __nonzero__(self):
        return True

    def __setitem__(self, key, value):
        self.update_properties({key: value})

    @property
    def _properties_resource(self):
        return self._subresource("properties")

    def get_cached_properties(self):
        """ Fetch last known properties without calling the server.

        :return: dictionary of properties
        """
        if self.is_abstract:
            return self._properties
        else:
            return self.__metadata__["data"]

    def get_properties(self):
        """ Fetch all properties.

        :return: dictionary of properties
        """
        if not self.is_abstract:
            self._properties = assembled(self._properties_resource._get()) or {}
        return self._properties

    def set_properties(self, properties):
        """ Replace all properties with those supplied.

        :param properties: dictionary of new properties
        """
        self._properties = dict(properties)
        if not self.is_abstract:
            if self._properties:
                self._properties_resource._put(compact(self._properties))
            else:
                self._properties_resource._delete()

    def delete_properties(self):
        """ Delete all properties.
        """
        self.set_properties({})

    def update_properties(self, properties):
        raise NotImplementedError("_Entity.update_properties")


class Node(PropertyContainer):
    """ A node within a graph, identified by a URI. For example:

        >>> from py2neo import Node
        >>> alice = Node()
        >>> alice.bind("http://localhost:7474/db/data/node/1")

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

    signature = ("self",)

    @staticmethod
    def cast(*args, **kwargs):
        """ Cast the arguments provided to a :py:class:`neo4j.Node`. The
        following general combinations are possible:

        - ``node()``
        - ``node(node_instance)``
        - ``node(property_dict)``
        - ``node(**properties)``

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
        if len(args) == 0:
            return Node(**kwargs)
        elif len(args) == 1 and not kwargs:
            arg = args[0]
            if arg is None:
                return None
            elif isinstance(arg, Node):
                return arg
            elif isinstance(arg, dict):
                return Node(**arg)
            else:
                raise TypeError("Cannot cast node from {0}".format(arg))
        else:
            raise TypeError("Cannot cast node from {0}".format((args, kwargs)))

    @classmethod
    def _hydrated(cls, data):
        obj = cls()
        obj.bind(data["self"])
        obj._metadata = ResourceMetadata(data)
        obj.properties.update(data.get("data", {}))
        return obj

    @classmethod
    def hydrate(cls, data):
        """ Create a new Node instance from a serialised representation held
        within a dictionary. It is expected there is at least a "self" key
        pointing to a URI for this Node; there may also optionally be
        properties passed in the "data" value.
        """
        try:
            properties = data["data"]
        except KeyError:
            inst = cls()
            inst.bind(data["self"])
            inst.__stale = {"labels", "properties"}
            return inst
        else:
            inst = cls(**properties)
            inst.bind(data["self"])
            inst.__stale = {"labels"}
            return inst

    @classmethod
    @deprecated("Use Node constructor instead")
    def abstract(cls, **properties):
        """ Create and return a new abstract node containing properties drawn
        from the keyword arguments supplied. An abstract node is not bound to
        a concrete node within a database but properties can be managed
        similarly to those within bound nodes::

            >>> alice = Node.abstract(name="Alice")
            >>> alice["name"]
            'Alice'
            >>> alice["age"] = 34
            alice.get_properties()
            {'age': 34, 'name': 'Alice'}

        If more complex property keys are required, abstract nodes may be
        instantiated with the ``**`` syntax::

            >>> alice = Node.abstract(**{"first name": "Alice"})
            >>> alice["first name"]
            'Alice'

        :param properties: node properties
        """
        instance = cls(**properties)
        return instance

    def __init__(self, *labels, **properties):
        PropertyContainer.__init__(self, **properties)
        self.__labels = LabelSet(labels)
        self.__stale = set()

    def __repr__(self):
        return self.__geoff__()

    def __eq__(self, other):
        # TODO: match on labels and properties only
        other = _cast(other, Node)
        if self.bound and other.bound:
            return self.resource == other.resource
        elif self.bound or other.bound:
            return False
        else:
            return self.properties == other.properties

    def __ne__(self, other):
        return not self.__eq__(other)

    def __geoff__(self):
        """ Return a Geoff representation of this Node.
        """
        s = []
        if self.bound:
            s.append(str(self._id))
        for label in sorted(self.labels):
            s.append(":")
            s.append(label)
        if self.properties:
            if s:
                s.append(" ")
            s.append(self.properties.__json__())
        s = ["("] + s + [")"]
        return "".join(s)

    def __hash__(self):
        if self.bound:
            return hash(self.resource.uri)
        else:
            # TODO: add labels to this hash
            return hash(tuple(sorted(self.properties.items())))

    @property
    def __uri__(self):
        return self.resource.uri

    @property
    def labels(self):
        """ The set of labels attached to this Node.
        """
        if "labels" in self.__stale:
            self.pull()
        return self.__labels

    @property
    def properties(self):
        """ The set of properties attached to this Node.
        """
        if "properties" in self.__stale:
            self.pull()
        return super(Node, self).properties

    def bind(self, uri):
        super(Node, self).bind(uri)
        self.__labels.bind(self.resource.metadata["labels"])

    def unbind(self):
        super(Node, self).unbind()
        self.__labels.unbind()

    def pull(self):
        query = CypherQuery(self.graph, "START a=node({a}) RETURN a,labels(a)")
        results = query.execute(a=self._id)
        node, labels = results[0].values
        super(Node, self).properties.clear()
        super(Node, self).properties.update(node.properties)
        self.__labels.clear()
        self.__labels.update(labels)
        self.__stale.clear()

    def push(self):
        # TODO combine this into a single call
        super(Node, self).push()
        self.labels.push()

    @property
    def _id(self):
        """ Return the internal ID for this entity.

        :return: integer ID of this entity within the database.
        """
        return int(self.resource.uri.path.segments[-1])

    def delete(self):
        """ Delete this entity from the database.
        """
        self.resource.delete()

    @property
    def exists(self):
        """ Detects whether this Node still exists in the database.
        """
        try:
            self.resource.get()
        except ClientError as err:
            if err.status_code == NOT_FOUND:
                return False
            else:
                raise
        else:
            return True

    def delete_related(self):
        """ Delete this node along with all related nodes and relationships.
        """
        if self.graph.supports_foreach_pipe:
            query = ("START a=node({a}) "
                     "MATCH (a)-[rels*0..]-(z) "
                     "FOREACH(r IN rels| DELETE r) "
                     "DELETE a, z")
        else:
            query = ("START a=node({a}) "
                     "MATCH (a)-[rels*0..]-(z) "
                     "FOREACH(r IN rels: DELETE r) "
                     "DELETE a, z")
        CypherQuery(self.graph, query).execute(a=self._id)

    def isolate(self):
        """ Delete all relationships connected to this node, both incoming and
        outgoing.
        """
        CypherQuery(self.graph, "START a=node({a}) "
                                "MATCH a-[r]-b "
                                "DELETE r").execute(a=self._id)

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
        - :py:const:`None`, representing an unspecified node that will be
          created as required

        :param items: alternating relationships and nodes
        :return: `Path` object representing the newly-created path
        """
        path = Path(self, *items)
        return path.create(self.graph)

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
        return path.get_or_create(self.graph)

    @deprecated("Use `labels` property instead")
    def get_labels(self):
        """ Fetch all labels associated with this node.

        :return: :py:class:`set` of text labels
        """
        self.labels.pull()
        return self.labels

    @deprecated("Use `add` or `update` method of `labels` property instead")
    def add_labels(self, *labels):
        """ Add one or more labels to this node.

        :param labels: one or more text labels
        """
        labels = [ustr(label) for label in set(flatten(labels))]
        self.labels.update(labels)
        try:
            self.labels.push()
        except ClientError as err:
            if err.status_code == BAD_REQUEST and err.cause.exception == 'ConstraintViolationException':
                raise ValueError(err.cause.message)
            else:
                raise

    @deprecated("Use `remove` method of `labels` property instead")
    def remove_labels(self, *labels):
        """ Remove one or more labels from this node.

        :param labels: one or more text labels
        """
        labels = [ustr(label) for label in set(flatten(labels))]
        batch = WriteBatch(self.graph)
        for label in labels:
            batch.remove_label(self, label)
        batch.run()

    @deprecated("Use `clear` and `update` methods of `labels` property instead")
    def set_labels(self, *labels):
        """ Replace all labels on this node.

        :param labels: one or more text labels
        """
        labels = [ustr(label) for label in set(flatten(labels))]
        self.labels.clear()
        self.add_labels(*labels)


class Rel(PropertyContainer):
    """ A relationship with no start or end nodes.
    """

    @classmethod
    def hydrate(cls, data):
        """ Create a new Rel instance from a serialised representation held
        within a dictionary. It is expected there is at least a "self" key
        pointing to a URI for this Rel; there may also optionally be a "type"
        and properties passed in the "data" value.
        """
        try:
            type_ = data["type"]
            properties = data["data"]
        except KeyError:
            inst = cls()
            inst.bind(data["self"])
            inst.__stale = {"type", "properties"}
            return inst
        else:
            inst = cls(type_, **properties)
            inst.bind(data["self"])
            return inst

    @classmethod
    def cast(cls, arg):
        if isinstance(arg, cls):
            return arg
        elif isinstance(arg, Relationship):
            return cls(arg.type, **arg.get_properties())
        elif isinstance(arg, tuple):
            if len(arg) == 1:
                return cls(str(arg[0]))
            elif len(arg) == 2:
                return cls(str(arg[0]), **arg[1])
            else:
                raise TypeError(arg)
        else:
            return cls(str(arg))

    def __init__(self, *type_, **properties):
        PropertyContainer.__init__(self, **properties)
        if len(type_) == 0:
            raise ValueError("A relationship type is required")
        elif len(type_) > 1:
            raise ValueError("Only one relationship type can be specified")
        self.__type = type_[0]
        self.__reverse = False
        self.__stale = set()

    def __repr__(self):
        return self.__geoff__()

    def __eq__(self, other):
        return (self.type == other.type and
                self.properties == other.properties)

    def __ne__(self, other):
        return not self.__eq__(other)

    def between(self, start_node, end_node):
        return Relationship.abstract(start_node, self.__type, end_node,
                                     **self.properties)

    @property
    def type(self):
        if "type" in self.__stale:
            self.pull()
        return self.__type

    @type.setter
    def type(self, name):
        if self.bound:
            raise TypeError("The type of a bound Rel is immutable")
        self.__type = name

    @property
    def properties(self):
        """ The set of properties attached to this Rel.
        """
        if "properties" in self.__stale:
            self.pull()
        return super(Rel, self).properties

    def pull(self):
        super(Rel, self).pull()
        self.__type = self.resource.metadata["type"]
        self.__stale.clear()

    @property
    def _id(self):
        """ Return the internal ID for this Rel.

        :return: integer ID of this entity within the database.
        """
        return int(self.resource.uri.path.segments[-1])

    def delete(self):
        """ Delete this Rel from the database.
        """
        self.resource.delete()

    @property
    def exists(self):
        """ Detects whether this Rel still exists in the database.
        """
        try:
            self.resource.get()
        except ClientError as err:
            if err.status_code == NOT_FOUND:
                return False
            else:
                raise
        else:
            return True

    def __geoff__(self):
        s = []
        if self.bound:
            s.append(str(self._id))
        s.append(":")
        s.append(self.__type)
        if self.properties:
            s.append(" ")
            s.append(self.properties.__json__())
        if self.__reverse:
            s = ["<-["] + s + ["]-"]
        else:
            s = ["-["] + s + ["]->"]
        return "".join(s)


class Rev(Rel):

    def __init__(self, *type_, **properties):
        Rel.__init__(self, *type_, **properties)
        self._Rel__reverse = True


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

    signature = ("length", "nodes", "relationships", "start", "end")

    @classmethod
    def _hydrated(cls, data):
        nodes = []
        for node_uri in data["nodes"]:
            node = Node()
            node.bind(node_uri)
            nodes.append(node)
        rels = map(Relationship, data["relationships"])
        return Path(*round_robin(nodes, rels))

    def __init__(self, node, *rels_and_nodes):
        self._nodes = [Node.cast(node)]
        self._nodes.extend(Node.cast(n) for n in rels_and_nodes[1::2])
        if len(rels_and_nodes) % 2 != 0:
            # If a trailing relationship is supplied, add a dummy end node
            self._nodes.append(Node())
        self._relationships = [
            Rel.cast(r)
            for r in rels_and_nodes[0::2]
        ]

    def __repr__(self):
        out = ", ".join(repr(item) for item in round_robin(self._nodes,
                                                           self._relationships))
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
        return (self._nodes == other._nodes and
                self._relationships == other._relationships)

    def __ne__(self, other):
        return (self._nodes != other._nodes or
                self._relationships != other._relationships)

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
            return Path(self._nodes[i], self._relationships[i],
                        self._nodes[i + 1])

    def __iter__(self):
        return iter(
            _rel((self._nodes[i], rel, self._nodes[i + 1]))
            for i, rel in enumerate(self._relationships)
        )

    @property
    def order(self):
        """ The number of nodes within this path.
        """
        return len(self._nodes)

    @property
    def size(self):
        """ The number of relationships within this path.
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
        left._relationships.append(Rel.cast(rel))
        left._nodes.extend(right._nodes)
        left._relationships.extend(right._relationships)
        return left

    def _create_query(self, unique):
        nodes, path, values, params = [], [], [], {}

        def append_node(i, node):
            if node is None:
                path.append("(n{0})".format(i))
                values.append("n{0}".format(i))
            elif node.is_abstract:
                path.append("(n{0} {{p{0}}})".format(i))
                params["p{0}".format(i)] = node.properties
                values.append("n{0}".format(i))
            else:
                path.append("(n{0})".format(i))
                nodes.append("n{0}=node({{i{0}}})".format(i))
                params["i{0}".format(i)] = node._id
                values.append("n{0}".format(i))

        def append_rel(i, rel):
            if rel.properties:
                path.append("-[r{0}:`{1}` {{q{0}}}]->".format(i, rel.type))
                params["q{0}".format(i)] = compact(rel.properties)
                values.append("r{0}".format(i))
            else:
                path.append("-[r{0}:`{1}`]->".format(i, rel.type))
                values.append("r{0}".format(i))

        append_node(0, self._nodes[0])
        for i, rel in enumerate(self._relationships):
            append_rel(i, rel)
            append_node(i + 1, self._nodes[i + 1])
        clauses = []
        if nodes:
            clauses.append("START {0}".format(",".join(nodes)))
        if unique:
            clauses.append("CREATE UNIQUE p={0}".format("".join(path)))
        else:
            clauses.append("CREATE p={0}".format("".join(path)))
        #clauses.append("RETURN {0}".format(",".join(values)))
        clauses.append("RETURN p")
        query = " ".join(clauses)
        return query, params

    def _create(self, graph, unique):
        query, params = self._create_query(unique=unique)
        try:
            results = CypherQuery(graph, query).execute(**params)
        except CypherError:
            raise NotImplementedError(
                "The Neo4j server at <{0}> does not support "
                "Cypher CREATE UNIQUE clauses or the query contains "
                "an unsupported property type".format(graph.__uri__)
            )
        else:
            for row in results:
                return row[0]

    def create(self, graph):
        """ Construct a path within the specified `graph` from the nodes
        and relationships within this :py:class:`Path` instance. This makes
        use of Cypher's ``CREATE`` clause.
        """
        return self._create(graph, unique=False)

    def get_or_create(self, graph):
        """ Construct a unique path within the specified `graph` from the
        nodes and relationships within this :py:class:`Path` instance. This
        makes use of Cypher's ``CREATE UNIQUE`` clause.
        """
        return self._create(graph, unique=True)


class Relationship(_Entity):
    """ A relationship within a graph, identified by a URI.

    :param uri: URI identifying this relationship
    """

    signature = ("self", "type")

    @classmethod
    def _hydrated(cls, data):
        obj = cls(data["self"])
        obj._metadata = ResourceMetadata(data)
        obj._properties = data.get("data", {})
        return obj

    @classmethod
    def abstract(cls, start_node, type_, end_node, **properties):
        """ Create and return a new abstract relationship.
        """
        instance = cls(None)
        instance._start_node = start_node
        instance._type = type_
        instance._end_node = end_node
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
            return (self._start_node == other._start_node and
                    self._type == other._type and
                    self._end_node == other._end_node and
                    self._properties == other._properties)

    def __ne__(self, other):
        other = _cast(other, Relationship)
        if self.__uri__:
            return _Entity.__ne__(self, other)
        else:
            return (self._start_node != other._start_node or
                    self._type != other._type or
                    self._end_node != other._end_node or
                    self._properties != other._properties)

    def __repr__(self):
        if not self.is_abstract:
            return "{0}({1})".format(
                self.__class__.__name__,
                repr(str(self.__uri__))
            )
        elif self._properties:
            return "rel({1}, {2}, {3}, {4})".format(
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
            type_str = json.dumps(type_str, ensure_ascii=False)
        if self._properties:
            return "{0}-[:{1} {2}]->{3}".format(
                str(self.start_node),
                type_str,
                json.dumps(self._properties, separators=(",", ":"), ensure_ascii=False),
                str(self.end_node),
            )
        else:
            return "{0}-[:{1}]->{2}".format(
                str(self.start_node),
                type_str,
                str(self.end_node),
            )

    def __hash__(self):
        if self.__uri__:
            return hash(self.__uri__)
        else:
            return hash(tuple(sorted(self._properties.items())))

    @property
    def _id(self):
        """ Return the internal ID for this entity.

        :return: integer ID of this entity within the database or
            :py:const:`None` if abstract
        """
        if self.is_abstract:
            return None
        else:
            return int(URI(self).path.segments[-1])

    def delete(self):
        """ Delete this entity from the database.
        """
        self._delete()

    @property
    def exists(self):
        """ Detects whether this entity still exists in the database.
        """
        try:
            self._get()
        except ClientError as err:
            if err.status_code == NOT_FOUND:
                return False
            else:
                raise
        else:
            return True

    @property
    def end_node(self):
        """ Return the end node of this relationship.
        """
        if self.__uri__ and not self._end_node:
            self._end_node = Node()
            self._end_node.bind(self.__metadata__['end'])
        return self._end_node

    @property
    def start_node(self):
        """ Return the start node of this relationship.
        """
        if self.__uri__ and not self._start_node:
            self._start_node = Node()
            self._start_node.bind(self.__metadata__['start'])
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
        if self.is_abstract:
            self._properties.update(properties)
            self._properties = compact(self._properties)
        else:
            query, params = ["START a=rel({A})"], {"A": self._id}
            for i, (key, value) in enumerate(properties.items()):
                value_tag = "V" + str(i)
                query.append("SET a.`" + key + "`={" + value_tag + "}")
                params[value_tag] = value
            query.append("RETURN a")
            rel = CypherQuery(self.graph, " ".join(query)).execute_one(**params)
            self._properties = rel.__metadata__["data"]


def _cast(obj, cls=(Node, Relationship), abstract=None):
    if obj is None:
        return None
    elif isinstance(obj, Node) or isinstance(obj, dict):
        entity = Node.cast(obj)
    elif isinstance(obj, Relationship) or isinstance(obj, tuple):
        entity = _rel(obj)
    else:
        raise TypeError(obj)
    if not isinstance(entity, cls):
        raise TypeError(obj)
    if abstract is not None and bool(abstract) != bool(entity.is_abstract):
        raise TypeError(obj)
    return entity


class BatchRequest(object):
    """ Individual batch request.
    """

    def __init__(self, method, uri, body=None):
        self._method = method
        self._uri = uri
        self._body = body

    def __eq__(self, other):
        return id(self) == id(other)

    def __ne__(self, other):
        return id(self) != id(other)

    def __hash__(self):
        return hash(id(self))

    @property
    def method(self):
        return self._method

    @property
    def uri(self):
        return self._uri

    @property
    def body(self):
        return self._body


class BatchResponse(object):
    """ Individual batch response.
    """

    @classmethod
    def __hydrate(cls, result, hydration_cache=None):
        body = result.get("body")
        if isinstance(body, dict):
            if has_all(body, CypherResults.signature):
                records = CypherResults._hydrated(body, hydration_cache)
                if len(records) == 0:
                    return None
                elif len(records) == 1:
                    if len(records[0]) == 1:
                        return records[0][0]
                    else:
                        return records[0]
                else:
                    return records
            elif has_all(body, ("exception", "stacktrace")):
                err = ServerException(body)
                try:
                    CustomBatchError = type(err.exception, (BatchError,), {})
                except TypeError:
                    # for Python 2.x
                    CustomBatchError = type(str(err.exception), (BatchError,), {})
                raise CustomBatchError(err)
            else:
                return _hydrated(body, hydration_cache)
        else:
            return _hydrated(body, hydration_cache)

    def __init__(self, result, raw=False, hydration_cache=None):
        self.id_ = result.get("id")
        self.uri = result.get("from")
        self.body = result.get("body")
        self.status_code = result.get("status", 200)
        self.location = URI(result.get("location"))
        if __debug__:
            batch_log.debug("<<< {{{0}}} {1} {2} {3}".format(self.id_, self.status_code, self.location, self.body))
        # We need to hydrate on construction to catch any errors in the batch
        # responses contained in the body
        if raw:
            self.__hydrated = None
        else:
            self.__hydrated = self.__hydrate(result, hydration_cache)

    @property
    def __uri__(self):
        return self.uri

    @property
    def hydrated(self):
        return self.__hydrated


class BatchRequestList(object):

    def __init__(self, graph):
        self._graph = graph
        self._batch = graph._subresource("batch")
        self._cypher = graph._subresource("cypher")
        self.clear()

    def __len__(self):
        return len(self._requests)

    def __nonzero__(self):
        return bool(self._requests)

    def append(self, request):
        self._requests.append(request)
        return request

    def append_get(self, uri):
        return self.append(BatchRequest("GET", uri))

    def append_put(self, uri, body=None):
        return self.append(BatchRequest("PUT", uri, body))

    def append_post(self, uri, body=None):
        return self.append(BatchRequest("POST", uri, body))

    def append_delete(self, uri):
        return self.append(BatchRequest("DELETE", uri))

    def append_cypher(self, query, params=None):
        """ Append a Cypher query to this batch. Resources returned from Cypher
        queries cannot be referenced by other batch requests.

        :param query: Cypher query
        :type query: :py:class:`str`
        :param params: query parameters
        :type params: :py:class:`dict`
        :return: batch request object
        :rtype: :py:class:`_Batch.Request`
        """
        if params:
            body = {"query": str(query), "params": dict(params)}
        else:
            body = {"query": str(query)}
        return self.append_post(self._uri_for(self._cypher), body)

    @property
    def _body(self):
        return [
            {
                "id": i,
                "method": request.method,
                "to": str(request.uri),
                "body": request.body,
            }
            for i, request in enumerate(self._requests)
        ]

    def clear(self):
        """ Clear all requests from this batch.
        """
        self._requests = []

    def find(self, request):
        """ Find the position of a request within this batch.
        """
        for i, req in pendulate(self._requests):
            if req == request:
                return i
        raise ValueError("Request not found")

    def _uri_for(self, resource, *segments, **kwargs):
        """ Return a relative URI in string format for the entity specified
        plus extra path segments.
        """
        if isinstance(resource, int):
            uri = "{{{0}}}".format(resource)
        elif isinstance(resource, BatchRequest):
            uri = "{{{0}}}".format(self.find(resource))
        elif isinstance(resource, Node):
            # TODO: remove when Rel is also Bindable
            offset = len(resource.graph.__uri__.string)
            uri = resource.resource.uri.string[offset:]
        else:
            offset = len(resource.service_root.graph.__uri__)
            uri = str(resource.__uri__)[offset:]
        if segments:
            if not uri.endswith("/"):
                uri += "/"
            uri += "/".join(map(percent_encode, segments))
        query = kwargs.get("query")
        if query is not None:
            uri += "?" + query
        return uri

    def _execute(self):
        request_count = len(self)
        request_text = "request" if request_count == 1 else "requests"
        batch_log.info("Executing batch with {0} {1}".format(request_count, request_text))
        if __debug__:
            for id_, request in enumerate(self._requests):
                batch_log.debug(">>> {{{0}}} {1} {2} {3}".format(id_, request.method, request.uri, request.body))
        try:
            response = self._batch._post(self._body)
        except (ClientError, ServerError) as e:
            if e.exception:
                # A CustomBatchError is a dynamically created subclass of
                # BatchError with the same name as the underlying server
                # exception
                CustomBatchError = type(str(e.exception), (BatchError,), {})
                raise CustomBatchError(e)
            else:
                raise BatchError(e)
        else:
            return response

    def run(self):
        """ Execute the batch on the server and discard the results. If the
        batch results are not required, this will generally be the fastest
        execution method.
        """
        return self._execute().close()

    def stream(self):
        """ Execute the batch on the server and return iterable results. This
        method allows handling of results as they are received from the server.

        :return: iterable results
        :rtype: :py:class:`BatchResponseList`
        """
        return BatchResponseList(self._execute())

    def submit(self):
        """ Execute the batch on the server and return a list of results. This
        method blocks until all results are received.

        :return: result records
        :rtype: :py:class:`list`
        """
        responses = self._execute()
        hydration_cache = {}
        try:
            return [BatchResponse(rs, hydration_cache=hydration_cache).hydrated
                    for rs in responses.content]
        finally:
            responses.close()

    def _index(self, content_type, index):
        """ Fetch an Index object.
        """
        if isinstance(index, Index):
            if content_type == index._content_type:
                return index
            else:
                raise TypeError("Index is not for {0}s".format(content_type))
        else:
            return self._graph.get_or_create_index(content_type, str(index))


class BatchResponseList(object):

    def __init__(self, response):
        self._response = response

    def __iter__(self):
        hydration_cache = {}
        for i, result in grouped(self._response):
            yield BatchResponse(assembled(result),
                                hydration_cache=hydration_cache).hydrated
        self.close()

    @property
    def closed(self):
        return self._response.closed

    def close(self):
        self._response.close()


class ReadBatch(BatchRequestList):
    """ Generic batch execution facility for data read requests,
    """

    def __init__(self, graph):
        BatchRequestList.__init__(self, graph)


class WriteBatch(BatchRequestList):
    """ Generic batch execution facility for data write requests. Most methods
    return a :py:class:`BatchRequest <py2neo.neo4j.BatchRequest>` object that
    can be used as a reference in other methods. See the
    :py:meth:`create <py2neo.neo4j.WriteBatch.create>` method for an example
    of this.
    """

    def __init__(self, graph):
        BatchRequestList.__init__(self, graph)

    def create(self, abstract):
        """ Create a node or relationship based on the abstract entity
        provided. For example::

            batch = WriteBatch(graph)
            a = batch.create(node(name="Alice"))
            b = batch.create(node(name="Bob"))
            batch.create(rel(a, "KNOWS", b))
            results = batch.submit()

        :param abstract: node or relationship
        :type abstract: abstract
        :return: batch request object
        """
        entity = _cast(abstract, abstract=True)
        if isinstance(entity, Node):
            uri = self._uri_for(self._graph._subresource("node"))
            body = entity.properties
        elif isinstance(entity, Relationship):
            uri = self._uri_for(entity.start_node, "relationships")
            body = {
                "type": entity._type,
                "to": self._uri_for(entity.end_node)
            }
            if entity._properties:
                body["data"] = compact(entity._properties)
        else:
            raise TypeError(entity)
        return self.append_post(uri, body)

    def create_path(self, node, *rels_and_nodes):
        """ Construct a path across a specified set of nodes and relationships.
        Nodes may be existing concrete node instances, abstract nodes or
        :py:const:`None` but references to other requests are not supported.

        :param node: start node
        :type node: concrete, abstract or :py:const:`None`
        :param rels_and_nodes: alternating relationships and nodes
        :type rels_and_nodes: concrete, abstract or :py:const:`None`
        :return: batch request object
        """
        query, params = Path(node, *rels_and_nodes)._create_query(unique=False)
        self.append_cypher(query, params)

    def get_or_create_path(self, node, *rels_and_nodes):
        """ Construct a unique path across a specified set of nodes and
        relationships, adding only parts that are missing. Nodes may be
        existing concrete node instances, abstract nodes or :py:const:`None`
        but references to other requests are not supported.

        :param node: start node
        :type node: concrete, abstract or :py:const:`None`
        :param rels_and_nodes: alternating relationships and nodes
        :type rels_and_nodes: concrete, abstract or :py:const:`None`
        :return: batch request object
        """
        query, params = Path(node, *rels_and_nodes)._create_query(unique=True)
        self.append_cypher(query, params)

    @deprecated("WriteBatch.get_or_create is deprecated, please use "
                "get_or_create_path instead")
    def get_or_create(self, rel_abstract):
        """ Use the abstract supplied to create a new relationship if one does
        not already exist.

        :param rel_abstract: relationship abstract to be fetched or created
        """
        rel = _cast(rel_abstract, cls=Relationship, abstract=True)
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
                "START b=node({B}) "
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
        return self.append_cypher(query, params)

    def delete(self, entity):
        """ Delete a node or relationship from the graph.

        :param entity: node or relationship to delete
        :type entity: concrete or reference
        :return: batch request object
        """
        return self.append_delete(self._uri_for(entity))

    def set_property(self, entity, key, value):
        """ Set a single property on a node or relationship.

        :param entity: node or relationship on which to set property
        :type entity: concrete or reference
        :param key: property key
        :type key: :py:class:`str`
        :param value: property value
        :return: batch request object
        """
        if value is None:
            self.delete_property(entity, key)
        else:
            uri = self._uri_for(entity, "properties", key)
            return self.append_put(uri, value)

    def set_properties(self, entity, properties):
        """ Replace all properties on a node or relationship.

        :param entity: node or relationship on which to set properties
        :type entity: concrete or reference
        :param properties: properties
        :type properties: :py:class:`dict`
        :return: batch request object
        """
        uri = self._uri_for(entity, "properties")
        return self.append_put(uri, compact(properties))

    def delete_property(self, entity, key):
        """ Delete a single property from a node or relationship.

        :param entity: node or relationship from which to delete property
        :type entity: concrete or reference
        :param key: property key
        :type key: :py:class:`str`
        :return: batch request object
        """
        uri = self._uri_for(entity, "properties", key)
        return self.append_delete(uri)

    def delete_properties(self, entity):
        """ Delete all properties from a node or relationship.

        :param entity: node or relationship from which to delete properties
        :type entity: concrete or reference
        :return: batch request object
        """
        uri = self._uri_for(entity, "properties")
        return self.append_delete(uri)

    def add_labels(self, node, *labels):
        """ Add labels to a node.

        :param node: node to which to add labels
        :type entity: concrete or reference
        :param labels: text labels
        :type labels: :py:class:`str`
        :return: batch request object
        """
        uri = self._uri_for(node, "labels")
        return self.append_post(uri, list(labels))

    def remove_label(self, node, label):
        """ Remove a label from a node.

        :param node: node from which to remove labels (can be a reference to
            another request within the same batch)
        :param label: text label
        :type label: :py:class:`str`
        :return: batch request object
        """
        uri = self._uri_for(node, "labels", label)
        return self.append_delete(uri)

    def set_labels(self, node, *labels):
        """ Replace all labels on a node.

        :param node: node on which to replace labels (can be a reference to
            another request within the same batch)
        :param labels: text labels
        :type labels: :py:class:`str`
        :return: batch request object
        """
        uri = self._uri_for(node, "labels")
        return self.append_put(uri, list(labels))

    # TODO: PullBatch
    # TODO: PushBatch


from py2neo.legacy import GraphDatabaseService, Index, \
    ReadBatch as LegacyReadBatch, WriteBatch as LegacyWriteBatch
