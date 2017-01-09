#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2017, Nigel Small
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

from __future__ import absolute_import

from collections import OrderedDict
from json import dumps as json_dumps, loads as json_loads

from neo4j.v1 import Driver, Session, StatementResult, Record, ResultSummary
from urllib3 import HTTPConnectionPool, HTTPSConnectionPool

from py2neo.addressing import keyring
from py2neo.compat import urlsplit
from py2neo.meta import HTTP_USER_AGENT
from py2neo.status import GraphError, Unauthorized




# import logging
#
# # Enabling debugging at http.client level (requests->urllib3->http.client)
# # you will see the REQUEST, including HEADERS and DATA, and RESPONSE with HEADERS but without DATA.
# # the only thing missing will be the response.body which is not logged.
# try: # for Python 3
#     from http.client import HTTPConnection
# except ImportError:
#     from httplib import HTTPConnection
# HTTPConnection.debuglevel = 1
#
# logging.basicConfig() # you need to initialize logging, otherwise you will not see anything from requests
# logging.getLogger().setLevel(logging.DEBUG)
# requests_log = logging.getLogger("urllib3")
# requests_log.setLevel(logging.DEBUG)
# requests_log.propagate = True



DEFAULT_PORT = 7474

OK = 200
CREATED = 201
NO_CONTENT = 204
UNAUTHORIZED = 401
NOT_FOUND = 404


ConnectionPool = {
    "http": HTTPConnectionPool,
    "https": HTTPSConnectionPool,
}

_http_headers = {
    (None, None, None): [
        ("User-Agent", HTTP_USER_AGENT),
        ("X-Stream", "true"),
    ],
}


def set_http_header(key, value, scheme=None, host=None, port=None):
    """ Add an HTTP header for all future requests. If a `host_port` is
    specified, this header will only be included in requests to that
    destination.

    :arg key: name of the HTTP header
    :arg value: value of the HTTP header
    :arg scheme:
    :arg host:
    :arg port:
    """
    address_key = (scheme, host, port)
    if address_key in _http_headers:
        _http_headers[address_key].append((key, value))
    else:
        _http_headers[address_key] = [(key, value)]


def get_http_headers(scheme, host, port):
    """Fetch all HTTP headers relevant to the `host_port` provided.

    :arg scheme:
    :arg host:
    :arg port:
    """
    uri_headers = {}
    for (s, h, p), headers in _http_headers.items():
        if (s is None or s == scheme) and (h is None or h == host) and (p is None or p == port):
            uri_headers.update(headers)
    for uri, auth in keyring.items():
        if auth and uri.scheme in ("http", "https") and uri.host == host and uri.port == port:
            uri_headers["Authorization"] = auth.http_authorization
    return uri_headers


def register_http_driver():
    """ TODO

    Notes: HTTP support; Graphy objects returned are py2neo objects
    """
    from neo4j.v1 import GraphDatabase
    if "http" not in GraphDatabase.uri_schemes:
        GraphDatabase.uri_schemes["http"] = HTTPDriver
        # TODO: HTTPS


def remote(obj):
    """ Return the remote counterpart of a local object.

    :param obj: the local object
    :return: the corresponding remote entity
    """
    try:
        return obj.__remote__
    except AttributeError:
        return None


def raise_error(uri, status_code, data):
    if status_code == UNAUTHORIZED:
        raise Unauthorized(uri)
    if data:
        content = json_loads(data.decode('utf-8'))
    else:
        content = {}
    message = content.pop("message", "HTTP request returned unexpected status code %s" % status_code)
    error = GraphError(message, **content)
    error.http_status_code = status_code
    raise error


class HTTPDriver(Driver):

    _graph_service = None

    def __init__(self, uri, **config):
        super(HTTPDriver, self).__init__(None)
        self._uri = uri
        self._auth = config.get("auth")

    @property
    def graph_service(self):
        if self._graph_service is None:
            from py2neo.graph import GraphService
            self._graph_service = GraphService(self._uri, auth=self._auth)
        return self._graph_service

    @property
    def graph(self):
        return self.graph_service.graph

    def session(self, access_mode=None):
        return HTTPSession(self.graph)


class HTTPResultLoader(object):

    def load(self, result):
        pass

    def fail(self):
        pass


class HTTPSession(Session):

    #: e.g. http://localhost:7474/db/data/transaction
    begin_path = None

    #: e.g. http://localhost:7474/db/data/transaction/commit
    autocommit_path = None

    #: e.g. http://localhost:7474/db/data/transaction/1
    transaction_path = None

    #: e.g. http://localhost:7474/db/data/transaction/1/commit
    commit_path = None

    def __init__(self, graph):
        self.graph = graph
        self.resource = WebResource(graph.transaction_uri)
        self.begin_path = "/db/data/transaction"
        self.autocommit_path = "%s/commit" % self.begin_path
        self.resource.path = self.autocommit_path
        self._statements = []
        self._result_loaders = []

    def close(self):
        super(HTTPSession, self).close()
        self.resource.close()

    def run(self, statement, parameters=None, **kwparameters):
        self._statements.append(OrderedDict([
            ("statement", statement),
            ("parameters", dict(parameters or {}, **kwparameters)),
            ("resultDataContents", ["REST"]),
            ("includeStats", True),
        ]))
        result_loader = HTTPResultLoader()
        self._result_loaders.append(result_loader)
        return HTTPStatementResult(self, result_loader)

    def fetch(self):
        return self.sync()

    def sync(self):
        path = self.resource.path
        # Some of the transactional URIs do not support empty statement
        # lists in versions earlier than 2.3. Which doesn't really matter
        # as it's a waste sending anything anyway.
        if path in (self.autocommit_path, self.begin_path, self.transaction_path) and not self._statements:
            return 0
        count = 0
        try:
            response = self.resource.post({"statements": self._statements}, expected=(OK, CREATED))
            if response.status == 201:
                self.transaction_path = urlsplit(response.headers["Location"]).path
                self.commit_path = "%s/commit" % self.transaction_path
                self.resource.path = self.transaction_path
            content = json_loads(response.data.decode("utf-8"))
            errors = content["errors"]
            if errors:
                from py2neo.graph import GraphError
                raise GraphError.hydrate(errors[0])
            for i, result_loader in enumerate(self._result_loaders):
                try:
                    count += result_loader.load(content["results"][i])
                except IndexError:
                    result_loader.fail()
            return count
        finally:
            self._statements[:] = ()
            self._result_loaders[:] = ()

    def begin_transaction(self, bookmark=None):
        transaction = super(HTTPSession, self).begin_transaction(bookmark)
        self.resource.path = self.begin_path
        return transaction

    def commit_transaction(self):
        super(HTTPSession, self).commit_transaction()
        self.resource.path = self.commit_path or self.autocommit_path
        try:
            self.sync()
        finally:
            self.commit_path = self.transaction_path = None
            self.resource.path = self.autocommit_path

    def rollback_transaction(self):
        super(HTTPSession, self).rollback_transaction()
        try:
            if self.transaction_path:
                self.resource.path = self.transaction_path
                self.resource.delete(expected=(OK, NOT_FOUND))
        finally:
            self.commit_path = self.transaction_path = None
            self.resource.path = self.autocommit_path


class HTTPStatementResult(StatementResult):

    value_system = None

    zipper = Record

    def __init__(self, session, result_loader):
        from py2neo.json import JSONValueSystem

        super(HTTPStatementResult, self).__init__(session)
        self.value_system = JSONValueSystem(session.graph, ())

        def load(result):
            self._keys = self.value_system.keys = tuple(result["columns"])
            self._records.extend(record["rest"] for record in result["data"])
            stats = result["stats"]
            # fix broken key
            if "relationship_deleted" in stats:
                stats["relationships_deleted"] = stats["relationship_deleted"]
                del stats["relationship_deleted"]
            if "contains_updates" in stats:
                del stats["contains_updates"]
            self._summary = ResultSummary(None, None, stats=stats)  # TODO: statement and params
            self._session = None
            return len(self._records)

        def fail():
            self._session = None

        result_loader.load = load
        result_loader.fail = fail


class WebResource(object):
    """ Base class for all local resources mapped to remote counterparts.
    """

    def __init__(self, uri, cached_json=None):
        self.uri = uri
        self.cached_json = cached_json
        parts = urlsplit(uri)
        scheme = parts.scheme
        host = parts.hostname
        port = parts.port
        self.path = parts.path
        self.http = http = ConnectionPool[scheme]("%s:%d" % (host, port))
        self.request = http.request
        self.headers = get_http_headers(scheme, host, port)

    def __del__(self):
        self.close()

    def __eq__(self, other):
        try:
            return self.uri == other.uri
        except AttributeError:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def get_json(self, force=True):
        """ Perform an HTTP GET to this resource and return JSON.
        """
        if not force and self.cached_json is not None:
            return self.cached_json
        rs = self.request("GET", self.path, headers=self.headers)
        try:
            if rs.status == 200:
                self.cached_json = json_loads(rs.data.decode('utf-8'))
                return self.cached_json
            else:
                raise_error(self.uri, rs.status, rs.data)
        finally:
            rs.close()

    def post(self, body, expected):
        """ Perform an HTTP POST to this resource.
        """
        headers = dict(self.headers)
        if body is not None:
            headers["Content-Type"] = "application/json"
            body = json_dumps(body).encode('utf-8')
        rs = self.request("POST", self.path, headers=self.headers, body=body)
        if rs.status not in expected:
            raise_error(self.uri, rs.status, rs.data)
        return rs

    def delete(self, expected):
        """ Perform an HTTP DELETE to this resource.
        """
        rs = self.request("DELETE", self.path, headers=self.headers)
        if rs.status not in expected:
            raise_error(self.uri, rs.status, rs.data)
        return rs

    def close(self):
        if self.http and self.http.pool:
            self.http.close()


class Remote(WebResource):

    _graph_service = None
    _ref = None
    _entity_type = None
    _entity_id = None

    def __init__(self, uri, metadata=None):
        WebResource.__init__(self, uri, metadata)

    def __repr__(self):
        return "<%s uri=%r>" % (self.__class__.__name__, self.uri)

    @property
    def graph_service(self):
        """ The root service associated with this resource.
        """
        if self._graph_service is None:
            uri = self.uri
            graph_service_uri = uri[:uri.find("/", uri.find("//") + 2)] + "/"
            if graph_service_uri == uri:
                self._graph_service = self
            else:
                from py2neo.graph import GraphService
                self._graph_service = GraphService(graph_service_uri)
        return self._graph_service

    @property
    def graph(self):
        """ The parent graph of this resource.
        """
        return self.graph_service.graph

    @property
    def ref(self):
        from py2neo.types import Node, Relationship
        if self._ref is None:
            self._ref = ref = self.uri[len(remote(self.graph).uri):]
            ref_parts = ref.partition("/")
            if ref_parts[0] == "node":
                try:
                    self._entity_id = int(ref_parts[-1])
                except ValueError:
                    pass
                else:
                    self._entity_type = Node
            elif ref_parts[0] == "relationship":
                try:
                    self._entity_id = int(ref_parts[-1])
                except ValueError:
                    pass
                else:
                    self._entity_type = Relationship
        return self._ref

    @property
    def entity_type(self):
        _ = self.ref
        return self._entity_type

    @property
    def entity_id(self):
        _ = self.ref
        return self._entity_id

    @property
    def _id(self):
        # TODO: deprecate
        return self.entity_id
