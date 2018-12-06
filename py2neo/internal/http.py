#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2018, Nigel Small
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

from base64 import b64encode
from collections import OrderedDict
from json import dumps as json_dumps, loads as json_loads
from warnings import catch_warnings, simplefilter

from neo4j import Driver, Session, StatementResult, TransactionError, SessionError
from neo4j.exceptions import AuthError, Forbidden
from neobolt.addressing import SocketAddress
from neobolt.direct import ServerInfo

from py2neo.data import Record
from py2neo.database import GraphError
from py2neo.internal.addressing import get_connection_data
from py2neo.internal.compat import urlsplit, ustr
from py2neo.internal.json import JSONDehydrator


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
FORBIDDEN = 403
NOT_FOUND = 404


def fix_parameters(parameters):
    if not parameters:
        return {}
    dehydrator = JSONDehydrator()
    try:
        dehydrated, = dehydrator.dehydrate([parameters])
    except TypeError as error:
        value = error.args[0]
        raise TypeError("Parameters of type {} are not supported".format(type(value).__name__))
    else:
        return dehydrated


class HTTP(object):
    """ Wrapper for HTTP method calls.
    """

    @staticmethod
    def authorization(user, password):
        return 'Basic ' + b64encode((user + ":" + password).encode("utf-8")).decode("ascii")

    def __init__(self, uri, headers, verified):
        self.uri = uri
        self.verified = verified
        parts = urlsplit(uri)
        scheme = parts.scheme
        host = parts.hostname
        port = parts.port
        if scheme == "http":
            from urllib3 import HTTPConnectionPool
            self._http = HTTPConnectionPool("%s:%d" % (host, port))
        elif scheme == "https":
            from urllib3 import HTTPSConnectionPool
            if verified:
                from certifi import where
                self._http = HTTPSConnectionPool("%s:%d" % (host, port), cert_reqs="CERT_REQUIRED", ca_certs=where())
            else:
                self._http = HTTPSConnectionPool("%s:%d" % (host, port))
        else:
            raise ValueError("Unsupported scheme %r" % scheme)
        self.path = parts.path
        if "auth" in headers:
            user, password = headers.pop("auth")
            headers["Authorization"] = 'Basic ' + b64encode(
                (ustr(user) + u":" + ustr(password)).encode("utf-8")).decode("ascii")
        self.headers = headers

    def __del__(self):
        self.close()

    def __eq__(self, other):
        try:
            return self.uri == other.uri
        except AttributeError:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def request(self, method, url, fields=None, headers=None, **urlopen_kw):
        from neo4j import ServiceUnavailable
        from urllib3.exceptions import MaxRetryError
        try:
            if self.verified:
                return self._http.request(method, url, fields, headers, **urlopen_kw)
            else:
                with catch_warnings():
                    simplefilter("ignore")
                    return self._http.request(method, url, fields, headers, **urlopen_kw)
        except MaxRetryError:
            raise ServiceUnavailable("Cannot send %s request to <%s>" % (method, url))

    def get_json(self, ref):
        """ Perform an HTTP GET to this resource and return JSON.
        """
        rs = self.request("GET", self.path + ref, headers=self.headers)
        try:
            if rs.status == 200:
                return json_loads(rs.data.decode('utf-8'))
            else:
                self.raise_error(rs.status, rs.data)
        finally:
            rs.close()

    def post(self, ref, json, expected):
        """ Perform an HTTP POST to this resource.
        """
        headers = dict(self.headers)
        if json is not None:
            headers["Content-Type"] = "application/json"
            json = json_dumps(json).encode('utf-8')
        rs = self.request("POST", self.path + ref, headers=self.headers, body=json)
        if rs.status not in expected:
            self.raise_error(rs.status, rs.data)
        return rs

    def delete(self, ref, expected):
        """ Perform an HTTP DELETE to this resource.
        """
        rs = self.request("DELETE", self.path + ref, headers=self.headers)
        if rs.status not in expected:
            self.raise_error(rs.status, rs.data)
        return rs

    def close(self):
        if self._http and self._http.pool:
            self._http.close()

    def raise_error(self, status_code, data):
        if status_code == UNAUTHORIZED:
            raise AuthError(self.uri)
        if status_code == FORBIDDEN:
            raise Forbidden(self.uri)
        if data:
            content = json_loads(data.decode('utf-8'))
        else:
            content = {}
        message = content.pop("message", "HTTP request to <%s> returned unexpected status code %s" % (self.uri, status_code))
        error = GraphError(message, **content)
        error.http_status_code = status_code
        raise error


class HTTPDriver(Driver):

    uri_scheme = "http"

    _connection_data = None

    _http = None

    _graph = None

    def __new__(cls, uri, **config):
        cls._check_uri(uri)
        instance = object.__new__(cls)
        instance._connection_data = connection_data = get_connection_data(uri, **config)
        instance._http = HTTP(connection_data["uri"] + "/db/data/", {
            "Authorization": HTTP.authorization(connection_data["user"], connection_data["password"]),
            "User-Agent": connection_data["user_agent"],
            "X-Stream": "true",
        }, connection_data["verified"])
        instance._graph = None
        return instance

    def session(self, access_mode=None, bookmark=None):
        if self._graph is None:
            from py2neo.database import Database
            self._graph = Database(self._connection_data["uri"], auth=self._connection_data["auth"]).default_graph
        return HTTPSession(self._graph, self._http)


class HTTPSDriver(Driver):

    uri_scheme = "https"

    _connection_data = None

    _http = None

    _graph = None

    def __new__(cls, uri, **config):
        cls._check_uri(uri)
        instance = object.__new__(cls)
        instance._connection_data = connection_data = get_connection_data(uri, **config)
        instance._http = HTTP(connection_data["uri"] + "/db/data/", {
            "Authorization": HTTP.authorization(connection_data["user"], connection_data["password"]),
            "User-Agent": connection_data["user_agent"],
            "X-Stream": "true",
        }, connection_data["verified"])
        instance._graph = None
        return instance

    def session(self, access_mode=None, bookmark=None):
        if self._graph is None:
            from py2neo.database import Database
            self._graph = Database(self._connection_data["uri"], auth=self._connection_data["auth"]).default_graph
        return HTTPSession(self._graph, self._http)


class HTTPResultLoader(object):

    def load(self, result):
        pass

    def fail(self):
        pass


class HTTPSession(Session):

    begin_ref = "transaction"

    autocommit_ref = "transaction/commit"

    transaction_ref = None      # e.g. "transaction/1"

    commit_ref = None           # e.g. "transaction/1/commit"

    def __init__(self, graph, http):
        self.graph = graph
        self.post = http.post
        self.delete = http.delete
        self.ref = self.autocommit_ref
        self._statements = []
        self._result_loaders = []
        self._bookmark = None

    def _connect(self, access_mode=None):
        pass

    def _disconnect(self, sync):
        if sync:
            self.sync()

    def run(self, statement, parameters=None, **kwparameters):
        if self.closed():
            raise SessionError("Session closed")
        if not statement:
            raise ValueError("Cannot run an empty statement")

        self._statements.append(OrderedDict([
            ("statement", ustr(statement)),
            ("parameters", fix_parameters(dict(parameters or {}, **kwparameters))),
            ("resultDataContents", ["REST"]),
            ("includeStats", True),
        ]))
        result_loader = HTTPResultLoader()
        self._result_loaders.append(result_loader)
        return HTTPStatementResult(self, result_loader)

    def send(self):
        self.sync()

    def fetch(self):
        return 0

    def sync(self):
        ref = self.ref
        # Some of the transactional URIs do not support empty statement
        # lists in versions earlier than 2.3. Which doesn't really matter
        # as it's a waste sending anything anyway.
        if ref in (self.autocommit_ref, self.begin_ref, self.transaction_ref) and not self._statements:
            return 0
        count = 0
        try:
            response = self.post(ref, {"statements": self._statements}, expected=(OK, CREATED))
            if response.status == 201:
                location_path = urlsplit(response.headers["Location"]).path
                self.transaction_ref = "".join(location_path.rpartition("transaction")[1:])
                self.commit_ref = "%s/commit" % self.transaction_ref
                self.ref = self.transaction_ref
            content = json_loads(response.data.decode("utf-8"))
            errors = content["errors"]
            if errors:
                from py2neo.database import GraphError
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

    def detach(self, result):
        return 0

    def last_bookmark(self):
        return None

    def commit_transaction(self):
        if not self.has_transaction():
            raise TransactionError("No transaction to commit")
        self._transaction = None
        self.ref = self.commit_ref or self.autocommit_ref
        try:
            self.sync()
        finally:
            self.commit_ref = self.transaction_ref = None
            self.ref = self.autocommit_ref
        self._bookmark = None
        return self._bookmark

    def rollback_transaction(self):
        if not self.has_transaction():
            raise TransactionError("No transaction to rollback")
        self._transaction = None
        self._bookmark = None
        try:
            if self.transaction_ref:
                self.ref = self.transaction_ref
                self.delete(self.ref, expected=(OK, NOT_FOUND))
        finally:
            self.commit_ref = self.transaction_ref = None
            self.ref = self.autocommit_ref

    def __begin__(self):
        self.ref = self.begin_ref


class HTTPStatementResult(StatementResult):

    zipper = Record

    def __init__(self, session, result_loader):
        from py2neo.internal.json import JSONHydrator

        super(HTTPStatementResult, self).__init__(session, JSONHydrator(session.graph, ()))

        def load(result):
            from neo4j import BoltStatementResultSummary

            keys = self._keys = self._hydrant.keys = tuple(result["columns"])
            hydrate = self._hydrant.hydrate
            for record in result["data"]:
                self._records.append(Record(zip(keys, hydrate(record["rest"]))))

            stats = result["stats"]
            # fix broken key
            if "relationship_deleted" in stats:
                stats["relationships_deleted"] = stats["relationship_deleted"]
                del stats["relationship_deleted"]
            if "contains_updates" in stats:
                del stats["contains_updates"]

            metadata = {"stats": stats}
            if "plan" in result:
                metadata["http_plan"] = result["plan"]

            # TODO: fill these in
            metadata["statement"] = None
            metadata["parameters"] = None

            metadata["server"] = ServerInfo(SocketAddress.from_uri(session.graph.database.uri))

            self._summary = BoltStatementResultSummary(**metadata)
            self._session = None
            return len(self._records)

        def fail():
            self._session = None

        result_loader.load = load
        result_loader.fail = fail

    def records(self):
        """ Generator for records obtained from this result.

        :yields: iterable of :class:`.Record` objects
        """
        if self.attached():
            self._session.send()
        records = self._records
        next_record = records.popleft
        while records:
            yield next_record()
