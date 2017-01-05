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
from json import loads as json_loads
from urllib3 import HTTPConnectionPool, HTTPSConnectionPool


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




from neo4j.compat import urlparse
from neo4j.v1 import ProtocolError, CypherError
from neo4j.v1.api import ValueSystem, GraphDatabase, Driver, Session, StatementResult
from neo4j.v1.types import Record, Node, Relationship, UnboundRelationship, Path

from py2neo.json import JSONValueSystem
from py2neo.addressing import authenticate
from py2neo.compat import urlsplit
from py2neo.graph import GraphService, GraphError
from py2neo.http import WebResource, get_http_headers
from py2neo.remoting import remote


DEFAULT_PORT = 7474


class HTTPDriver(Driver):

    def __init__(self, uri, **config):
        super(HTTPDriver, self).__init__(None)
        authenticate("localhost:7474", "neo4j", "password")
        self.graph_service = GraphService(uri)
        self.graph = self.graph_service.graph

    def session(self, access_mode=None):
        return HTTPSession(self.graph)


GraphDatabase.uri_schemes["http"] = HTTPDriver


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
        ]))
        result_loader = HTTPResultLoader()
        self._result_loaders.append(result_loader)
        return HTTPStatementResult(self, result_loader)

    def fetch(self):
        return self.sync()

    def sync(self):
        count = 0
        try:
            response = self.resource.post({"statements": self._statements}, expected=(200, 201))
            if response.status == 201:
                self.transaction_path = urlsplit(response.headers["Location"]).path
                self.commit_path = "%s/commit" % self.transaction_path
                self.resource.path = self.transaction_path
            content = json_loads(response.data.decode("utf-8"))
            errors = content["errors"]
            if errors:
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
                self.resource.request("DELETE")
        finally:
            self.commit_path = self.transaction_path = None
            self.resource.path = self.autocommit_path


class HTTPStatementResult(StatementResult):

    value_system = None

    zipper = Record

    def __init__(self, session, result_loader):
        super(HTTPStatementResult, self).__init__(session)
        self.value_system = JSONValueSystem(session.graph, ())

        def load(result):
            self._keys = self.value_system.keys = tuple(result["columns"])
            self._records.extend(record["rest"] for record in result["data"])
            self._session = None
            return len(self._records)

        def fail():
            self._session = None

        result_loader.load = load
        result_loader.fail = fail
