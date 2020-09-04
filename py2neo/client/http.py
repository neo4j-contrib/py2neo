#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2020, Nigel Small
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
from logging import getLogger
from json import dumps as json_dumps, loads as json_loads

from packaging.version import Version
from six import raise_from
from urllib3 import HTTPConnectionPool, HTTPSConnectionPool, make_headers
from urllib3.exceptions import ConnectionError, HTTPError

from py2neo.compat import urlsplit
from py2neo.client import Connection, Transaction, Result, Bookmark, \
    TransactionError, BrokenTransactionError
from py2neo.client.config import http_user_agent
from py2neo.client.json import JSONHydrant


log = getLogger(__name__)


class HTTP(Connection):

    @classmethod
    def default_hydrant(cls, profile, graph):
        return JSONHydrant(graph)

    @classmethod
    def open(cls, profile, user_agent=None, on_bind=None, on_unbind=None, on_release=None):
        http = cls(profile, (user_agent or http_user_agent()),
                   on_bind=on_bind, on_unbind=on_unbind, on_release=on_release)
        http._hello()
        return http

    def __init__(self, profile, user_agent, on_bind=None, on_unbind=None, on_release=None):
        super(HTTP, self).__init__(profile, user_agent,
                                   on_bind=on_bind, on_unbind=on_unbind, on_release=on_release)
        self.http_pool = None
        self.headers = make_headers(basic_auth=":".join(profile.auth))
        self._transactions = set()
        self.__closed = False
        self._make_pool(profile)

    def _make_pool(self, profile):
        if profile.secure:
            from ssl import CERT_NONE, CERT_REQUIRED
            from certifi import where as cert_where
            self.http_pool = HTTPSConnectionPool(
                host=profile.host,
                port=profile.port_number,
                maxsize=1,
                block=True,
                cert_reqs=CERT_REQUIRED if profile.verify else CERT_NONE,
                ca_certs=cert_where()
            )
        else:
            self.http_pool = HTTPConnectionPool(
                host=profile.host,
                port=profile.port_number,
                maxsize=1,
                block=True,
            )

    def close(self):
        self.http_pool.close()
        self.__closed = True

    @property
    def closed(self):
        return self.__closed

    @property
    def broken(self):
        return False

    @property
    def local_port(self):
        raise NotImplementedError

    def _hello(self):
        r = self.http_pool.request(method="GET",
                                   url="/",
                                   headers=dict(self.headers))
        metadata = json_loads(r.data.decode("utf-8"))
        if "neo4j_version" in metadata:
            # {
            #   "bolt_routing" : "neo4j://localhost:7687",
            #   "transaction" : "http://localhost:7474/db/{databaseName}/tx",
            #   "bolt_direct" : "bolt://localhost:7687",
            #   "neo4j_version" : "4.0.0",
            #   "neo4j_edition" : "community"
            # }
            self.neo4j_version = Version(metadata["neo4j_version"])  # Neo4j 4.x
        else:                               # Neo4j 3.x
            # {
            #   "data" : "http://localhost:7474/db/data/",
            #   "management" : "http://localhost:7474/db/manage/",
            #   "bolt" : "bolt://localhost:7687"
            # }
            r = self.http_pool.request(method="GET",
                                       url="/db/data/",
                                       headers=dict(self.headers))
            metadata = json_loads(r.data.decode("utf-8"))
            # {
            #   "extensions" : { },
            #   "node" : "http://localhost:7474/db/data/node",
            #   "relationship" : "http://localhost:7474/db/data/relationship",
            #   "node_index" : "http://localhost:7474/db/data/index/node",
            #   "relationship_index" : "http://localhost:7474/db/data/index/relationship",
            #   "extensions_info" : "http://localhost:7474/db/data/ext",
            #   "relationship_types" : "http://localhost:7474/db/data/relationship/types",
            #   "batch" : "http://localhost:7474/db/data/batch",
            #   "cypher" : "http://localhost:7474/db/data/cypher",
            #   "indexes" : "http://localhost:7474/db/data/schema/index",
            #   "constraints" : "http://localhost:7474/db/data/schema/constraint",
            #   "transaction" : "http://localhost:7474/db/data/transaction",
            #   "node_labels" : "http://localhost:7474/db/data/labels",
            #   "neo4j_version" : "3.5.12"
            # }
            self.neo4j_version = Version(metadata["neo4j_version"])  # Neo4j 3.x
        self.server_agent = "Neo4j/{}".format(self.neo4j_version)

    def fast_forward(self, bookmark):
        raise NotImplementedError("Bookmarking is not yet supported over HTTP")

    def auto_run(self, graph_name, cypher, parameters=None,
                 # readonly=False, after=None, metadata=None, timeout=None
                 ):
        if graph_name and not self.supports_multi():
            raise TypeError("Neo4j {} does not support "
                            "named graphs".format(self.neo4j_version))
        r = self._post(HTTPTransaction.autocommit_uri(graph_name), cypher, parameters)
        assert r.status == 200  # TODO: other codes
        rs = HTTPResponse.from_json(r.status, r.data.decode("utf-8"))
        self.release()
        rs.audit()
        return HTTPResult(graph_name, rs.result())

    def begin(self, graph_name,
              # readonly=False, after=None, metadata=None, timeout=None
              ):
        if graph_name and not self.supports_multi():
            raise TypeError("Neo4j {} does not support "
                            "named graphs".format(self.neo4j_version))
        # if readonly:
        #     raise TypeError("Readonly transactions are not supported over HTTP")
        # if after:
        #     raise TypeError("Bookmarks are not supported over HTTP")
        # if metadata:
        #     raise TypeError("Transaction metadata is not supported over HTTP")
        # if timeout:
        #     raise TypeError("Transaction timeouts are not supported over HTTP")
        try:
            r = self._post(HTTPTransaction.begin_uri(graph_name))
        except ConnectionError as error:
            raise_from(TransactionError("Transaction failed to begin"), error)
        except HTTPError as error:
            raise_from(TransactionError("Transaction failed to begin"), error)
        else:
            if r.status != 201:
                raise TransactionError("Transaction failed to begin "
                                       "due to HTTP status %r" % r.status)
            rs = HTTPResponse.from_json(r.status, r.data.decode("utf-8"))
            location_path = urlsplit(r.headers["Location"]).path
            tx = HTTPTransaction(graph_name, location_path.rpartition("/")[-1])
            self._transactions.add(tx)
            self.release()
            rs.audit(tx)
            return tx

    def commit(self, tx):
        self._assert_transaction_open(tx)
        self._transactions.remove(tx)
        try:
            r = self._post(tx.commit_uri())
        except ConnectionError as error:
            tx.mark_broken()
            raise_from(BrokenTransactionError("Transaction broken by disconnection "
                                              "during commit"), error)
        except HTTPError as error:
            tx.mark_broken()
            raise_from(BrokenTransactionError("Transaction broken by HTTP error "
                                              "during commit"), error)
        else:
            if r.status != 200:
                tx.mark_broken()
                raise BrokenTransactionError("Transaction broken by HTTP %d status "
                                             "during commit" % r.status)
            rs = HTTPResponse.from_json(r.status, r.data.decode("utf-8"))
            self.release()
            rs.audit(tx)
            return Bookmark()

    def rollback(self, tx):
        self._assert_transaction_open(tx)
        self._transactions.remove(tx)
        try:
            r = self._delete(tx.uri())
        except ConnectionError as error:
            tx.mark_broken()
            raise_from(BrokenTransactionError("Transaction broken by disconnection "
                                              "during rollback"), error)
        except HTTPError as error:
            tx.mark_broken()
            raise_from(BrokenTransactionError("Transaction broken by HTTP error "
                                              "during rollback"), error)
        else:
            if r.status != 200:
                tx.mark_broken()
                raise BrokenTransactionError("Transaction broken by HTTP %d status "
                                             "during rollback" % r.status)
            rs = HTTPResponse.from_json(r.status, r.data.decode("utf-8"))
            self.release()
            rs.audit(tx)
            return Bookmark()

    def run_in_tx(self, tx, cypher, parameters=None):
        r = self._post(tx.uri(), cypher, parameters)
        if r.status != 200:
            tx.mark_broken()
            raise BrokenTransactionError("Transaction broken by HTTP %d status "
                                         "during query execution" % r.status)
        rs = HTTPResponse.from_json(r.status, r.data.decode("utf-8"))
        self.release()
        rs.audit(tx)
        return HTTPResult(tx.graph_name, rs.result(), profile=self.profile)

    def pull(self, result, n=-1):
        pass

    def discard(self, result, n=-1):
        pass

    def sync(self, result):
        pass

    def fetch(self, result):
        record = result.take_record()
        return record

    def _assert_transaction_open(self, tx):
        if tx not in self._transactions:
            raise ValueError("Transaction %r is not open on this connection", tx)
        if tx.broken:
            raise ValueError("Transaction is broken")

    def _post(self, url, statement=None, parameters=None):
        if statement:
            statements = [
                OrderedDict([
                    ("statement", statement),
                    ("parameters", parameters or {}),
                    ("resultDataContents", ["REST"]),
                    ("includeStats", True),
                ])
            ]
        else:
            statements = []
        return self.http_pool.request(method="POST",
                                      url=url,
                                      headers=dict(self.headers, **{"Content-Type": "application/json"}),
                                      body=json_dumps({"statements": statements}))

    def _delete(self, url):
        return self.http_pool.request(method="DELETE",
                                      url=url,
                                      headers=dict(self.headers))

    def supports_multi(self):
        return self.neo4j_version >= Version("4.0")


class HTTPTransaction(Transaction):

    def __init__(self, graph_name, txid=None, readonly=False):
        super(HTTPTransaction, self).__init__(graph_name, txid, readonly)
        self.failure = None

    def __bool__(self):
        return not self.broken

    __nonzero__ = __bool__

    @classmethod
    def autocommit_uri(cls, graph_name):
        if graph_name:
            return "/db/{}/tx/commit".format(graph_name)
        else:
            return "/db/data/transaction/commit"

    @classmethod
    def begin_uri(cls, graph_name):
        if graph_name:
            return "/db/{}/tx".format(graph_name)
        else:
            return "/db/data/transaction"

    def uri(self):
        if self.graph_name:
            return "/db/{}/tx/{}".format(self.graph_name, self.txid)
        else:
            return "/db/data/transaction/{}".format(self.txid)

    def commit_uri(self):
        if self.graph_name:
            return "/db/{}/tx/{}/commit".format(self.graph_name, self.txid)
        else:
            return "/db/data/transaction/{}/commit".format(self.txid)


class HTTPResult(Result):

    def __init__(self, graph_name, result, profile=None):
        Result.__init__(self, graph_name)
        self._columns = result.get("columns", ())
        self._data = result.get("data", [])
        self._summary = {}
        if "stats" in result:
            self._summary["stats"] = result["stats"]
        if profile:
            self._summary["connection"] = dict(profile)
        self._cursor = 0

    def buffer(self):
        pass

    def fields(self):
        return self._columns

    def summary(self):
        return self._summary

    def fetch(self):
        return self.take_record()

    def has_records(self):
        return self._cursor < len(self._data)

    def take_record(self):
        try:
            record = self._data[self._cursor]["rest"]
        except IndexError:
            return None
        else:
            self._cursor += 1
            return record

    def peek_records(self, limit):
        records = []
        for i in range(limit):
            try:
                records.append(self._data[self._cursor + i]["rest"])
            except IndexError:
                break
        return records


class HTTPResponse(object):

    @classmethod
    def from_json(cls, status, data):
        return cls(status, json_loads(data, object_hook=JSONHydrant.json_to_packstream))

    def __init__(self, status, content):
        self._status = status
        self._content = content

    @property
    def status(self):
        return self._status

    def columns(self):
        return tuple(self._content.get("columns", ()))

    def result(self, index=0):
        try:
            results = self._content["results"]
        except KeyError:
            return {}  # TODO return None, somehow
        else:
            try:
                return results[index]
            except IndexError:
                return {}  # TODO return None, somehow

    def stats(self):
        return self._content.get("stats", {})

    def errors(self):
        return self._content.get("errors", [])

    def audit(self, tx=None):
        if self.errors():
            # FIXME: This currently clumsily breaks through abstraction
            #   layers. This should create a Failure instead of a
            #   Neo4jError. See also BoltResponse.set_failure
            from py2neo.database.work import Neo4jError
            failure = Neo4jError.hydrate(self.errors().pop(0))
            if tx is not None:
                tx.mark_broken()
            raise failure
