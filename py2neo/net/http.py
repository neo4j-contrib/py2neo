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


from collections import OrderedDict
from logging import getLogger
from json import dumps as json_dumps, loads as json_loads

from urllib3 import HTTPConnectionPool, make_headers

from py2neo import http_user_agent
from py2neo.internal.compat import urlsplit
from py2neo.net import Connection
from py2neo.net.api import Result, Failure


log = getLogger(__name__)


class HTTP(Connection):

    scheme = "http"

    @classmethod
    def open(cls, service, user_agent=None):
        http = cls(service, (user_agent or http_user_agent()))
        http.hello()
        return http

    def __init__(self, service, user_agent):
        super(HTTP, self).__init__(service, user_agent)
        self.http_pool = HTTPConnectionPool(
            host=service.host,
            port=service.port_number,
            maxsize=1,
            block=True,
        )
        self.headers = make_headers(basic_auth=":".join(service.auth))
        self.transactions = set()
        self.__closed = False

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

    def hello(self):
        r = self.http_pool.request(method="GET",
                              url="/",
                              headers=dict(self.headers))
        metadata = json_loads(r.data.decode("utf-8"))
        if "neo4j_version" in metadata:     # Neo4j 4.x
            # {
            #   "bolt_routing" : "neo4j://localhost:7687",
            #   "transaction" : "http://localhost:7474/db/{databaseName}/tx",
            #   "bolt_direct" : "bolt://localhost:7687",
            #   "neo4j_version" : "4.0.0",
            #   "neo4j_edition" : "community"
            # }
            neo4j_version = metadata["neo4j_version"]
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
            neo4j_version = metadata["neo4j_version"]
        self.server_agent = "Neo4j/{}".format(neo4j_version)

    def auto_run(self, cypher, parameters=None,
                 db=None, readonly=False, bookmarks=None, metadata=None, timeout=None):
        r = self._post("/db/data/transaction/commit", cypher, parameters)
        assert r.status == 200  # TODO: other codes
        data = json_loads(r.data.decode("utf-8"))
        if data.get("errors"):
            raise Failure(**data["errors"][0])
        return HTTPResult(data["results"][0])

    def begin(self, db=None, readonly=False, bookmarks=None, metadata=None, timeout=None):
        r = self._post("/db/data/transaction")
        if r.status == 201:
            location_path = urlsplit(r.headers["Location"]).path
            tx = location_path.rpartition("/")[-1]
            self.transactions.add(tx)
            return tx
        else:
            raise RuntimeError("Can't begin a new transaction")

    def commit(self, tx):
        self._assert_valid_tx(tx)
        self.transactions.remove(tx)
        self._post("/db/data/transaction/%s/commit" % tx)

    def rollback(self, tx):
        self._assert_valid_tx(tx)
        self.transactions.remove(tx)
        self._delete("/db/data/transaction/%s" % tx)

    def run_in_tx(self, tx, cypher, parameters=None):
        r = self._post("/db/data/transaction/%s" % tx, cypher, parameters)
        assert r.status == 200  # TODO: other codes
        data = json_loads(r.data.decode("utf-8"))
        if data.get("errors"):
            raise Failure(**data["errors"][0])
        return HTTPResult(data["results"][0])

    def pull(self, result, n=-1):
        pass

    def discard(self, result, n=-1):
        pass

    def wait(self, result):
        pass

    def take(self, result):
        pass

    def _assert_valid_tx(self, tx):
        from py2neo.database import TransactionError
        if not tx:
            raise TransactionError("No transaction")
        if tx not in self.transactions:
            raise TransactionError("Invalid transaction")

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


class HTTPResult(Result):

    def __init__(self, result):
        super(Result, self).__init__()
        self.columns = result.get("columns")
        self.data = result.get("data")
        self.cursor = 0

    def has_records(self):
        return self.cursor < len(self.data)

    def take_record(self):
        try:
            record = self.data[self.cursor]["rest"]
        except IndexError:
            return None
        else:
            self.cursor += 1
            return record
