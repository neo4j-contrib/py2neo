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

from collections import deque, OrderedDict
from json import dumps as json_dumps, loads as json_loads

from certifi import where
from neobolt.direct import connect, ConnectionPool
from neobolt.routing import RoutingConnectionPool
from urllib3 import HTTPConnectionPool, HTTPSConnectionPool, make_headers

from py2neo.database import Cursor
from py2neo.internal.addressing import get_connection_data


class Connector(object):

    scheme = NotImplemented

    pool = None

    @classmethod
    def walk_subclasses(cls):
        subclasses = cls.__subclasses__()
        for subclass in subclasses:
            yield subclass
            for c in subclass.walk_subclasses():
                yield c

    def __new__(cls, uri, **settings):
        cx_data = get_connection_data(uri, **settings)
        for subclass in cls.walk_subclasses():
            if subclass.scheme == cx_data["scheme"]:
                inst = object.__new__(subclass)
                inst.open(cx_data)
                return inst
        raise ValueError("Unsupported scheme %r" % cx_data["scheme"])

    @property
    def server_agent(self):
        raise NotImplementedError()

    def open(self, cx_data):
        raise NotImplementedError()

    def close(self):
        raise NotImplementedError()

    def run(self, statement, parameters=None):
        raise NotImplementedError()


class BoltConnector(Connector):

    scheme = "bolt"

    @property
    def server_agent(self):
        with self.pool.acquire() as cx:
            return cx.server.agent

    def open(self, cx_data):

        def connector(address_, **kwargs):
            return connect(address_, auth=cx_data["auth"], **kwargs)

        address = (cx_data["host"], cx_data["port"])
        self.pool = ConnectionPool(connector, address)

    def close(self):
        self.pool.close()

    def run(self, statement, parameters=None):
        cx = self.pool.acquire()
        result = RecordBuffer(on_more=cx.fetch, on_done=lambda: self.pool.release(cx))
        cx.run(statement, parameters or {}, on_success=result.update_header)
        cx.pull_all(on_records=result.append_records, on_success=result.update_footer, on_summary=result.done)
        cx.send()
        return Cursor(result)


class BoltRoutingConnector(BoltConnector):

    scheme = "bolt+routing"

    def open(self, cx_data):

        def connector(address_, **kwargs):
            return connect(address_, auth=cx_data["auth"], **kwargs)

        address = (cx_data["host"], cx_data["port"])
        self.pool = RoutingConnectionPool(connector, address, {})


class HTTPConnector(Connector):

    scheme = "http"

    headers = None

    @property
    def server_agent(self):
        r = self.pool.request(method="GET",
                              url="/db/data/",
                              headers=dict(self.headers))
        return "Neo4j/{neo4j_version}".format(**json_loads(r.data.decode("utf-8")))

    def open(self, cx_data):
        self.pool = HTTPConnectionPool(host=cx_data["host"], port=cx_data["port"])
        self.headers = make_headers(basic_auth=":".join(cx_data["auth"]))

    def close(self):
        self.pool.close()

    def run(self, statement, parameters=None):
        r = self.pool.request(method="POST",
                              url="/db/data/transaction/commit",
                              headers=dict(self.headers, **{"Content-Type": "application/json"}),
                              body=json_dumps({
                                  "statements": [
                                      OrderedDict([
                                          ("statement", statement),
                                          ("parameters", parameters or {}),
                                          ("resultDataContents", ["REST"]),
                                          ("includeStats", True),
                                      ])
                                  ]
                              }))
        return Cursor(RecordBuffer.from_json(r.data.decode("utf-8")))


class SecureHTTPConnector(HTTPConnector):

    scheme = "https"

    def open(self, cx_data):
        self.pool = HTTPSConnectionPool(host=cx_data["host"], port=cx_data["port"],
                                        cert_reqs="CERT_NONE", ca_certs=where())
        self.headers = make_headers(basic_auth=":".join(cx_data["auth"]))


class RecordBuffer(object):
    """ Intermediary buffer for a result from a Cypher query.
    """

    @classmethod
    def from_json(cls, s):
        data = json_loads(s)
        result = data["results"][0]
        buffer = cls()
        buffer.update_header({"fields": result["columns"]})
        buffer.append_records(record["rest"] for record in result["data"])
        footer = {"stats": result["stats"]}
        if "plan" in result:
            footer["plan"] = result["plan"]
        buffer.update_footer(footer)
        buffer.done()
        return buffer

    def __init__(self, on_more=None, on_done=None):
        self._on_more = on_more
        self._on_done = on_done
        self._header = {}
        self._records = deque()
        self._footer = {}
        self._done = False

    def append_records(self, records):
        self._records.extend(records)

    def update_header(self, metadata):
        self._header.update(metadata)

    def update_footer(self, metadata):
        self._footer.update(metadata)

    def done(self):
        if callable(self._on_done):
            self._on_done()
        self._done = True

    def keys(self):
        if not self._header and not self._done and callable(self._on_more):
            self._on_more()
        return self._header.get("fields")

    def summary(self):
        # TODO
        while not self._done:
            if callable(self._on_more):
                self._on_more()
        return self._footer

    def plan(self):
        from py2neo.database import CypherPlan
        while not self._done:
            if callable(self._on_more):
                self._on_more()
        if "plan" in self._footer:
            return CypherPlan(**self._footer["plan"])
        elif "profile" in self._footer:
            return CypherPlan(**self._footer["profile"])
        else:
            return None

    def stats(self):
        from py2neo.database import CypherStats
        while not self._done:
            if callable(self._on_more):
                self._on_more()
        return CypherStats(**self._footer.get("stats", {}))

    def fetch(self):
        if self._records:
            return self._records.popleft()
        elif self._done:
            return None
        else:
            while not self._records:
                if callable(self._on_more):
                    self._on_more()
            return self._records.popleft()
