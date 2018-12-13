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
from neobolt.packstream import Structure
from neobolt.routing import RoutingConnectionPool
from urllib3 import HTTPConnectionPool, HTTPSConnectionPool, make_headers

from py2neo.internal.compat import urlsplit
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

    def run(self, statement, parameters=None, tx=None, hydrator=None):
        raise NotImplementedError()

    def begin(self):
        raise NotImplementedError()

    def commit(self, tx):
        raise NotImplementedError()

    def rollback(self, tx):
        raise NotImplementedError()

    def sync(self, tx):
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

    def _assert_valid_tx(self, cx):
        if cx is None:
            raise ValueError("No connection")
        if cx.pool is not self.pool:
            raise ValueError("Connection does not belong to pool")

    def _run_1(self, statement, parameters, hydrator):
        cx = self.pool.acquire()
        result = RecordBuffer(hydrator=hydrator, on_more=cx.fetch, on_done=lambda: self.pool.release(cx))
        cx.run(statement, parameters or {}, on_success=result.update_header)
        cx.pull_all(on_records=result.append_records, on_success=result.update_footer, on_summary=result.done)
        cx.send()
        return Cursor(result)

    def _run_in_tx(self, statement, parameters, cx, hydrator):
        self._assert_valid_tx(cx)

        def fetch():
            cx.send()
            cx.fetch()

        result = RecordBuffer(hydrator=hydrator, on_more=fetch)
        cx.run(statement, parameters or {}, on_success=result.update_header)
        cx.pull_all(on_records=result.append_records, on_success=result.update_footer, on_summary=result.done)
        return Cursor(result)

    def run(self, statement, parameters=None, tx=None, hydrator=None):
        if tx is None:
            return self._run_1(statement, parameters, hydrator)
        else:
            return self._run_in_tx(statement, parameters, tx, hydrator)

    def begin(self):
        cx = self.pool.acquire()
        cx.begin()
        return cx

    def commit(self, cx):
        self._assert_valid_tx(cx)
        cx.commit()
        self.pool.release(cx)

    def rollback(self, cx):
        self._assert_valid_tx(cx)
        cx.rollback()
        self.pool.release(cx)

    def sync(self, cx):
        cx.sync()


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

    def _assert_valid_tx(self, tx):
        if tx is None:
            raise ValueError("No transaction")

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
        return self.pool.request(method="POST",
                                 url=url,
                                 headers=dict(self.headers, **{"Content-Type": "application/json"}),
                                 body=json_dumps({"statements": statements}))

    def _delete(self, url):
        return self.pool.request(method="DELETE",
                                 url=url,
                                 headers=dict(self.headers))

    def _run_1(self, statement, parameters, hydrator):
        r = self._post("/db/data/transaction/commit", statement, parameters)
        return Cursor(RecordBuffer.from_json(r.data.decode("utf-8"), hydrator=hydrator))

    def _run_in_tx(self, statement, parameters, tx, hydrator):
        r = self._post("/db/data/transaction/%s" % tx, statement, parameters)
        assert r.status == 200
        return Cursor(RecordBuffer.from_json(r.data.decode("utf-8"), hydrator=hydrator))

    def run(self, statement, parameters=None, tx=None, hydrator=None):
        if tx is None:
            return self._run_1(statement, parameters, hydrator)
        else:
            return self._run_in_tx(statement, parameters, tx, hydrator)

    def begin(self):
        r = self._post("/db/data/transaction")
        if r.status == 201:
            location_path = urlsplit(r.headers["Location"]).path
            tx = location_path.rpartition("/")[-1]
            return tx
        else:
            raise RuntimeError("Can't begin a new transaction")

    def commit(self, tx):
        self._assert_valid_tx(tx)
        self._post("/db/data/transaction/%s/commit" % tx)

    def rollback(self, tx):
        self._assert_valid_tx(tx)
        self._delete("/db/data/transaction/%s" % tx)

    def sync(self, tx):
        pass


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
    def uri_to_id(cls, uri):
        _, _, identity = uri.rpartition("/")
        return int(identity)

    @classmethod
    def _partially_hydrate_object(cls, data):
        if "self" in data:
            if "type" in data:
                return Structure(b"R",
                                 cls.uri_to_id(data["self"]),
                                 cls.uri_to_id(data["start"]),
                                 cls.uri_to_id(data["end"]),
                                 data["type"],
                                 data["data"])
            else:
                return Structure(b"N",
                                 cls.uri_to_id(data["self"]),
                                 data["metadata"]["labels"],
                                 data["data"])
        elif "nodes" in data and "relationships" in data:
            # TODO
            data["nodes"] = list(map(uri_to_id, data["nodes"]))
            data["relationships"] = list(map(uri_to_id, data["relationships"]))
            if "directions" not in data:
                directions = []
                relationships = graph.evaluate(
                    "MATCH ()-[r]->() WHERE id(r) IN {x} RETURN collect(r)",
                    x=data["relationships"])
                for i, relationship in enumerate(relationships):
                    if relationship.start_node.identity == data["nodes"][i]:
                        directions.append("->")
                    else:
                        directions.append("<-")
                data["directions"] = directions
            return hydrate_path(graph, data)
        else:
            # from warnings import warn
            # warn("Map literals returned over the Neo4j REST interface are ambiguous "
            #      "and may be hydrated as graph objects")
            return data

    @classmethod
    def from_json(cls, s, hydrator=None):
        data = json_loads(s, object_hook=cls._partially_hydrate_object)     # TODO: other partial hydration
        result = data["results"][0]
        buffer = cls(hydrator=hydrator)
        buffer.update_header({"fields": result["columns"]})
        buffer.append_records(record["rest"] for record in result["data"])
        footer = {"stats": result["stats"]}
        if "plan" in result:
            footer["plan"] = result["plan"]
        buffer.update_footer(footer)
        buffer.done()
        return buffer

    def __init__(self, hydrator=None, on_more=None, on_done=None):
        self._hydrator = hydrator
        self._on_more = on_more
        self._on_done = on_done
        self._header = {}
        self._keys = None
        self._records = deque()
        self._footer = {}
        self._done = False

    def append_records(self, records):
        if self._hydrator:
            self._records.extend(tuple(self._hydrator.hydrate(record)) for record in records)
        else:
            self._records.extend(tuple(record) for record in records)

    def update_header(self, metadata):
        self._header.update(metadata)
        self._keys = self._header.get("fields")
        if self._hydrator is not None:
            self._hydrator.keys = self._keys    # TODO: unscrew this

    def update_footer(self, metadata):
        self._footer.update(metadata)

    def done(self):
        if callable(self._on_done):
            self._on_done()
        self._done = True

    def keys(self):
        if not self._keys and not self._done and callable(self._on_more):
            self._on_more()
        return self._keys

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
            while not self._records and not self._done:
                if callable(self._on_more):
                    self._on_more()
            try:
                return self._records.popleft()
            except IndexError:
                return None
