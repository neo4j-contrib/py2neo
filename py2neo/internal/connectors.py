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
    transactions = set()

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
                inst.connection_data = cx_data
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

    def is_valid_transaction(self, tx):
        return tx is not None and tx in self.transactions

    def _assert_valid_tx(self, tx):
        from py2neo import TransactionError
        if tx is None:
            raise TransactionError("No transaction")
        if tx not in self.transactions:
            raise TransactionError("Invalid transaction")

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
        cx = self.pool.acquire()
        try:
            return cx.server.agent
        finally:
            self.pool.release(cx)

    def open(self, cx_data):

        def connector(address_, **kwargs):
            return connect(address_, auth=cx_data["auth"], **kwargs)

        address = (cx_data["host"], cx_data["port"])
        self.pool = ConnectionPool(connector, address)

    def close(self):
        self.pool.close()

    def _run_1(self, statement, parameters, hydrator):
        cx = self.pool.acquire()
        result = RecordDeque(hydrator=hydrator, on_more=cx.fetch, on_done=lambda: self.pool.release(cx))
        cx.run(statement, parameters or {}, on_success=result.update_header, on_failure=self._fail)
        cx.pull_all(on_records=result.append_records, on_success=result.update_footer, on_failure=self._fail, on_summary=result.done)
        cx.send()
        cx.fetch()
        return Cursor(result)

    def _run_in_tx(self, statement, parameters, tx, hydrator):
        self._assert_valid_tx(tx)

        def fetch():
            tx.fetch()

        def fail(metadata):
            self.transactions.remove(tx)
            self.pool.release(tx)
            self._fail(metadata)

        result = RecordDeque(hydrator=hydrator, on_more=fetch)
        tx.run(statement, parameters or {}, on_success=result.update_header, on_failure=fail)
        tx.pull_all(on_records=result.append_records, on_success=result.update_footer, on_failure=fail, on_summary=result.done)
        tx.send()
        result.keys()   # force receipt of RUN summary, to detect any errors
        return Cursor(result)

    @classmethod
    def _fail(cls, metadata):
        from py2neo import GraphError
        raise GraphError.hydrate(metadata)

    def run(self, statement, parameters=None, tx=None, hydrator=None):
        if tx is None:
            return self._run_1(statement, parameters, hydrator)
        else:
            return self._run_in_tx(statement, parameters, tx, hydrator)

    def begin(self):
        tx = self.pool.acquire()
        tx.begin()
        self.transactions.add(tx)
        return tx

    def commit(self, tx):
        self._assert_valid_tx(tx)
        self.transactions.remove(tx)
        tx.commit()
        tx.sync()
        self.pool.release(tx)

    def rollback(self, tx):
        self._assert_valid_tx(tx)
        self.transactions.remove(tx)
        tx.rollback()
        tx.sync()
        self.pool.release(tx)

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
        return Cursor(RecordDeque.from_json(r.data.decode("utf-8"), hydrator=hydrator))

    def _run_in_tx(self, statement, parameters, tx, hydrator):
        from py2neo import GraphError
        r = self._post("/db/data/transaction/%s" % tx, statement, parameters)
        assert r.status == 200  # TODO: other codes
        try:
            return Cursor(RecordDeque.from_json(r.data.decode("utf-8"), hydrator=hydrator))
        except GraphError:
            self.transactions.remove(tx)
            raise

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

    def sync(self, tx):
        pass


class SecureHTTPConnector(HTTPConnector):

    scheme = "https"

    def open(self, cx_data):
        self.pool = HTTPSConnectionPool(host=cx_data["host"], port=cx_data["port"],
                                        cert_reqs="CERT_NONE", ca_certs=where())
        self.headers = make_headers(basic_auth=":".join(cx_data["auth"]))


class RecordDeque(object):
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
        if data.get("errors"):
            from py2neo.database import GraphError
            raise GraphError.hydrate(data["errors"][0])
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
        while not self._keys and not self._done and callable(self._on_more):
            self._on_more()
        return self._keys

    def buffer(self):
        while not self._done:
            if callable(self._on_more):
                self._on_more()

    def summary(self):
        # TODO
        self.buffer()
        return self._footer

    def plan(self):
        from py2neo.database import CypherPlan
        self.buffer()
        if "plan" in self._footer:
            return CypherPlan(**self._footer["plan"])
        elif "profile" in self._footer:
            return CypherPlan(**self._footer["profile"])
        else:
            return None

    def stats(self):
        from py2neo.database import CypherStats
        self.buffer()
        return CypherStats(**self._footer.get("stats", {}))

    def fetch(self):
        from py2neo.database import Record
        if self._records:
            return Record(zip(self.keys(), self._records.popleft()))
        elif self._done:
            return None
        else:
            while not self._records and not self._done:
                if callable(self._on_more):
                    self._on_more()
            try:
                return Record(zip(self.keys(), self._records.popleft()))
            except IndexError:
                return None
