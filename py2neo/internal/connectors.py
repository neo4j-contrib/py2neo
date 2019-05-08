#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2019, Nigel Small
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
from hashlib import new as hashlib_new
from json import dumps as json_dumps, loads as json_loads

from certifi import where
from neobolt.direct import connect, ConnectionPool
from neobolt.routing import RoutingConnectionPool
from urllib3 import HTTPConnectionPool, HTTPSConnectionPool, make_headers

from py2neo.internal.compat import bstr, urlsplit
from py2neo.internal.hydration import CypherResult, JSONHydrator, PackStreamHydrator, HydrationError
from py2neo.meta import NEO4J_URI, NEO4J_AUTH, NEO4J_USER_AGENT, NEO4J_SECURE, NEO4J_VERIFIED, \
    bolt_user_agent, http_user_agent


DEFAULT_SCHEME = "bolt"
DEFAULT_SECURE = False
DEFAULT_VERIFIED = False
DEFAULT_USER = "neo4j"
DEFAULT_PASSWORD = "password"
DEFAULT_HOST = "localhost"
DEFAULT_BOLT_PORT = 7687
DEFAULT_HTTP_PORT = 7474
DEFAULT_HTTPS_PORT = 7473


def coalesce(*values):
    """ Utility function to return the first non-null value from a
    sequence of values.
    """
    for value in values:
        if value is not None:
            return value
    return None


def get_connection_data(uri=None, **settings):
    """ Generate a dictionary of connection data for an optional URI plus
    additional connection settings.

    :param uri:
    :param settings:
    :return:
    """
    data = {
        "host": None,
        "password": None,
        "port": None,
        "scheme": None,
        "secure": None,
        "verified": None,
        "user": None,
        "user_agent": None,
    }
    # apply uri
    uri = coalesce(uri, NEO4J_URI)
    if uri is not None:
        parsed = urlsplit(uri)
        if parsed.scheme is not None:
            data["scheme"] = parsed.scheme
            if data["scheme"] in ["https"]:
                data["secure"] = True
            elif data["scheme"] in ["http"]:
                data["secure"] = False
        data["user"] = coalesce(parsed.username, data["user"])
        data["password"] = coalesce(parsed.password, data["password"])
        data["host"] = coalesce(parsed.hostname, data["host"])
        data["port"] = coalesce(parsed.port, data["port"])
    # apply auth (this can override `uri`)
    if "auth" in settings and settings["auth"] is not None:
        data["user"], data["password"] = settings["auth"]
    elif NEO4J_AUTH is not None:
        data["user"], _, data["password"] = NEO4J_AUTH.partition(":")
    # apply components (these can override `uri` and `auth`)
    data["user_agent"] = coalesce(settings.get("user_agent"), NEO4J_USER_AGENT, data["user_agent"])
    data["secure"] = coalesce(settings.get("secure"), data["secure"], NEO4J_SECURE)
    data["verified"] = coalesce(settings.get("verified"), data["verified"], NEO4J_VERIFIED)
    data["scheme"] = coalesce(settings.get("scheme"), data["scheme"])
    data["user"] = coalesce(settings.get("user"), data["user"])
    data["password"] = coalesce(settings.get("password"), data["password"])
    data["host"] = coalesce(settings.get("host"), data["host"])
    data["port"] = coalesce(settings.get("port"), data["port"])
    # apply correct scheme for security
    if data["secure"] is True and data["scheme"] == "http":
        data["scheme"] = "https"
    if data["secure"] is False and data["scheme"] == "https":
        data["scheme"] = "http"
    # apply default port for scheme
    if data["scheme"] and not data["port"]:
        if data["scheme"] == "http":
            data["port"] = DEFAULT_HTTP_PORT
        elif data["scheme"] == "https":
            data["port"] = DEFAULT_HTTPS_PORT
        elif data["scheme"] in ["bolt", "bolt+routing"]:
            data["port"] = DEFAULT_BOLT_PORT
    # apply other defaults
    if not data["user_agent"]:
        data["user_agent"] = http_user_agent() if data["scheme"] in ["http", "https"] else bolt_user_agent()
    if data["secure"] is None:
        data["secure"] = DEFAULT_SECURE
    if data["verified"] is None:
        data["verified"] = DEFAULT_VERIFIED
    if not data["scheme"]:
        data["scheme"] = DEFAULT_SCHEME
        if data["scheme"] == "http":
            data["secure"] = False
            data["verified"] = False
        if data["scheme"] == "https":
            data["secure"] = True
            data["verified"] = True
    if not data["user"]:
        data["user"] = DEFAULT_USER
    if not data["password"]:
        data["password"] = DEFAULT_PASSWORD
    if not data["host"]:
        data["host"] = DEFAULT_HOST
    if not data["port"]:
        data["port"] = DEFAULT_BOLT_PORT
    # apply composites
    data["auth"] = (data["user"], data["password"])
    data["uri"] = "%s://%s:%s" % (data["scheme"], data["host"], data["port"])
    h = hashlib_new("md5")
    for key in sorted(data):
        h.update(bstr(data[key]))
    data["hash"] = h.hexdigest()
    return data


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

    def run(self, statement, parameters=None, tx=None, graph=None, keys=None, entities=None):
        raise NotImplementedError()

    def is_valid_transaction(self, tx):
        return tx is not None and tx in self.transactions

    def _assert_valid_tx(self, tx):
        from py2neo.database import TransactionError
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
            return connect(address_, auth=cx_data["auth"],
                           encrypted=cx_data["secure"], **kwargs)

        address = (cx_data["host"], cx_data["port"])
        self.pool = ConnectionPool(connector, address)

    def close(self):
        self.pool.close()

    def _run_1(self, statement, parameters, graph, keys, entities):
        cx = self.pool.acquire()
        hydrator = PackStreamHydrator(version=cx.protocol_version, graph=graph, keys=keys, entities=entities)
        dehydrated_parameters = hydrator.dehydrate(parameters)
        result = CypherResult(on_more=cx.fetch, on_done=lambda: self.pool.release(cx))
        result.update_metadata({"connection": self.connection_data})

        def update_metadata_with_keys(metadata):
            result.update_metadata(metadata)
            hydrator.keys = result.keys()

        cx.run(statement, dehydrated_parameters or {}, on_success=update_metadata_with_keys, on_failure=self._fail)
        cx.pull_all(on_records=lambda records: result.append_records(map(hydrator.hydrate, records)),
                    on_success=result.update_metadata, on_failure=self._fail, on_summary=result.done)
        cx.send()
        cx.fetch()
        return result

    def _run_in_tx(self, statement, parameters, tx, graph, keys, entities):
        self._assert_valid_tx(tx)

        def fetch():
            tx.fetch()

        def fail(metadata):
            self.transactions.remove(tx)
            self.pool.release(tx)
            self._fail(metadata)

        hydrator = PackStreamHydrator(version=tx.protocol_version, graph=graph, keys=keys, entities=entities)
        dehydrated_parameters = hydrator.dehydrate(parameters)
        result = CypherResult(on_more=fetch)
        result.update_metadata({"connection": self.connection_data})

        def update_metadata_with_keys(metadata):
            result.update_metadata(metadata)
            hydrator.keys = result.keys()

        tx.run(statement, dehydrated_parameters or {}, on_success=update_metadata_with_keys, on_failure=fail)
        tx.pull_all(on_records=lambda records: result.append_records(map(hydrator.hydrate, records)),
                    on_success=result.update_metadata, on_failure=fail, on_summary=result.done)
        tx.send()
        result.keys()   # force receipt of RUN summary, to detect any errors
        return result

    @classmethod
    def _fail(cls, metadata):
        from py2neo.database import GraphError
        raise GraphError.hydrate(metadata)

    def run(self, statement, parameters=None, tx=None, graph=None, keys=None, entities=None):
        if tx is None:
            return self._run_1(statement, parameters, graph, keys, entities)
        else:
            return self._run_in_tx(statement, parameters, tx, graph, keys, entities)

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
            return connect(address_, auth=cx_data["auth"],
                           encrypted=cx_data["secure"], **kwargs)

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

    def run(self, statement, parameters=None, tx=None, graph=None, keys=None, entities=None):
        hydrator = JSONHydrator(version="rest", graph=graph, keys=keys, entities=entities)
        r = self._post("/db/data/transaction/%s" % (tx or "commit"), statement, hydrator.dehydrate(parameters))
        assert r.status == 200  # TODO: other codes
        try:
            raw_result = hydrator.hydrate_result(r.data.decode("utf-8"))
        except HydrationError as e:
            from py2neo.database import GraphError  # TODO: breaks abstraction layers :(
            if tx is not None:
                self.transactions.remove(tx)
            raise GraphError.hydrate(e.args[0])
        else:
            result = CypherResult({
                "connection": self.connection_data,
                "fields": raw_result.get("columns"),
                "plan": raw_result.get("plan"),
                "stats": raw_result.get("stats"),
            })
            hydrator.keys = result.keys()
            result.append_records(hydrator.hydrate(record[hydrator.version]) for record in raw_result["data"])
            result.done()
            return result

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
