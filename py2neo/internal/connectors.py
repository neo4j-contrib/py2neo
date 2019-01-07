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

from collections import deque, namedtuple, OrderedDict
from hashlib import new as hashlib_new
from json import dumps as json_dumps, loads as json_loads

from certifi import where
from neobolt.direct import connect, ConnectionPool
from neobolt.packstream import Structure
from neobolt.routing import RoutingConnectionPool
from urllib3 import HTTPConnectionPool, HTTPSConnectionPool, make_headers

from py2neo.internal.compat import Sequence, Mapping, bstr, integer_types, string_types, urlsplit
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


INT64_LO = -(2 ** 63)
INT64_HI = 2 ** 63 - 1


def coalesce(*values):
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


class PackStreamHydrator(object):

    unbound_relationship = namedtuple("UnboundRelationship", ["id", "type", "properties"])

    def __init__(self, graph, keys, entities=None):
        # TODO: protocol version
        # super(PackStreamHydrator, self).__init__(2)  # maximum known protocol version
        self.graph = graph
        self.keys = keys
        self.entities = entities or {}

    def hydrate_records(self, keys, record_values):
        from py2neo.data import Record
        for values in record_values:
            yield Record(zip(keys, self.hydrate(values)))

    def hydrate(self, values):
        """ Convert PackStream values into native values.
        """
        from py2neo.data import Node, Relationship, Path
        from py2neo.matching import RelationshipMatcher

        graph = self.graph
        entities = self.entities
        keys = self.keys

        def hydrate_object(obj, inst=None):
            if isinstance(obj, Structure):
                tag = obj.tag
                fields = obj.fields
                if tag == b"N":
                    return hydrate_node(fields[0], inst=inst,
                                        metadata={"labels": fields[1]}, data=hydrate_object(fields[2]))
                elif tag == b"R":
                    return hydrate_relationship(fields[0], inst=inst,
                                                start=fields[1], end=fields[2],
                                                type=fields[3], data=hydrate_object(fields[4]))
                elif tag == b"P":
                    # Herein lies a dirty hack to retrieve missing relationship
                    # detail for paths received over HTTP.
                    nodes = [hydrate_object(node) for node in fields[0]]
                    u_rels = []
                    typeless_u_rel_ids = []
                    for r in fields[1]:
                        u_rel = self.unbound_relationship(*map(hydrate_object, r))
                        u_rels.append(u_rel)
                        if u_rel.type is None:
                            typeless_u_rel_ids.append(u_rel.id)
                    if typeless_u_rel_ids:
                        r_dict = {r.identity: r for r in RelationshipMatcher(graph).get(typeless_u_rel_ids)}
                        for i, u_rel in enumerate(u_rels):
                            if u_rel.type is None:
                                u_rels[i] = self.unbound_relationship(
                                    u_rel.id,
                                    type(r_dict[u_rel.id]).__name__,
                                    u_rel.properties
                                )
                    sequence = fields[2]
                    last_node = nodes[0]
                    steps = [last_node]
                    for i, rel_index in enumerate(sequence[::2]):
                        next_node = nodes[sequence[2 * i + 1]]
                        if rel_index > 0:
                            u_rel = u_rels[rel_index - 1]
                            rel = hydrate_relationship(u_rel.id,
                                                       start=last_node.identity, end=next_node.identity,
                                                       type=u_rel.type, data=u_rel.properties)
                        else:
                            u_rel = u_rels[-rel_index - 1]
                            rel = hydrate_relationship(u_rel.id,
                                                       start=next_node.identity, end=last_node.identity,
                                                       type=u_rel.type, data=u_rel.properties)
                        steps.append(rel)
                        steps.append(next_node)
                        last_node = next_node
                    return Path(*steps)
                else:
                    try:
                        f = self.hydration_functions[obj.tag]
                    except KeyError:
                        # If we don't recognise the structure type, just return it as-is
                        return obj
                    else:
                        return f(*map(hydrate_object, obj.fields))
            elif isinstance(obj, list):
                return list(map(hydrate_object, obj))
            elif isinstance(obj, dict):
                return {key: hydrate_object(value) for key, value in obj.items()}
            else:
                return obj

        def hydrate_node(identity, inst=None, **rest):
            if inst is None:

                def inst_constructor():
                    new_inst = Node()
                    new_inst.graph = graph
                    new_inst.identity = identity
                    new_inst._stale.update({"labels", "properties"})
                    return new_inst

                inst = graph.node_cache.update(identity, inst_constructor)
            else:
                inst.graph = graph
                inst.identity = identity
                graph.node_cache.update(identity, inst)

            properties = rest.get("data")
            if properties is not None:
                inst._stale.discard("properties")
                inst.clear()
                inst.update(properties)

            labels = rest.get("metadata", {}).get("labels")
            if labels is not None:
                inst._stale.discard("labels")
                inst._remote_labels = frozenset(labels)
                inst.clear_labels()
                inst.update_labels(labels)

            return inst

        def hydrate_relationship(identity, inst=None, **rest):
            start = rest["start"]
            end = rest["end"]

            if inst is None:

                def inst_constructor():
                    properties = rest.get("data")
                    if properties is None:
                        new_inst = Relationship(hydrate_node(start), rest.get("type"),
                                                hydrate_node(end))
                        new_inst._stale.add("properties")
                    else:
                        new_inst = Relationship(hydrate_node(start), rest.get("type"),
                                                hydrate_node(end), **properties)
                    new_inst.graph = graph
                    new_inst.identity = identity
                    return new_inst

                inst = graph.relationship_cache.update(identity, inst_constructor)
            else:
                inst.graph = graph
                inst.identity = identity
                hydrate_node(start, inst=inst.start_node)
                hydrate_node(end, inst=inst.end_node)
                inst._type = rest.get("type")
                if "data" in rest:
                    inst.clear()
                    inst.update(rest["data"])
                else:
                    inst._stale.add("properties")
                graph.relationship_cache.update(identity, inst)
            return inst

        return tuple(hydrate_object(value, entities.get(keys[i])) for i, value in enumerate(values))


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
            return connect(address_, auth=cx_data["auth"], **kwargs)

        address = (cx_data["host"], cx_data["port"])
        self.pool = ConnectionPool(connector, address)

    def close(self):
        self.pool.close()

    def _run_1(self, statement, parameters, hydrator):
        from py2neo.database import Cursor
        cx = self.pool.acquire()
        result = CypherResult(hydrator=hydrator, on_more=cx.fetch, on_done=lambda: self.pool.release(cx))
        result.update_metadata({"connection": self.connection_data})
        cx.run(statement, parameters or {}, on_success=result.update_metadata, on_failure=self._fail)
        cx.pull_all(on_records=result.append_records,
                    on_success=result.update_metadata, on_failure=self._fail, on_summary=result.done)
        cx.send()
        cx.fetch()
        return Cursor(result)

    def _run_in_tx(self, statement, parameters, tx, hydrator):
        from py2neo.database import Cursor
        self._assert_valid_tx(tx)

        def fetch():
            tx.fetch()

        def fail(metadata):
            self.transactions.remove(tx)
            self.pool.release(tx)
            self._fail(metadata)

        result = CypherResult(hydrator=hydrator, on_more=fetch)
        result.update_metadata({"connection": self.connection_data})
        tx.run(statement, parameters or {}, on_success=result.update_metadata, on_failure=fail)
        tx.pull_all(on_records=result.append_records,
                    on_success=result.update_metadata, on_failure=fail, on_summary=result.done)
        tx.send()
        result.keys()   # force receipt of RUN summary, to detect any errors
        return Cursor(result)

    @classmethod
    def _fail(cls, metadata):
        from py2neo.database import GraphError
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

    def _check_parameters(self, data):
        if data is None or data is True or data is False or isinstance(data, float) or isinstance(data, string_types):
            return data
        elif isinstance(data, integer_types):
            if data < INT64_LO or data > INT64_HI:
                raise ValueError("Integers must be within the signed 64-bit range")
            return data
        elif isinstance(data, bytearray):
            return list(data)
        elif isinstance(data, Mapping):
            d = {}
            for key in data:
                if not isinstance(key, string_types):
                    raise TypeError("Dictionary keys must be strings")
                d[key] = self._check_parameters(data[key])
            return d
        elif isinstance(data, Sequence):
            return list(map(self._check_parameters, data))
        else:
            raise TypeError("Neo4j does not support parameters of type %s" % type(data).__name__)

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
        from py2neo.database import Cursor
        r = self._post("/db/data/transaction/commit", statement, self._check_parameters(parameters))
        result = CypherResult.from_json(r.data.decode("utf-8"), hydrator=hydrator)
        result.update_metadata({"connection": self.connection_data})
        return Cursor(result)

    def _run_in_tx(self, statement, parameters, tx, hydrator):
        from py2neo.database import Cursor, GraphError
        r = self._post("/db/data/transaction/%s" % tx, statement, self._check_parameters(parameters))
        assert r.status == 200  # TODO: other codes
        try:
            result = CypherResult.from_json(r.data.decode("utf-8"), hydrator=hydrator)
            result.update_metadata({"connection": self.connection_data})
            return Cursor(result)
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


class CypherResult(object):
    """ Buffer for a result from a Cypher query.
    """

    @classmethod
    def _uri_to_id(cls, uri):
        _, _, identity = uri.rpartition("/")
        return int(identity)

    @classmethod
    def _partially_hydrate_object(cls, data):
        if "self" in data:
            if "type" in data:
                return Structure(b"R",
                                 cls._uri_to_id(data["self"]),
                                 cls._uri_to_id(data["start"]),
                                 cls._uri_to_id(data["end"]),
                                 data["type"],
                                 data["data"])
            else:
                return Structure(b"N",
                                 cls._uri_to_id(data["self"]),
                                 data["metadata"]["labels"],
                                 data["data"])
        elif "nodes" in data and "relationships" in data:
            nodes = [Structure(b"N", i, None, None) for i in map(cls._uri_to_id, data["nodes"])]
            relps = [Structure(b"r", i, None, None) for i in map(cls._uri_to_id, data["relationships"])]
            seq = [i // 2 + 1 for i in range(2 * len(data["relationships"]))]
            for i, direction in enumerate(data["directions"]):
                if direction == "<-":
                    seq[2 * i] *= -1
            return Structure(b"P", nodes, relps, seq)
        else:
            # from warnings import warn
            # warn("Map literals returned over the Neo4j HTTP interface are ambiguous "
            #      "and may be unintentionally hydrated as graph objects")
            return data

    @classmethod
    def from_json(cls, s, hydrator=None):
        from py2neo.database import GraphError
        data = json_loads(s, object_hook=cls._partially_hydrate_object)     # TODO: other partial hydration
        if data.get("errors"):
            raise GraphError.hydrate(data["errors"][0])
        raw_result = data["results"][0]
        result = cls(hydrator=hydrator)
        result.update_metadata({"fields": raw_result["columns"]})
        result.append_records(record["rest"] for record in raw_result["data"])
        metadata = {"stats": raw_result["stats"]}
        if "plan" in raw_result:
            metadata["plan"] = raw_result["plan"]
        result.update_metadata(metadata)
        result.done()
        return result

    def __init__(self, hydrator=None, on_more=None, on_done=None):
        self._hydrator = hydrator
        self._on_more = on_more
        self._on_done = on_done
        self._keys = None
        self._records = deque()
        self._footer = {}
        self._done = False

    def append_records(self, records):
        if self._hydrator:
            self._records.extend(tuple(self._hydrator.hydrate(record)) for record in records)
        else:
            self._records.extend(tuple(record) for record in records)

    def update_metadata(self, metadata):
        self._footer.update(metadata)
        if "fields" in metadata:
            self._keys = metadata["fields"]
            if self._hydrator is not None:
                self._hydrator.keys = self._keys    # TODO: unscrew this

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
        from py2neo.database import CypherSummary
        self.buffer()
        return CypherSummary(**self._footer)

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
        from py2neo.data import Record
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
