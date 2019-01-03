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
from json import dumps as json_dumps, loads as json_loads


from certifi import where
from neobolt.direct import connect, ConnectionPool
from neobolt.packstream import Structure
from neobolt.routing import RoutingConnectionPool
from urllib3 import HTTPConnectionPool, HTTPSConnectionPool, make_headers

from py2neo.data import Record
from py2neo.database import Cursor, GraphError
from py2neo.internal.collections import round_robin
from py2neo.internal.compat import Sequence, Mapping, urlsplit, string_types, integer_types
from py2neo.internal.addressing import get_connection_data


INT64_LO = -(2 ** 63)
INT64_HI = 2 ** 63 - 1


# TODO: replace with None
class UnknownType(object):

    def __repr__(self):
        return "Unknown"


Unknown = UnknownType()


_unbound_relationship = namedtuple("UnboundRelationship", ["id", "type", "properties"])


def hydrate_node(graph, identity, inst=None, **rest):
    if inst is None:

        def inst_constructor():
            from py2neo.data import Node
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

    properties = rest.get("data", Unknown)
    if properties is not Unknown:
        inst._stale.discard("properties")
        inst.clear()
        inst.update(properties)

    labels = rest.get("metadata", {}).get("labels", Unknown)
    if labels is not Unknown:
        inst._stale.discard("labels")
        inst._remote_labels = frozenset(labels)
        inst.clear_labels()
        inst.update_labels(labels)

    return inst


def hydrate_relationship(graph, identity, inst=None, **rest):
    start = rest["start"]
    end = rest["end"]

    if inst is None:

        def inst_constructor():
            from py2neo.data import Relationship
            properties = rest.get("data", Unknown)
            if properties is Unknown:
                new_inst = Relationship(hydrate_node(graph, start), rest.get("type"),
                                        hydrate_node(graph, end))
                new_inst._stale.add("properties")
            else:
                new_inst = Relationship(hydrate_node(graph, start), rest.get("type"),
                                        hydrate_node(graph, end), **properties)
            new_inst.graph = graph
            new_inst.identity = identity
            return new_inst

        inst = graph.relationship_cache.update(identity, inst_constructor)
    else:
        inst.graph = graph
        inst.identity = identity
        hydrate_node(graph, start, inst=inst.start_node)
        hydrate_node(graph, end, inst=inst.end_node)
        inst._type = rest.get("type")
        if "data" in rest:
            inst.clear()
            inst.update(rest["data"])
        else:
            inst._stale.add("properties")
        graph.relationship_cache.update(identity, inst)
    return inst


def hydrate_path(graph, data):
    # TODO: unused?
    from py2neo.data import Path
    node_ids = data["nodes"]
    relationship_ids = data["relationships"]
    offsets = [(0, 1) if direction == "->" else (1, 0) for direction in data["directions"]]
    nodes = [hydrate_node(graph, identity) for identity in node_ids]
    relationships = [hydrate_relationship(graph, identity,
                                          start=node_ids[i + offsets[i][0]],
                                          end=node_ids[i + offsets[i][1]])
                     for i, identity in enumerate(relationship_ids)]
    inst = Path(*round_robin(nodes, relationships))
    inst.__metadata = data
    return inst


class PackStreamHydrator(object):

    def __init__(self, graph, keys, entities=None):
        # TODO: protocol version
        # super(PackStreamHydrator, self).__init__(2)  # maximum known protocol version
        self.graph = graph
        self.keys = keys
        self.entities = entities or {}

    def hydrate_records(self, keys, record_values):
        for values in record_values:
            yield Record(zip(keys, self.hydrate(values)))

    def hydrate(self, values):
        """ Convert PackStream values into native values.
        """
        from neobolt.packstream import Structure

        graph = self.graph
        entities = self.entities
        keys = self.keys

        def hydrate_(obj, inst=None):
            if isinstance(obj, Structure):
                tag = obj.tag
                fields = obj.fields
                if tag == b"N":
                    return hydrate_node(graph, fields[0], inst=inst,
                                        metadata={"labels": fields[1]}, data=hydrate_(fields[2]))
                elif tag == b"R":
                    return hydrate_relationship(graph, fields[0], inst=inst,
                                                start=fields[1], end=fields[2],
                                                type=fields[3], data=hydrate_(fields[4]))
                elif tag == b"P":
                    # Herein lies a dirty hack to retrieve missing relationship
                    # detail for paths received over HTTP.
                    from py2neo.data import Path
                    nodes = [hydrate_(node) for node in fields[0]]
                    u_rels = []
                    typeless_u_rel_ids = []
                    for r in fields[1]:
                        u_rel = _unbound_relationship(*map(hydrate_, r))
                        u_rels.append(u_rel)
                        if u_rel.type is None or u_rel.type is Unknown:
                            typeless_u_rel_ids.append(u_rel.id)
                    if typeless_u_rel_ids:
                        from py2neo.matching import RelationshipMatcher
                        r_dict = {r.identity: r for r in RelationshipMatcher(graph).get(typeless_u_rel_ids)}
                        for u_rel in u_rels:
                            if u_rel.type is None:
                                u_rel.type = type(r_dict[u_rel.id]).__name__
                    sequence = fields[2]
                    last_node = nodes[0]
                    steps = [last_node]
                    for i, rel_index in enumerate(sequence[::2]):
                        next_node = nodes[sequence[2 * i + 1]]
                        if rel_index > 0:
                            u_rel = u_rels[rel_index - 1]
                            rel = hydrate_relationship(graph, u_rel.id,
                                                       start=last_node.identity, end=next_node.identity,
                                                       type=u_rel.type, data=u_rel.properties)
                        else:
                            u_rel = u_rels[-rel_index - 1]
                            rel = hydrate_relationship(graph, u_rel.id,
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
                        return f(*map(hydrate_, obj.fields))
            elif isinstance(obj, list):
                return list(map(hydrate_, obj))
            elif isinstance(obj, dict):
                return {key: hydrate_(value) for key, value in obj.items()}
            else:
                return obj

        return tuple(hydrate_(value, entities.get(keys[i])) for i, value in enumerate(values))


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
        result = CypherResult(hydrator=hydrator, on_more=cx.fetch, on_done=lambda: self.pool.release(cx))
        result.update_metadata({"connection": self.connection_data})
        cx.run(statement, parameters or {}, on_success=result.update_metadata, on_failure=self._fail)
        cx.pull_all(on_records=result.append_records,
                    on_success=result.update_metadata, on_failure=self._fail, on_summary=result.done)
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

    def _check_parameters(self, data):
        if data is None or data is True or data is False or isinstance(data, float) or isinstance(data, string_types):
            return data
        elif isinstance(data, integer_types):
            if data < INT64_LO or data > INT64_HI:
                raise ValueError("Integers must be within the signed 64-bit range")
            return data
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
        r = self._post("/db/data/transaction/commit", statement, self._check_parameters(parameters))
        result = CypherResult.from_json(r.data.decode("utf-8"), hydrator=hydrator)
        result.update_metadata({"connection": self.connection_data})
        return Cursor(result)

    def _run_in_tx(self, statement, parameters, tx, hydrator):
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
            nodes = [Structure(b"N", i, Unknown, Unknown) for i in map(cls._uri_to_id, data["nodes"])]
            relps = [Structure(b"r", i, Unknown, Unknown) for i in map(cls._uri_to_id, data["relationships"])]
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
        data = json_loads(s, object_hook=cls._partially_hydrate_object)     # TODO: other partial hydration
        if data.get("errors"):
            from py2neo.database import GraphError
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
