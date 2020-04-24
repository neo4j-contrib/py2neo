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


from collections import deque
from itertools import islice
from logging import getLogger
from socket import socket, SOL_SOCKET, SO_KEEPALIVE

from py2neo.meta import bolt_user_agent
from py2neo.connect import Connection, Transaction, TransactionError, Result, Failure, Bookmark
from py2neo.connect.packstream import MessageReader, MessageWriter, PackStreamHydrant
from py2neo.connect.wire import Wire
from py2neo.internal.versioning import Version


log = getLogger(__name__)


class Bolt(Connection):

    protocol_version = ()

    __local_port = 0

    @classmethod
    def _walk_subclasses(cls):
        for subclass in cls.__subclasses__():
            assert issubclass(subclass, cls)  # for the benefit of the IDE
            yield subclass
            for k in subclass._walk_subclasses():
                yield k

    @classmethod
    def _get_subclass(cls, protocol_version):
        for subclass in cls._walk_subclasses():
            if subclass.protocol_version == protocol_version:
                return subclass
        raise RuntimeError("Unsupported protocol version %r" % protocol_version)

    @classmethod
    def default_hydrant(cls, profile, graph):
        return PackStreamHydrant(graph)

    @classmethod
    def protocol_catalogue(cls):
        return [bolt.protocol_version for bolt in Bolt._walk_subclasses()]

    @classmethod
    def open(cls, profile, user_agent=None):
        wire = cls._connect(profile)
        protocol_version = cls._handshake(wire)
        subclass = cls._get_subclass(protocol_version)
        if subclass is None:
            raise RuntimeError("Unable to agree supported protocol version")
        bolt = subclass(wire, profile, (user_agent or bolt_user_agent()))
        bolt._hello()
        bolt.__local_port = wire.local_address.port_number
        return bolt

    @classmethod
    def _connect(cls, profile):
        s = socket(family=profile.address.family)
        s.setsockopt(SOL_SOCKET, SO_KEEPALIVE, 1)
        log.debug("[#%04X] C: <DIAL> '%s'", 0, profile.address)
        s.connect(profile.address)
        wire = Wire(s)
        local_port = wire.local_address.port_number
        log.debug("[#%04X] S: <ACCEPT>", local_port)
        if profile.secure:
            log.debug("[#%04X] C: <SECURE>", local_port)
            wire.secure(verify=profile.verify, hostname=profile.host)
        return wire

    @classmethod
    def _handshake(cls, wire):
        local_port = wire.local_address.port_number
        log.debug("[#%04X] C: <BOLT>", local_port)
        wire.write(b"\x60\x60\xB0\x17")
        versions = list(reversed(cls.protocol_catalogue()))[:4]
        log.debug("[#%04X] C: <PROTOCOL> %s",
                  local_port, " | ".join("%d.%d" % v for v in versions))
        wire.write(b"".join(bytes(bytearray([0, 0, minor, major]))
                            for major, minor in versions).ljust(16, b"\x00"))
        wire.send()
        v = bytearray(wire.read(4))
        log.debug("[#%04X] S: <PROTOCOL> %d.%d", local_port, v[-1], v[-2])
        return v[-1], v[-2]

    def __init__(self, wire, profile, user_agent):
        super(Bolt, self).__init__(profile, user_agent)
        self._wire = wire

    def close(self):
        if self.closed or self.broken:
            return
        self._goodbye()
        self._wire.close()
        log.debug("[#%04X] C: <HANGUP>", self.local_port)

    @property
    def closed(self):
        return self._wire.closed

    @property
    def broken(self):
        return self._wire.broken

    @property
    def local_port(self):
        return self.__local_port

    def _assert_open(self):
        # TODO: better errors and hooks back into the pool, for
        #  deactivating and/or removing addresses from the routing table
        if self.closed:
            raise RuntimeError("Connection has been closed")
        if self.broken:
            raise RuntimeError("Connection is broken")


class Bolt1(Bolt):

    protocol_version = (1, 0)

    def __init__(self, wire, profile, user_agent):
        super(Bolt1, self).__init__(wire, profile, user_agent)
        self._reader = MessageReader(wire)
        self._writer = MessageWriter(wire)
        self._responses = deque()
        self._transaction = None
        self._metadata = {}

    def bookmark(self):
        return self._metadata.get("bookmark")

    def _hello(self):
        self._assert_open()
        extra = {"scheme": "basic",
                 "principal": self.profile.user,
                 "credentials": self.profile.password}
        clean_extra = dict(extra)
        clean_extra.update({"credentials": "*******"})
        log.debug("[#%04X] C: INIT %r %r", self.local_port, self.user_agent, clean_extra)
        response = self._write_request(0x01, self.user_agent, extra, vital=True)
        self._sync(response)
        self._audit(response)
        self.connection_id = response.metadata.get("connection_id")
        self.server_agent = response.metadata.get("server")
        if self.server_agent.startswith("Neo4j/"):
            self.neo4j_version = Version.parse(self.server_agent[6:])
        else:
            raise RuntimeError("Unexpected server agent {!r}".format(self.server_agent))

    def reset(self, force=False):
        self._assert_open()
        if force or self._transaction is not None:
            log.debug("[#%04X] C: RESET", self.local_port)
            response = self._write_request(0x0F, vital=True)
            self._sync(response)
            self._audit(response)

    def _begin(self, graph_name=None, readonly=False, after=None, metadata=None, timeout=None):
        self._assert_open()
        self._assert_no_transaction()
        if graph_name and not self.supports_multi():
            raise TypeError("Neo4j {}.{} does not support "
                            "named graphs".format(*self.neo4j_version.major_minor))
        if metadata:
            raise TypeError("Transaction metadata not supported until Bolt v3")
        if timeout:
            raise TypeError("Transaction timeout not supported until Bolt v3")
        self._transaction = BoltTransaction(graph_name, self.protocol_version,
                                            readonly, after)

    def auto_run(self, graph_name, cypher, parameters=None,
                 readonly=False, after=None, metadata=None, timeout=None):
        self._begin(graph_name, readonly, after, metadata, timeout)
        return self._run(graph_name, cypher, parameters or {}, final=True)

    def begin(self, graph_name, readonly=False, after=None, metadata=None, timeout=None):
        self._begin(graph_name, readonly, after, metadata, timeout)
        log.debug("[#%04X] C: RUN 'BEGIN' %r", self.local_port, self._transaction.extra)
        log.debug("[#%04X] C: DISCARD_ALL", self.local_port)
        responses = (self._write_request(0x10, "BEGIN", self._transaction.extra),
                     self._write_request(0x2F))
        if after:
            self._sync(*responses)
            self._audit(self._transaction)
        self._bind()
        return self._transaction

    def commit(self, tx):
        self._assert_open()
        self._assert_open_transaction(tx)
        self._transaction.set_complete()
        log.debug("[#%04X] C: RUN 'COMMIT' {}", self.local_port)
        log.debug("[#%04X] C: DISCARD_ALL", self.local_port)
        self._sync(self._write_request(0x10, "COMMIT", {}),
                   self._write_request(0x2F))
        self._audit(self._transaction)
        self._unbind()
        return Bookmark()

    def rollback(self, tx):
        self._assert_open()
        self._assert_open_transaction(tx)
        self._transaction.set_complete()
        log.debug("[#%04X] C: RUN 'ROLLBACK' {}", self.local_port)
        log.debug("[#%04X] C: DISCARD_ALL", self.local_port)
        self._sync(self._write_request(0x10, "ROLLBACK", {}),
                   self._write_request(0x2F))
        self._audit(self._transaction)
        self._unbind()
        return Bookmark()

    def run_in_tx(self, tx, cypher, parameters=None):
        self._assert_open()
        self._assert_open_transaction(tx)
        return self._run(tx.graph_name, cypher, parameters or {})

    def _run(self, graph_name, cypher, parameters, extra=None, final=False):
        log.debug("[#%04X] C: RUN %r %r", self.local_port, cypher, parameters)
        response = self._write_request(0x10, cypher, parameters)
        result = BoltResult(graph_name, self, response)
        self._transaction.append(result, final=final)
        return result

    def pull(self, result, n=-1, capacity=-1):
        self._assert_open()
        self._assert_is_last_result(result)
        if n != -1:
            raise TypeError("Flow control is not supported before Bolt 4.0")
        log.debug("[#%04X] C: PULL_ALL", self.local_port)
        response = self._write_request(0x3F, capacity=capacity)
        result.append(response, final=True)
        return response

    def discard(self, result, n=-1):
        self._assert_open()
        self._assert_is_last_result(result)
        if n != -1:
            raise TypeError("Flow control is not supported before Bolt 4.0")
        log.debug("[#%04X] C: DISCARD_ALL", self.local_port)
        response = self._write_request(0x2F)
        result.append(response, final=True)
        return response

    def sync(self, result):
        self._send()
        self._wait(result.last())
        self._audit(result)

    def fetch(self, result):
        if not result.has_records() and not result.done():
            while self._responses[0] is not result.last():
                self._wait(self._responses[0])
            self._wait(result.last())
            self._audit(result)
        record = result.take_record()
        return record

    def _assert_no_transaction(self):
        if self._transaction:
            raise TransactionError("Bolt connection already holds transaction %r", self._transaction)

    def _assert_open_transaction(self, tx):
        if self._transaction is not tx:
            raise TypeError("Transaction %r is not open on this connection", self._transaction)

    def _assert_is_last_result(self, result):
        if not self._transaction:
            raise TransactionError("No active transaction")
        if result is not self._transaction.last():
            raise TypeError("Random query access is not supported before Bolt 4.0")

    def _write_request(self, tag, *fields, **kwargs):
        # capacity denotes the preferred max number of records that a response can hold
        # vital responses close on failure and cannot be ignored
        response = BoltResponse(**kwargs)
        self._writer.write_message(tag, *fields)
        self._responses.append(response)
        return response

    def _send(self):
        sent = self._writer.send()
        if sent:
            log.debug("[#%04X] C: <SENT %r bytes>", self.local_port, sent)

    def _wait(self, response):
        """ Read all incoming responses up to and including a
        particular response.

        This method calls fetch, but does not raise an exception on
        FAILURE.
        """

        def fetch():
            """ Fetch and process the next incoming message.

            This method does not raise an exception on receipt of a
            FAILURE message. Instead, it sets the response (and
            consequently the parent query and transaction) to a failed
            state. It is the responsibility of the caller to convert this
            failed state into an exception.
            """
            rs = self._responses[0]
            tag, fields = self._reader.read_message()
            if tag == 0x70:
                log.debug("[#%04X] S: SUCCESS %s", self.local_port, " ".join(map(repr, fields)))
                rs.set_success(**fields[0])
                self._responses.popleft()
                self._metadata.update(fields[0])
            elif tag == 0x71:
                log.debug("[#%04X] S: RECORD %s", self.local_port, " ".join(map(repr, fields)))
                rs.add_record(fields[0])
            elif tag == 0x7F:
                log.debug("[#%04X] S: FAILURE %s", self.local_port, " ".join(map(repr, fields)))
                rs.set_failure(**fields[0])
                self._responses.popleft()
                if rs.vital:
                    self._wire.close()
            elif tag == 0x7E and not rs.vital:
                log.debug("[#%04X] S: IGNORED", self.local_port)
                rs.set_ignored()
                self._responses.popleft()
            else:
                log.debug("[#%04X] S: <ERROR>", self.local_port)
                self._wire.close()
                raise RuntimeError("Unexpected protocol message #%02X", tag)

        while not response.full() and not response.done():
            fetch()
            if not self._transaction:
                self.release()

    def _sync(self, *responses):
        self._send()
        for response in responses:
            self._wait(response)

    def _audit(self, task):
        """ Checks a specific task (response, result or transaction)
        for failure, raising an exception if one is found.

        :raise BoltFailure:
        """
        try:
            task.audit()
        except Failure:
            self.reset(force=True)
            raise

    def _bind(self):
        if self.pool:
            self.pool.bind(self._transaction, self)

    def _unbind(self):
        if self.pool:
            self.pool.unbind(self._transaction)


class Bolt2(Bolt1):

    protocol_version = (2, 0)


class Bolt3(Bolt2):

    protocol_version = (3, 0)

    def _hello(self):
        self._assert_open()
        extra = {"user_agent": self.user_agent,
                 "scheme": "basic",
                 "principal": self.profile.user,
                 "credentials": self.profile.password}
        clean_extra = dict(extra)
        clean_extra.update({"credentials": "*******"})
        log.debug("[#%04X] C: HELLO %r", self.local_port, clean_extra)
        response = self._write_request(0x01, extra, vital=True)
        self._sync(response)
        self._audit(response)
        self.server_agent = response.metadata.get("server")
        self.connection_id = response.metadata.get("connection_id")

    def _goodbye(self):
        log.debug("[#%04X] C: GOODBYE", self.local_port)
        self._write_request(0x02)
        self._send()

    def auto_run(self, graph_name, cypher, parameters=None,
                 readonly=False, after=None, metadata=None, timeout=None):
        self._assert_open()
        self._assert_no_transaction()
        self._transaction = BoltTransaction(graph_name, self.protocol_version,
                                            readonly, after, metadata, timeout)
        return self._run(graph_name, cypher, parameters or {}, self._transaction.extra, final=True)

    def begin(self, graph_name, readonly=False, after=None, metadata=None, timeout=None):
        self._assert_open()
        self._assert_no_transaction()
        self._transaction = BoltTransaction(graph_name, self.protocol_version,
                                            readonly, after, metadata, timeout)
        log.debug("[#%04X] C: BEGIN %r", self.local_port, self._transaction.extra)
        response = self._write_request(0x11, self._transaction.extra)
        if after:
            self._sync(response)
            self._audit(self._transaction)
        self._bind()
        return self._transaction

    def commit(self, tx):
        self._assert_open()
        self._assert_open_transaction(tx)
        self._transaction.set_complete()
        log.debug("[#%04X] C: COMMIT", self.local_port)
        response = self._write_request(0x12)
        self._sync(response)
        self._audit(self._transaction)
        self._unbind()
        return Bookmark(response.metadata.get("bookmark"))

    def rollback(self, tx):
        self._assert_open()
        self._assert_open_transaction(tx)
        self._transaction.set_complete()
        log.debug("[#%04X] C: ROLLBACK", self.local_port)
        response = self._write_request(0x13)
        self._sync(response)
        self._audit(self._transaction)
        self._unbind()
        return Bookmark(response.metadata.get("bookmark"))

    def run(self, tx, cypher, parameters=None):
        self._assert_open()
        self._assert_open_transaction(tx)
        return self._run(tx.graph_name, cypher, parameters or {}, self._transaction.extra)

    def _run(self, graph_name, cypher, parameters, extra=None, final=False):
        log.debug("[#%04X] C: RUN %r %r %r", self.local_port, cypher, parameters, extra or {})
        response = self._write_request(0x10, cypher, parameters, extra or {})
        result = BoltResult(graph_name, self, response)
        self._transaction.append(result, final=final)
        return result


class Bolt4x0(Bolt3):

    protocol_version = (4, 0)

    def _assert_is_last_result(self, result):
        if not self._transaction:
            raise TransactionError("No active transaction")
        if result is not self._transaction.last():
            raise NotImplementedError("Random query access is not yet supported")

    def pull(self, result, n=-1, capacity=-1):
        self._assert_open()
        self._assert_is_last_result(result)  # TODO: random query access (qid)
        args = {"n": n}
        log.debug("[#%04X] C: PULL %r", self.local_port, args)
        response = self._write_request(0x3F, args, capacity=capacity)
        result.append(response, final=(n == -1))
        return response

    def discard(self, result, n=-1):
        self._assert_open()
        self._assert_is_last_result(result)  # TODO: random query access (qid)
        args = {"n": n}
        log.debug("[#%04X] C: DISCARD %r", self.local_port, args)
        response = self._write_request(0x2F, args)
        result.append(response, final=(n == -1))
        return response


class Task(object):

    def done(self):
        raise NotImplementedError

    def failed(self):
        raise NotImplementedError

    def audit(self):
        raise NotImplementedError


class ItemizedTask(Task):
    """ This class represents a form of dynamic checklist. Items may
    be added, up to a "final" item which marks the list as complete.
    Each item may then be marked as done.
    """

    def __init__(self):
        self._items = deque()
        self._complete = False

    def __bool__(self):
        return not self.done() and not self.failed()

    __nonzero__ = __bool__

    def items(self):
        return iter(self._items)

    def append(self, item, final=False):
        self._items.append(item)
        if final:
            self.set_complete()

    def set_complete(self):
        self._complete = True

    def complete(self):
        """ Flag to indicate whether all items have been appended to
        this task, whether or not they are done.
        """
        return self._complete

    def first(self):
        try:
            return self._items[0]
        except IndexError:
            return None

    def last(self):
        try:
            return self._items[-1]
        except IndexError:
            return None

    def done(self):
        """ Flag to indicate whether the list of items is complete and
        all items are done.
        """
        if self.complete():
            last = self.last()
            return (last and last.done()) or not last
        else:
            return False

    def failed(self):
        return any(item.failed() for item in self._items)

    def audit(self):
        for item in self._items:
            item.audit()


class BoltTransaction(ItemizedTask, Transaction):

    def __init__(self, graph_name, protocol_version, readonly=False,
                 after=None, metadata=None, timeout=None):
        if graph_name and protocol_version < (4, 0):
            raise TypeError("Database selection is not supported "
                            "prior to Neo4j 4.0")
        ItemizedTask.__init__(self)
        Transaction.__init__(self, graph_name, readonly=readonly)
        self.after = after
        self.metadata = metadata
        self.timeout = timeout

    @property
    def extra(self):
        extra = {}
        if self.graph_name:
            extra["db"] = self.graph_name
        if self.readonly:
            extra["mode"] = "r"
        if self.after:
            extra["bookmarks"] = list(Bookmark(self.after))
        if self.metadata:
            extra["metadata"] = self.metadata
        if self.timeout:
            extra["timeout"] = self.timeout
        return extra


# TODO: use 'has_more' metadata from PULL success response
class BoltResult(ItemizedTask, Result):
    """ A query carried out over a Bolt connection.

    Implementation-wise, this form of query is comprised of a number of
    individual message exchanges. Each of these exchanges may succeed
    or fail in its own right, but contribute to the overall success or
    failure of the query.
    """

    def __init__(self, graph_name, cx, response):
        ItemizedTask.__init__(self)
        Result.__init__(self, graph_name)
        self.__record_type = None
        self.__cx = cx
        self.append(response)

    @property
    def graph_name(self):
        return self._items[-1].metadata.get("db", super(BoltResult, self).graph_name)

    @property
    def query_id(self):
        return self.header().metadata.get("qid")

    @property
    def protocol_version(self):
        return self.__cx.protocol_version

    def buffer(self):
        if not self.done():
            self.__cx.sync(self)

    def header(self):
        self.__cx.sync(self)
        return self._items[0]

    def fields(self):
        return self.header().metadata.get("fields")

    def summary(self):
        return dict(self._items[-1].metadata,
                    connection=self.__cx.profile.to_dict())

    def fetch(self):
        return self.__cx.fetch(self)

    def has_records(self):
        return any(response.has_records()
                   for response in self._items)

    def take_record(self):
        for response in self._items:
            record = response.take_record()
            if record is None:
                continue
            return record
        return None

    def peek_records(self, limit):
        records = []
        for response in self._items:
            records.extend(response.peek_records(limit - len(records)))
            if len(records) == limit:
                break
        return records


class BoltResponse(Task):

    # status values:
    #   0 = not done
    #   1 = success
    #   2 = failure
    #   3 = ignored

    def __init__(self, capacity=-1, vital=False):
        super(BoltResponse, self).__init__()
        self.capacity = capacity
        self.vital = vital
        self._records = deque()
        self._status = 0
        self._metadata = {}
        self._failure = None

    def __repr__(self):
        if self._status == 1:
            return "<BoltResponse SUCCESS %r>" % self._metadata
        elif self._status == 2:
            return "<BoltResponse FAILURE %r>" % self._metadata
        elif self._status == 3:
            return "<BoltResponse IGNORED>"
        else:
            return "<BoltResponse ?>"

    def add_record(self, values):
        self._records.append(values)

    def has_records(self):
        return bool(self._records)

    def take_record(self):
        try:
            return self._records.popleft()
        except IndexError:
            return None

    def peek_records(self, n):
        return islice(self._records, 0, n)

    def set_success(self, **metadata):
        self._status = 1
        self._metadata.update(metadata)

    def set_failure(self, **metadata):
        self._status = 2
        # TODO: tidy up where these errors live
        from py2neo.database import GraphError
        self._failure = GraphError.hydrate(metadata)

    def set_ignored(self):
        self._status = 3

    def full(self):
        if self.capacity >= 0:
            return len(self._records) >= self.capacity
        else:
            return False

    def done(self):
        return self._status != 0

    def failed(self):
        return self._status >= 2

    def audit(self):
        if self._failure:
            self.set_ignored()
            raise self._failure

    @property
    def metadata(self):
        return self._metadata
