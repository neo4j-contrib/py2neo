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


from collections import deque, namedtuple
from logging import getLogger
from socket import socket

from py2neo.meta import bolt_user_agent
from py2neo.net.api import Transaction, TransactionError, Result, \
    Task, ItemizedTask, Failure
from py2neo.net import Connection
from py2neo.net.packstream import MessageReader, MessageWriter
from py2neo.net.wire import ByteReader, ByteWriter


log = getLogger(__name__)


class Bolt(Connection):

    scheme = "bolt"

    __local_port = 0

    @classmethod
    def open(cls, service, user_agent=None):
        # TODO
        s = socket(family=service.address.family)
        log.debug("[#%04X] C: <DIAL> '%s'", 0, service.address)
        s.connect(service.address)
        local_port = s.getsockname()[1]
        log.debug("[#%04X] S: <ACCEPT>", local_port)

        # TODO: secure socket

        byte_reader = ByteReader(s)
        byte_writer = ByteWriter(s)

        def handshake():
            # TODO
            log.debug("[#%04X] C: <BOLT>", local_port)
            byte_writer.write(b"\x60\x60\xB0\x17")
            log.debug("[#%04X] C: <PROTOCOL> 4.0 | 3.0 | 2.0 | 1.0", local_port)
            byte_writer.write(b"\x00\x00\x00\x03"
                              b"\x00\x00\x00\x02"
                              b"\x00\x00\x00\x01"
                              b"\x00\x00\x00\x00")
            byte_writer.send()
            v = bytearray(byte_reader.read(4))
            log.debug("[#%04X] S: <PROTOCOL> %d.%d", local_port, v[-1], v[-2])
            return v[-1], v[-2]

        protocol_version = handshake()
        subclass = cls._get_subclass(cls.scheme, protocol_version)
        if subclass is None:
            raise RuntimeError("Unable to agree supported protocol version")
        bolt = subclass(service, (user_agent or bolt_user_agent()), byte_reader, byte_writer)
        bolt.__local_port = local_port
        bolt.hello()
        return bolt

    def __init__(self, service, user_agent, byte_reader, byte_writer):
        super(Bolt, self).__init__(service, user_agent)
        self.byte_reader = byte_reader
        self.byte_writer = byte_writer

    def close(self):
        if self.closed or self.broken:
            return
        self.goodbye()
        self.byte_writer.close()
        log.debug("[#%04X] C: <HANGUP>", self.local_port)

    @property
    def closed(self):
        return self.byte_writer.closed

    @property
    def broken(self):
        return self.byte_reader.broken or self.byte_writer.broken

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

    def __init__(self, service, user_agent, byte_reader, byte_writer):
        super(Bolt1, self).__init__(service, user_agent, byte_reader, byte_writer)
        self.reader = MessageReader(byte_reader)
        self.writer = MessageWriter(byte_writer)
        self.responses = deque()
        self.transaction = None
        self.metadata = {}

    def bookmark(self):
        return self.metadata.get("bookmark")

    def hello(self):
        self._assert_open()
        extra = {"scheme": "basic",
                 "principal": self.service.user,
                 "credentials": self.service.password}
        clean_extra = dict(extra)
        clean_extra.update({"credentials": "*******"})
        log.debug("[#%04X] C: INIT %r %r", self.local_port, self.user_agent, clean_extra)
        response = self._write_request(0x01, self.user_agent, extra, vital=True)
        self._sync(response)
        self._audit(response)
        self.server_agent = response.metadata.get("server")
        self.connection_id = response.metadata.get("connection_id")

    def reset(self, force=False):
        self._assert_open()
        if force or self.transaction is not None:
            log.debug("[#%04X] C: RESET", self.local_port)
            response = self._write_request(0x0F, vital=True)
            self._sync(response)
            self._audit(response)

    def _begin(self, db=None, readonly=False, bookmarks=None, metadata=None, timeout=None):
        self._assert_open()
        self._assert_no_transaction()
        if metadata:
            raise TransactionError("Transaction metadata not supported until Bolt v3")
        if timeout:
            raise TransactionError("Transaction timeout not supported until Bolt v3")
        self.transaction = Transaction(db, readonly, bookmarks)

    def auto_run(self, cypher, parameters=None,
                 db=None, readonly=False, bookmarks=None, metadata=None, timeout=None):
        self._begin()
        return self._run(cypher, parameters or {}, final=True)

    def begin(self, db=None, readonly=False, bookmarks=None, metadata=None, timeout=None):
        self._begin()
        log.debug("[#%04X] C: RUN 'BEGIN' %r", self.local_port, self.transaction.extra)
        log.debug("[#%04X] C: DISCARD_ALL", self.local_port)
        responses = (self._write_request(0x10, "BEGIN", self.transaction.extra),
                     self._write_request(0x2F))
        if bookmarks:
            self._sync(*responses)
            self._audit(self.transaction)
        return self.transaction

    def commit(self, tx):
        self._assert_open()
        self._assert_open_transaction(tx)
        self.transaction.set_complete()
        log.debug("[#%04X] C: RUN 'COMMIT' {}", self.local_port)
        log.debug("[#%04X] C: DISCARD_ALL", self.local_port)
        self._sync(self._write_request(0x10, "COMMIT", {}),
                   self._write_request(0x2F))
        self._audit(self.transaction)

    def rollback(self, tx):
        self._assert_open()
        self._assert_open_transaction(tx)
        self.transaction.set_complete()
        log.debug("[#%04X] C: RUN 'ROLLBACK' {}", self.local_port)
        log.debug("[#%04X] C: DISCARD_ALL", self.local_port)
        self._sync(self._write_request(0x10, "ROLLBACK", {}),
                   self._write_request(0x2F))
        self._audit(self.transaction)

    def run_in_tx(self, tx, cypher, parameters=None):
        self._assert_open()
        self._assert_open_transaction(tx)
        return self._run(cypher, parameters or {})

    def _run(self, cypher, parameters, extra=None, final=False):
        log.debug("[#%04X] C: RUN %r %r", self.local_port, cypher, parameters)
        response = self._write_request(0x10, cypher, parameters)  # TODO: dehydrate parameters
        result = BoltResult(response)
        self.transaction.append(result, final=final)
        return result

    def pull(self, result, n=-1, capacity=-1):
        self._assert_open()
        self._assert_open_query(result)
        if n != -1:
            raise TransactionError("Flow control is not supported before Bolt 4.0")
        log.debug("[#%04X] C: PULL_ALL", self.local_port)
        response = self._write_request(0x3F, capacity=capacity)
        result.append(response, final=True)
        return response

    def discard(self, result, n=-1):
        self._assert_open()
        self._assert_open_query(result)
        if n != -1:
            raise TransactionError("Flow control is not supported before Bolt 4.0")
        log.debug("[#%04X] C: DISCARD_ALL", self.local_port)
        response = self._write_request(0x2F)
        result.append(response, final=True)
        return response

    def sync(self, result):
        self._send()
        self._wait(result.last())
        self._audit(result)

    def take(self, result):
        if not result.has_records() and not result.done():
            while self.responses[0] is not result.last():
                self._wait(self.responses[0])
            self._wait(result.last())
            self._audit(result)
        record = result.take_record()
        return record

    def _assert_no_transaction(self):
        if self.transaction:
            raise TransactionError("Bolt connection already holds transaction %r", self.transaction)

    def _assert_open_transaction(self, tx):
        if self.transaction is not tx:
            raise TransactionError("Transaction %r is not open on this connection", self.transaction)

    def _assert_open_query(self, query):
        if not self.transaction:
            raise TransactionError("No active transaction")
        if query is not self.transaction.last():
            raise TransactionError("Random query access is not supported before Bolt 4.0")

    def _write_request(self, tag, *fields, **kwargs):
        # capacity denotes the preferred max number of records that a response can hold
        # vital responses close on failure and cannot be ignored
        response = BoltResponse(**kwargs)
        self.writer.write_message(tag, *fields)
        self.responses.append(response)
        return response

    def _send(self):
        sent = self.writer.send()
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
            rs = self.responses[0]
            tag, fields = self.reader.read_message()
            if tag == 0x70:
                log.debug("[#%04X] S: SUCCESS %s", self.local_port, " ".join(map(repr, fields)))
                rs.set_success(**fields[0])
                self.responses.popleft()
                self.metadata.update(fields[0])
            elif tag == 0x71:
                log.debug("[#%04X] S: RECORD %s", self.local_port, " ".join(map(repr, fields)))
                rs.add_record(fields[0])
            elif tag == 0x7F:
                log.debug("[#%04X] S: FAILURE %s", self.local_port, " ".join(map(repr, fields)))
                rs.set_failure(**fields[0])
                self.responses.popleft()
                if rs.vital:
                    self.byte_writer.close()
            elif tag == 0x7E and not rs.vital:
                log.debug("[#%04X] S: IGNORED", self.local_port)
                rs.set_ignored()
                self.responses.popleft()
            else:
                log.debug("[#%04X] S: <ERROR>", self.local_port)
                self.byte_writer.close()
                raise RuntimeError("Unexpected protocol message #%02X", tag)

        while not response.full() and not response.done():
            fetch()
            if not self.transaction and self.pool:
                self.pool.release(self)

    def _sync(self, *responses):
        self._send()
        for response in responses:
            self._wait(response)

    def _audit(self, task):
        """ Checks a specific task (response, query or transaction)
        for failure, raising an exception if one is found.

        :raise BoltFailure:
        """
        try:
            task.audit()
        except Failure:
            self.reset(force=True)
            raise


class Bolt2(Bolt1):

    protocol_version = (2, 0)


class Bolt3(Bolt2):

    protocol_version = (3, 0)

    def hello(self):
        self._assert_open()
        extra = {"user_agent": self.user_agent,
                 "scheme": "basic",
                 "principal": self.service.user,
                 "credentials": self.service.password}
        clean_extra = dict(extra)
        clean_extra.update({"credentials": "*******"})
        log.debug("[#%04X] C: HELLO %r", self.local_port, clean_extra)
        response = self._write_request(0x01, extra, vital=True)
        self._sync(response)
        self._audit(response)
        self.server_agent = response.metadata.get("server")
        self.connection_id = response.metadata.get("connection_id")

    def goodbye(self):
        log.debug("[#%04X] C: GOODBYE", self.local_port)
        self._write_request(0x02)
        self._send()

    def auto_run(self, cypher, parameters=None,
                 db=None, readonly=False, bookmarks=None, metadata=None, timeout=None):
        self._assert_open()
        self._assert_no_transaction()
        self.transaction = Transaction(db, readonly, bookmarks, metadata, timeout)
        return self._run(cypher, parameters or {}, self.transaction.extra, final=True)

    def begin(self, db=None, readonly=False, bookmarks=None, metadata=None, timeout=None):
        self._assert_open()
        self._assert_no_transaction()
        self.transaction = Transaction(db, readonly, bookmarks, metadata, timeout)
        log.debug("[#%04X] C: BEGIN %r", self.local_port, self.transaction.extra)
        response = self._write_request(0x11, self.transaction.extra)
        if bookmarks:
            self._sync(response)
            self._audit(self.transaction)
        return self.transaction

    def commit(self, tx):
        self._assert_open()
        self._assert_open_transaction(tx)
        self.transaction.set_complete()
        log.debug("[#%04X] C: COMMIT", self.local_port)
        self._sync(self._write_request(0x12))
        self._audit(self.transaction)

    def rollback(self, tx):
        self._assert_open()
        self._assert_open_transaction(tx)
        self.transaction.set_complete()
        log.debug("[#%04X] C: ROLLBACK", self.local_port)
        self._sync(self._write_request(0x13))
        self._audit(self.transaction)

    def run(self, tx, cypher, parameters=None):
        self._assert_open()
        self._assert_open_transaction(tx)
        return self._run(cypher, parameters or {}, self.transaction.extra)

    def _run(self, cypher, parameters, extra=None, final=False):
        log.debug("[#%04X] C: RUN %r %r %r", self.local_port, cypher, parameters, extra or {})
        response = self._write_request(0x10, cypher, parameters, extra or {})  # TODO: dehydrate parameters
        result = BoltResult(response)
        self.transaction.append(result, final=final)
        return result


class Bolt4x0(Bolt3):

    protocol_version = (4, 0)


class BoltResult(ItemizedTask, Result):
    """ A query carried out over a Bolt connection.

    Implementation-wise, this form of query is comprised of a number of
    individual message exchanges. Each of these exchanges may succeed
    or fail in its own right, but contribute to the overall success or
    failure of the query.
    """

    def __init__(self, response):
        Result.__init__(self)
        ItemizedTask.__init__(self)
        self.__record_type = None
        self.append(response)

    @property
    def header(self):
        if len(self._items) < 1:
            return None
        return self._items[0]

    @property
    def footer(self):
        if len(self._items) < 2:
            return None
        return self._items[-1]

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


class BoltResponse(Task):

    # 0 = not done
    # 1 = success
    # 2 = failure
    # 3 = ignored

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

    def set_success(self, **metadata):
        self._status = 1
        self._metadata.update(metadata)

    def set_failure(self, **metadata):
        self._status = 2
        # self._failure = Failure(**metadata)
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
