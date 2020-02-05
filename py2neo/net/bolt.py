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
from py2neo.net.api import Transaction, TransactionError, Query, \
    Task, ItemizedTask
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
        log.debug("[#%04X] C: <ACCEPT>", local_port)

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
        # TODO: better errors
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
        self.server_agent = response.summary("server")
        self.connection_id = response.summary("connection_id")

    def reset(self, force=False):
        self._assert_open()
        if force or self.transaction is not None:
            log.debug("[#%04X] C: RESET", self.local_port)
            response = self._write_request(0x0F, vital=True)
            self._sync(response)

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
        return self.transaction

    def commit(self, tx):
        self._assert_open()
        self._assert_open_transaction(tx)
        try:
            log.debug("[#%04X] C: RUN 'COMMIT' {}", self.local_port)
            log.debug("[#%04X] C: DISCARD_ALL", self.local_port)
            self._sync(self._write_request(0x10, "COMMIT", {}),
                       self._write_request(0x2F))
        finally:
            self.transaction = None

    def rollback(self, tx):
        self._assert_open()
        self._assert_open_transaction(tx)
        try:
            log.debug("[#%04X] C: RUN 'ROLLBACK' {}", self.local_port)
            log.debug("[#%04X] C: DISCARD_ALL", self.local_port)
            self._sync(self._write_request(0x10, "ROLLBACK", {}),
                       self._write_request(0x2F))
        finally:
            self.transaction = None

    def run(self, tx, cypher, parameters=None):
        self._assert_open()
        self._assert_open_transaction(tx)
        return self._run(cypher, parameters or {})

    def _run(self, cypher, parameters, extra=None, final=False):
        log.debug("[#%04X] C: RUN %r %r", self.local_port, cypher, parameters)
        response = self._write_request(0x10, cypher, parameters)  # TODO: dehydrate parameters
        query = BoltQuery(response)
        self.transaction.append(query, final=final)
        return query

    def pull(self, query, n=-1):
        self._assert_open()
        self._assert_open_query(query)
        if n != -1:
            raise TransactionError("Flow control is not supported before Bolt 4.0")
        log.debug("[#%04X] C: PULL_ALL", self.local_port)
        response = self._write_request(0x3F)
        query.append(response, final=True)
        return response

    def discard(self, query, n=-1):
        self._assert_open()
        self._assert_open_query(query)
        if n != -1:
            raise TransactionError("Flow control is not supported before Bolt 4.0")
        log.debug("[#%04X] C: DISCARD_ALL", self.local_port)
        response = self._write_request(0x2F)
        query.append(response, final=True)
        return response

    def send(self, query):
        self._assert_open()
        self._assert_open_query(query)
        self._send()

    def wait(self, query):
        self._assert_open()
        self._assert_open_query(query)
        self._wait(query.latest())
        if self.transaction.done():
            self.transaction = None
            if query.failure():
                self.reset()
                raise query.failure()

    def _assert_no_transaction(self):
        if self.transaction is not None:
            raise TransactionError("Bolt connection already holds transaction %r", self.transaction)

    def _assert_open_transaction(self, tx):
        if self.transaction is not tx:
            raise TransactionError("Transaction %r is not open on this connection", self.transaction)

    def _assert_open_query(self, query):
        if not self.transaction:
            raise TransactionError("No active transaction")
        if query is not self.transaction.latest():
            raise TransactionError("Random query access is not supported before Bolt 4.0")

    def _write_request(self, tag, *fields, vital=False):
        # vital responses close on failure and cannot be ignored
        response = BoltResponse(vital=vital)
        self.writer.write_message(tag, *fields)
        self.responses.append(response)
        return response

    def _send(self):
        log.debug("[#%04X] C: <SEND>", self.local_port)
        self.writer.send()

    def _wait(self, response):
        """ Read all incoming responses up to and including a
        particular response.
        """
        while not response.done():
            top_response = self.responses[0]
            tag, fields = self.reader.read_message()
            if tag == 0x70:
                log.debug("[#%04X] S: SUCCESS %s", self.local_port, " ".join(map(repr, fields)))
                top_response.set_success(**fields[0])
                self.responses.popleft()
                self.metadata.update(fields[0])
            elif tag == 0x71:
                log.debug("[#%04X] S: RECORD %s", self.local_port, " ".join(map(repr, fields)))
                top_response.add_record(fields[0])
            elif tag == 0x7F:
                log.debug("[#%04X] S: FAILURE %s", self.local_port, " ".join(map(repr, fields)))
                top_response.set_failure(**fields[0])
                self.responses.popleft()
                if top_response.vital:
                    self.byte_writer.close()
            elif tag == 0x7E and not top_response.vital:
                log.debug("[#%04X] S: IGNORED", self.local_port)
                top_response.set_ignored()
                self.responses.popleft()
            else:
                log.debug("[#%04X] S: <ERROR>", self.local_port)
                self.byte_writer.close()
                raise RuntimeError("Unexpected protocol message #%02X", tag)

    def _sync(self, *responses):
        self._send()
        self._wait(responses[-1])
        for response in responses:
            if response.failure():
                raise response.failure()
        # TODO: handle IGNORED


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
        self.server_agent = response.summary("server")
        self.connection_id = response.summary("connection_id")

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
        return self.transaction

    def commit(self, tx):
        self._assert_open()
        self._assert_open_transaction(tx)
        try:
            log.debug("[#%04X] C: COMMIT", self.local_port)
            self._sync(self._write_request(0x12))
        finally:
            self.transaction = None

    def rollback(self, tx):
        self._assert_open()
        self._assert_open_transaction(tx)
        try:
            log.debug("[#%04X] C: ROLLBACK", self.local_port)
            self._sync(self._write_request(0x13))
        finally:
            self.transaction = None

    def run(self, tx, cypher, parameters=None):
        self._assert_open()
        self._assert_open_transaction(tx)
        return self._run(cypher, parameters or {}, self.transaction.extra)

    def _run(self, cypher, parameters, extra=None, final=False):
        log.debug("[#%04X] C: RUN %r %r %r", self.local_port, cypher, parameters, extra or {})
        response = self._write_request(0x10, cypher, parameters, extra or {})  # TODO: dehydrate parameters
        query = BoltQuery(response)
        self.transaction.append(query, final=final)
        return query


class Bolt4x0(Bolt3):

    protocol_version = (4, 0)


class BoltQuery(ItemizedTask, Query):
    """ A query carried out over a Bolt connection.

    Implementation-wise, this form of query is comprised of a number of
    individual message exchanges. Each of these exchanges may succeed
    or fail in its own right, but contribute to the overall success or
    failure of the query.
    """

    def __init__(self, response):
        Query.__init__(self)
        ItemizedTask.__init__(self)
        self.append(response)

    def record_type(self):
        try:
            header = self._items[0]
        except IndexError:
            return super(BoltQuery, self).record_type()
        else:
            return namedtuple("Record", header.summary("fields"))

    def records(self):
        t = self.record_type()
        for response in self._items:
            for record in response.records():
                yield t(record)


class BoltResponse(Task):

    # 0 = not done
    # 1 = success
    # 2 = failure
    # 3 = ignored

    def __init__(self, vital=False):
        super(BoltResponse, self).__init__()
        self.vital = vital
        self._records = deque()
        self._status = 0
        self._metadata = {}
        self._failure = None

    def records(self):
        return iter(self._records)

    def add_record(self, values):
        self._records.append(values)

    def set_success(self, **metadata):
        self._status = 1
        self._metadata.update(metadata)

    def set_failure(self, **metadata):
        self._status = 2
        self._failure = BoltFailure(**metadata)

    def set_ignored(self):
        self._status = 3

    def done(self):
        return self._status != 0

    def failure(self):
        return self._failure

    def ignored(self):
        return self._status == 2

    def summary(self, key, default=None):
        return self._metadata.get(key, default)


class BoltFailure(Exception):

    def __init__(self, message, code):
        super(BoltFailure, self).__init__(message)
        self.code = code

    def __str__(self):
        return "[%s] %s" % (self.code, super(BoltFailure, self).__str__())
