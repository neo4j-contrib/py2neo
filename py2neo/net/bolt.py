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

from py2neo.net.api import get_connection_data, Connection, Transaction, TransactionError, Query, \
    Task, ItemizedTask
from py2neo.net.syntax import MessageReader, MessageWriter
from py2neo.net.wire import ByteReader, ByteWriter


log = getLogger(__name__)


class Bolt(Connection):

    scheme = "bolt"

    protocol_version = (0, 0)

    connection_id = None

    __subclasses = None

    @classmethod
    def walk_subclasses(cls):
        for subclass in cls.__subclasses__():
            assert issubclass(subclass, cls)  # for the benefit of the IDE
            yield subclass
            for k in subclass.walk_subclasses():
                yield k

    @classmethod
    def get_subclass(cls, protocol_version):
        if cls.__subclasses is None:
            cls.__subclasses = {}
            for subclass in cls.walk_subclasses():
                cls.__subclasses[subclass.protocol_version] = subclass
        return cls.__subclasses.get(protocol_version)

    @classmethod
    def open(cls, uri=None, **settings):
        # TODO
        cx_data = get_connection_data(uri, **settings)
        s = cls.connect(("localhost", 7687))
        s = cls.secure(s)
        byte_reader = ByteReader(s)
        byte_writer = ByteWriter(s)
        protocol_version = cls.handshake(byte_reader, byte_writer)
        subclass = cls.get_subclass(protocol_version)
        if subclass is None:
            raise RuntimeError("Unable to agree supported protocol version")
        bolt = subclass(cx_data, byte_reader, byte_writer)
        bolt.hello(auth=("neo4j", "password"))
        return bolt

    @classmethod
    def connect(cls, address):
        s = socket()
        s.connect(address)
        return s

    @classmethod
    def secure(cls, s):
        return s

    @classmethod
    def handshake(cls, byte_reader, byte_writer):
        log.debug("C: <BOLT>")
        byte_writer.write(b"\x60\x60\xB0\x17")
        log.debug("C: <PROTOCOL> 4.0 | 3.0 | 2.0 | 1.0")
        byte_writer.write(b"\x00\x00\x00\x03"
                          b"\x00\x00\x00\x02"
                          b"\x00\x00\x00\x01"
                          b"\x00\x00\x00\x00")
        byte_writer.send()
        v = bytearray(byte_reader.read(4))
        log.debug("S: <PROTOCOL> %d.%d", v[-1], v[-2])
        return v[-1], v[-2]

    def __init__(self, cx_data, byte_reader, byte_writer):
        super(Bolt, self).__init__(cx_data)
        self.byte_reader = byte_reader
        self.byte_writer = byte_writer

    def close(self):
        self.goodbye()
        self.byte_writer.close()
        log.debug("C: <CLOSE>")


class Bolt1(Bolt):

    protocol_version = (1, 0)

    def __init__(self, cx_data, byte_reader, byte_writer):
        super(Bolt1, self).__init__(cx_data, byte_reader, byte_writer)
        self.reader = MessageReader(byte_reader)
        self.writer = MessageWriter(byte_writer)
        self.responses = deque()
        self.transaction = None
        self.metadata = {}

    def bookmark(self):
        return self.metadata.get("bookmark")

    def hello(self, auth):
        user, password = auth
        extra = {"scheme": "basic",
                 "principal": user,
                 "credentials": password}
        clean_extra = dict(extra)
        clean_extra.update({"credentials": "*******"})
        log.debug("C: INIT %r %r", self.user_agent, clean_extra)
        response = self._write_request(0x01, self.user_agent, extra, vital=True)
        self._sync(response)
        self.server_agent = response.summary("server")
        self.connection_id = response.summary("connection_id")

    def reset(self):
        log.debug("C: RESET")
        response = self._write_request(0x0F, vital=True)
        self._sync(response)

    def _begin(self, db=None, readonly=False, bookmarks=None, metadata=None, timeout=None):
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
        log.debug("C: RUN 'BEGIN' %r", self.transaction.extra)
        log.debug("C: DISCARD_ALL")
        responses = (self._write_request(0x10, "BEGIN", self.transaction.extra),
                     self._write_request(0x2F))
        if bookmarks:
            self._sync(*responses)
        return self.transaction

    def commit(self, tx):
        self._assert_open_transaction(tx)
        try:
            log.debug("C: RUN 'COMMIT' {}")
            log.debug("C: DISCARD_ALL")
            self._sync(self._write_request(0x10, "COMMIT", {}),
                       self._write_request(0x2F))
        finally:
            self.transaction = None

    def rollback(self, tx):
        self._assert_open_transaction(tx)
        try:
            log.debug("C: RUN 'ROLLBACK' {}")
            log.debug("C: DISCARD_ALL")
            self._sync(self._write_request(0x10, "ROLLBACK", {}),
                       self._write_request(0x2F))
        finally:
            self.transaction = None

    def run(self, tx, cypher, parameters=None):
        self._assert_open_transaction(tx)
        return self._run(cypher, parameters or {})

    def _run(self, cypher, parameters, extra=None, final=False):
        log.debug("C: RUN %r %r", cypher, parameters)
        response = self._write_request(0x10, cypher, parameters)  # TODO: dehydrate parameters
        query = BoltQuery(response)
        self.transaction.append(query, final=final)
        return query

    def pull(self, query, n=-1):
        self._assert_open_query(query)
        if n != -1:
            raise TransactionError("Flow control is not supported before Bolt 4.0")
        log.debug("C: PULL_ALL")
        response = self._write_request(0x3F)
        query.append(response, final=True)
        return response

    def discard(self, query, n=-1):
        self._assert_open_query(query)
        if n != -1:
            raise TransactionError("Flow control is not supported before Bolt 4.0")
        log.debug("C: DISCARD_ALL")
        response = self._write_request(0x2F)
        query.append(response, final=True)
        return response

    def send(self, query):
        self._assert_open_query(query)
        self._send()

    def wait(self, query):
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
        # TODO: logging
        self.writer.write_message(tag, *fields)
        self.responses.append(response)
        return response

    def _send(self):
        log.debug("C: <SEND>")
        self.writer.send()

    def _wait(self, response):
        """ Read all incoming responses up to and including a
        particular response.
        """
        while not response.done():
            top_response = self.responses[0]
            tag, fields = self.reader.read_message()
            if tag == 0x70:
                log.debug("S: SUCCESS %s", " ".join(map(repr, fields)))
                top_response.set_success(**fields[0])
                self.responses.popleft()
                self.metadata.update(fields[0])
            elif tag == 0x71:
                log.debug("S: RECORD %s", " ".join(map(repr, fields)))
                top_response.add_record(fields[0])
            elif tag == 0x7F:
                log.debug("S: FAILURE %s", " ".join(map(repr, fields)))
                top_response.set_failure(**fields[0])
                self.responses.popleft()
                if top_response.vital:
                    self.byte_writer.close()
            elif tag == 0x7E and not top_response.vital:
                log.debug("S: IGNORED")
                top_response.set_ignored()
                self.responses.popleft()
            else:
                log.debug("S: <ERROR>")
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

    def hello(self, auth):
        user, password = auth
        extra = {"user_agent": self.user_agent,
                 "scheme": "basic",
                 "principal": user,
                 "credentials": password}
        clean_extra = dict(extra)
        clean_extra.update({"credentials": "*******"})
        log.debug("C: HELLO %r", clean_extra)
        response = self._write_request(0x01, extra, vital=True)
        self._sync(response)
        self.server_agent = response.summary("server")
        self.connection_id = response.summary("connection_id")

    def goodbye(self):
        log.debug("C: GOODBYE")
        self._write_request(0x02)
        self._send()

    def auto_run(self, cypher, parameters=None,
                 db=None, readonly=False, bookmarks=None, metadata=None, timeout=None):
        self._assert_no_transaction()
        self.transaction = Transaction(db, readonly, bookmarks, metadata, timeout)
        return self._run(cypher, parameters or {}, self.transaction.extra, final=True)

    def begin(self, db=None, readonly=False, bookmarks=None, metadata=None, timeout=None):
        self._assert_no_transaction()
        self.transaction = Transaction(db, readonly, bookmarks, metadata, timeout)
        log.debug("C: BEGIN %r", self.transaction.extra)
        response = self._write_request(0x11, self.transaction.extra)
        if bookmarks:
            self._sync(response)
        return self.transaction

    def commit(self, tx):
        self._assert_open_transaction(tx)
        try:
            log.debug("C: COMMIT")
            self._sync(self._write_request(0x12))
        finally:
            self.transaction = None

    def rollback(self, tx):
        self._assert_open_transaction(tx)
        try:
            log.debug("C: ROLLBACK")
            self._sync(self._write_request(0x13))
        finally:
            self.transaction = None

    def run(self, tx, cypher, parameters=None):
        self._assert_open_transaction(tx)
        return self._run(cypher, parameters or {}, self.transaction.extra)

    def _run(self, cypher, parameters, extra=None, final=False):
        log.debug("C: RUN %r %r %r", cypher, parameters, extra or {})
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


def main():
    from neobolt.diagnostics import watch
    watch(__name__)
    cx = Bolt.open()
    print(cx.server_agent)
    print(cx.connection_id)
    tx = cx.begin()
    q1 = cx.run(tx, "UNWIND range(1, $max) AS n RETURN n", {"max": 3})
    cx.pull(q1)
    cx.commit(tx)
    print(cx.bookmark())
    # bolt.reset()
    q2 = cx.auto_run("UNWIND range(1, $max) AS n RETURN n", {"max": 3})
    cx.pull(q2)
    cx.send(q2)
    cx.wait(q2)
    assert q2.done()
    for record in q2.records():
        print(record)
    print(cx.bookmark())
    cx.close()


if __name__ == "__main__":
    main()
