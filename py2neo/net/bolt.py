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
from logging import getLogger
from socket import socket

from py2neo.net.api import get_connection_data, Connection, Transaction, TransactionError, Query
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
        rx = ByteReader(s)
        tx = ByteWriter(s)
        protocol_version = cls.handshake(rx, tx)
        subclass = cls.get_subclass(protocol_version)
        if subclass is None:
            raise RuntimeError("Unable to agree supported protocol version")
        bolt = subclass(cx_data, rx, tx)
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
    def handshake(cls, rx, tx):
        tx.write(b"\x60\x60\xB0\x17"
                 b"\x00\x00\x00\x03"
                 b"\x00\x00\x00\x02"
                 b"\x00\x00\x00\x01"
                 b"\x00\x00\x00\x00")
        tx.send()
        v = bytearray(rx.read(4))
        return v[-1], v[-2]

    def __init__(self, cx_data, rx, tx):
        super(Bolt, self).__init__(cx_data)
        self.rx = rx
        self.tx = tx

    def close(self):
        self.goodbye()
        self.tx.close()
        log.debug("C: <CLOSE>")


class Bolt1(Bolt):

    protocol_version = (1, 0)

    def __init__(self, cx_data, rx, tx):
        super(Bolt1, self).__init__(cx_data, rx, tx)
        self.reader = MessageReader(rx)
        self.writer = MessageWriter(tx)
        self.responses = deque()
        self.transaction = None

    def hello(self, auth):
        user, password = auth
        extra = {"scheme": "basic",
                 "principal": user,
                 "credentials": password}
        clean_extra = dict(extra)
        clean_extra.update({"credentials": "*******"})
        log.debug("C: INIT %r %r", self.user_agent, clean_extra)
        response = self._write_request(0x01, self.user_agent, extra, vital=True)
        self.writer.send()
        self._wait(response)
        if response.failure():
            raise response.failure()
        self.server_agent = response.summary("server")
        self.connection_id = response.summary("connection_id")

    def reset(self):
        log.debug("C: RESET")
        response = self._write_request(0x0F, vital=True)
        self.writer.send()
        self._wait(response)
        if response.failure():
            raise response.failure()

    def _write_request(self, tag, *fields, vital=False):
        # vital responses close on failure and cannot be ignored
        response = BoltResponse(vital=vital)
        # TODO: logging
        self.writer.write_message(tag, *fields)
        self.responses.append(response)
        return response

    def pull(self, query, n=-1):
        if query is not self.transaction.last_query():
            raise TransactionError("Random query access is not supported before Bolt 4.0")
        if n != -1:
            raise TransactionError("Flow control is not supported before Bolt 4.0")
        log.debug("C: PULL_ALL")
        response = self._write_request(0x3F)
        query.add_response(response)
        query.set_done()
        return response

    def discard(self, query, n=-1):
        if query is not self.transaction.last_query():
            raise TransactionError("Random query access is not supported before Bolt 4.0")
        if n != -1:
            raise TransactionError("Flow control is not supported before Bolt 4.0")
        log.debug("C: DISCARD_ALL")
        response = self._write_request(0x2F)
        query.add_response(response)
        query.set_done()
        return response

    def send(self, query):
        if query is not self.transaction.last_query():
            raise TransactionError("Random query access is not supported before Bolt 4.0")
        self.writer.send()

    def wait(self, query):
        if query is not self.transaction.last_query():
            raise TransactionError("Random query access is not supported before Bolt 4.0")
        response = query.last_response()
        if response is not None:
            self._wait(response)

    def _wait(self, response):
        while not response.done():
            top_response = self.responses[0]
            tag, fields = self.reader.read_message()
            if tag == 0x70:
                log.debug("S: SUCCESS %s", " ".join(map(repr, fields)))
                top_response.set_success(**fields[0])
                self.responses.popleft()
            elif tag == 0x71:
                log.debug("S: RECORD %s", " ".join(map(repr, fields)))
                top_response.add_record(*fields[0])
            elif tag == 0x7F:
                log.debug("S: FAILURE %s", " ".join(map(repr, fields)))
                top_response.set_failure(**fields[0])
                self.responses.popleft()
                if top_response.vital:
                    self.tx.close()
            elif tag == 0x7E and not top_response.vital:
                log.debug("S: IGNORED")
                top_response.set_ignored()
                self.responses.popleft()
            else:
                log.debug("S: <ERROR>")
                self.tx.close()
                raise RuntimeError("Unexpected protocol message #%02X", tag)


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
        self.writer.send()
        self._wait(response)
        if response.failure():
            raise response.failure()
        # TODO: handle IGNORED
        self.server_agent = response.summary("server")
        self.connection_id = response.summary("connection_id")

    def goodbye(self):
        log.debug("C: GOODBYE")
        self._write_request(0x02)
        self.writer.send()

    def auto_run(self, db, cypher, parameters=None, readonly=False, bookmarks=None, metadata=None, timeout=None):
        if self.transaction is not None:
            raise TransactionError("Bolt connection already holds transaction %r", self.transaction)
        transaction = Transaction(db, readonly, bookmarks, metadata, timeout)
        log.debug("C: RUN %r %r %r", cypher, parameters or {}, transaction.extra)
        response = self._write_request(0x10, cypher, parameters or {}, transaction.extra)  # TODO: dehydrate parameters
        query = BoltQuery(response)
        transaction.add_query(query)
        return query

    def begin(self, db, readonly=False, bookmarks=None, metadata=None, timeout=None):
        if self.transaction is not None:
            raise TransactionError("Bolt connection already holds transaction %r", self.transaction)
        transaction = Transaction(db, readonly, bookmarks, metadata, timeout)
        log.debug("C: BEGIN %r", transaction.extra)
        response = self._write_request(0x11, transaction.extra)
        self.writer.send()
        self._wait(response)
        if response.failure():
            raise response.failure()
        # TODO: handle IGNORED
        self.transaction = transaction
        return self.transaction


class Bolt4x0(Bolt3):

    protocol_version = (4, 0)


class BoltResponse(object):
    # A future-like object

    def __init__(self, vital=False):
        self.vital = vital
        self._records = deque()
        self._status = None
        self._metadata = {}
        self._failure = None

    def add_record(self, *values):
        self._records.append(values)

    def set_success(self, **metadata):
        self._status = 1
        self._metadata.update(metadata)

    def set_failure(self, **metadata):
        self._status = -1
        self._failure = BoltFailure(**metadata)

    def set_ignored(self):
        self._status = 0

    def done(self):
        return self._status is not None

    def failure(self):
        return self._failure

    def ignored(self):
        return self._status == 0

    def records(self):
        return iter(self._records)

    def summary(self, key, default=None):
        return self._metadata.get(key, default)


class BoltQuery(Query):

    def __init__(self, *responses):
        self._responses = deque(responses)
        self._done = False

    def add_response(self, response):
        self._responses.append(response)

    def last_response(self):
        try:
            return self._responses[-1]
        except IndexError:
            return None

    def set_done(self):
        self._done = True

    def done(self):
        return self._done

    def records(self):
        for response in self._responses:
            for record in response.records():
                yield record


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
    print(cx.protocol_version)
    print(cx.server_agent)
    print(cx.connection_id)
    # tx = bolt.begin("neo4j")
    # bolt.reset()
    query = cx.auto_run("neo4j", "UNWIND range(1, 3) AS n RETURN n")
    cx.pull(query)
    cx.send(query)
    cx.wait(query)
    for record in query.records():
        print(record)
    cx.close()


if __name__ == "__main__":
    main()
