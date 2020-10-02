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
from socketserver import TCPServer
from threading import Thread

from py2neo.client.bolt import BoltMessageReader, BoltMessageWriter
from py2neo.wiring import WireRequestHandler, BrokenWireError


CONNECTED = 0
READY = 1
STREAMING = 2
TX_READY = 3
TX_STREAMING = 4
FAILED = -1


class Bolt1:

    INIT = 0x01
    RESET = 0x0F
    RUN = 0x10
    DISCARD_ALL = 0x2F
    PULL_ALL = 0x3F

    SUCCESS = 0x70
    RECORD = 0x71
    IGNORED = 0x7E
    FAILURE = 0x7F

    def __init__(self, wire, version, queries=None):
        self.wire = wire
        self.reader = BoltMessageReader(wire)
        self.writer = BoltMessageWriter(wire)
        self.version = version
        self.queries = dict(queries or {})
        self.state = CONNECTED
        self.records = deque()

    def log(self, message, *args):
        remote = self.wire.remote_address
        print("[%s:%s] %s" % (remote.host, remote.port_number, message % args))

    def send(self):
        self.writer.send()

    def handle(self):
        while not (self.wire.closed or self.wire.broken):
            try:
                tag, fields = self.reader.read_message()
            except BrokenWireError:
                self.log("C: (Hangup)")
            else:
                try:
                    path = self.state_machine[self.state]
                except KeyError:
                    raise NotImplementedError("Unknown state {!r}".format(self.state))
                else:
                    try:
                        f = path[tag]
                    except KeyError:
                        self.abort("Illegal message {!r} for state {!r}".format(tag, self.state))
                    except TypeError:
                        self.abort("Wrong number of fields for message {!r} "
                                   "in state {!r}".format(tag, self.state))
                    else:
                        f(self, *fields)

    @property
    def server_agent(self):
        return "Neo4j/%r.%r" % self.version

    @classmethod
    def _hidden_credentials(cls, data):
        hidden_data = dict(data)
        if "credentials" in hidden_data:
            hidden_data["credentials"] = "*******"
        return hidden_data

    def on_init(self, user_agent, auth):
        self.log("C: INIT %r %r", user_agent, self._hidden_credentials(auth))
        self.summary(server=self.server_agent)
        self.send()
        self.state = READY

    def on_reset(self):
        self.log("C: RESET")
        self.summary()
        self.send()
        self.state = READY

    def on_run(self, cypher, parameters):
        self.log("C: RUN %r %r", cypher, parameters)
        try:
            query = self.queries[cypher]
        except KeyError:
            self.fail()
        else:
            self.summary(fields=list(query[0]))
            self.records.extend(query[1:])
            self.send()
            self.state = STREAMING

    def on_discard_all(self):
        self.log("C: DISCARD_ALL")
        self.records.clear()
        self.summary()
        self.send()

    def on_pull_all(self):
        self.log("C: PULL_ALL")
        while self.records:
            self.record(list(self.records.popleft()))
        self.summary()
        self.send()

    def summary(self, **metadata):
        self.log("S: SUCCESS %r", metadata)
        self.writer.write_message(self.SUCCESS, dict(metadata or {}))

    def record(self, values):
        self.log("S: RECORD %r", values)
        self.writer.write_message(self.RECORD, values)

    def ignore(self):
        self.log("S: IGNORED")
        self.writer.write_message(self.IGNORED)
        self.send()

    def fail(self, metadata=None):
        self.log("S: FAILURE %r", metadata)
        self.writer.write_message(self.FAILURE, dict(metadata or {}))
        self.send()
        self.state = FAILED

    def abort(self, message):
        """ Abort due to protocol violation.
        """
        print("Aborting due to protocol violation: %s" % message)
        self.wire.close()

    state_machine = {
        CONNECTED: {
            INIT: on_init,
        },
        READY: {
            RESET: on_reset,
            RUN: on_run,
        },
        STREAMING: {
            RESET: on_reset,
            DISCARD_ALL: on_discard_all,
            PULL_ALL: on_pull_all,
        },
        FAILED: {
            RESET: on_reset,
            RUN: ignore,
            DISCARD_ALL: ignore,
            PULL_ALL: ignore,
        }
    }


class Bolt2(Bolt1):

    pass


class Bolt3(Bolt2):

    HELLO = 0x01
    GOODBYE = 0x02
    RESET = 0x0F
    RUN = 0x10
    DISCARD = 0x2F
    PULL = 0x3F

    def on_hello(self, metadata):
        self.log("C: HELLO %r", self._hidden_credentials(metadata))
        self.summary(server=self.server_agent)
        self.send()
        self.state = READY

    def on_goodbye(self):
        self.log("C: GOODBYE")
        self.wire.close()

    def on_reset(self):
        super(Bolt3, self).on_reset()

    def on_run3(self, cypher, parameters, extras):
        self.log("C: RUN %r %r %r", cypher, parameters, extras)
        try:
            query = self.queries[cypher]
        except KeyError:
            self.fail()
        else:
            self.summary(fields=list(query[0]))
            self.records.extend(query[1:])
            self.send()
            self.state = STREAMING

    def on_discard(self, metadata):
        self.log("C: DISCARD %r", metadata)
        self.records.clear()
        self.summary()
        self.send()

    def on_pull(self, metadata):
        self.log("C: PULL %r", metadata)
        while self.records:
            self.record(list(self.records.popleft()))
        self.summary()
        self.send()

    def ignore(self):
        super(Bolt3, self).ignore()

    state_machine = {
        CONNECTED: {
            HELLO: on_hello,
        },
        READY: {
            GOODBYE: on_goodbye,
            RESET: on_reset,
            RUN: on_run3,
        },
        STREAMING: {
            GOODBYE: on_goodbye,
            RESET: on_reset,
            DISCARD: on_discard,
            PULL: on_pull,
        },
        FAILED: {
            GOODBYE: on_goodbye,
            RESET: on_reset,
            RUN: ignore,
            DISCARD: ignore,
            PULL: ignore,
        }
    }


class Bolt4x0(Bolt3):

    pass


class Bolt4x1(Bolt4x0):

    pass


class Neo4jSimulator(TCPServer):

    products = {
        (4, 1): {
            (4, 1): Bolt4x1,
            (4, 0): Bolt4x0,
            (3, 0): Bolt3,
        },
        (4, 0): {
            (4, 0): Bolt4x0,
            (3, 0): Bolt3,
        },
        (3, 6): {
            (3, 0): Bolt3,
            (2, 0): Bolt2,
            (1, 0): Bolt1,
        },
        (3, 5): {
            (3, 0): Bolt3,
            (2, 0): Bolt2,
            (1, 0): Bolt1,
        },
        (3, 4): {
            (2, 0): Bolt2,
            (1, 0): Bolt1,
        },
        (3, 3): {
            (1, 0): Bolt1,
        },
        (3, 2): {
            (1, 0): Bolt1,
        },
        (3, 1): {
            (1, 0): Bolt1,
        },
        (3, 0): {
            (1, 0): Bolt1,
        },
    }

    allow_reuse_address = True

    def __init__(self, server_address, version, bind_and_activate=True, queries=None,
                 max_requests=None):

        self.requests = max_requests

        try:
            handlers = self.products[version]
        except KeyError:
            raise ValueError("Unsupported Neo4j version {!r}".format(version))

        # Dynamically created class
        class Neo4jRequestHandler(WireRequestHandler):

            handler = None

            def handle(self):
                self.handle_handshake()
                self.handler.handle()

            def handle_handshake(self):
                data = self.wire.read(20)
                if data[0:4] == b"\x60\x60\xB0\x17":
                    protocol_versions = [(data[p], data[p - 1]) for p in range(7, 20, 4)]
                    protocol_version = self.set_message_handler(self.wire, protocol_versions)
                    print([0, 0, protocol_version[1], protocol_version[0]])
                    self.wire.write(bytearray([0, 0, protocol_version[1], protocol_version[0]]))
                    self.wire.send()
                else:
                    print("Not a Bolt connection")
                    self.wire.close()

            def set_message_handler(self, wire, versions):
                overlap = set(versions) & set(handlers.keys())
                if overlap:
                    protocol_version = max(overlap)
                    self.handler = handlers[protocol_version](wire, version, queries=queries)
                    return protocol_version
                else:
                    return 0, 0

        super(Neo4jSimulator, self).__init__(server_address,
                                             Neo4jRequestHandler,
                                             bind_and_activate)

    def finish_request(self, request, client_address):
        super(Neo4jSimulator, self).finish_request(request, client_address)
        if self.requests is not None:
            self.requests -= 1
            if self.requests <= 0:
                self.shutdown_thread().start()

    def shutdown_thread(self):
        server = self

        class ShutdownThread(Thread):

            def run(self):
                server.shutdown()
                server.server_close()

        print("Shutting down server")
        return ShutdownThread()


def main():
    host, port = "localhost", 17687
    server = Neo4jSimulator((host, port), version=(3, 0), max_requests=1, queries={
        "SHOW DATABASES": [
            ["name", "address", "role", "requestedStatus", "currentStatus", "error", "default"],
            ["neo4j", "localhost:7687", "standalone", None, None, None, True],
            ["system", "localhost:7687", "standalone", None, None, None, False],
        ],
        "return 1": [
            ["1"],
            [1],
        ],
    })
    server.serve_forever()


if __name__ == "__main__":
    main()
