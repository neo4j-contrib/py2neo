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


from socket import socket

from py2neo.io.connect import Connection, get_connection_data
from py2neo.io.syntax import MessageReader, MessageWriter
from py2neo.io.wire import ByteReader, ByteWriter


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
                 b"\x00\x00\x00\x04"
                 b"\x00\x00\x00\x03"
                 b"\x00\x00\x00\x02"
                 b"\x00\x00\x00\x01")
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


class Bolt1(Bolt):

    protocol_version = (1, 0)

    def __init__(self, cx_data, rx, tx):
        super(Bolt1, self).__init__(cx_data, rx, tx)
        self.reader = MessageReader(rx)
        self.writer = MessageWriter(tx)

    def hello(self, auth):
        user, password = auth
        self.writer.write_message(0x01, self.user_agent, {"scheme": "basic",
                                                          "principal": user,
                                                          "credentials": password})
        self.writer.send()
        tag, (metadata,) = self.reader.read_message()
        if tag != 0x70:
            self.tx.close()
            raise BoltFailure(**metadata)
        self.server_agent = metadata.get("server")
        self.connection_id = metadata.get("connection_id")

    def reset(self):
        self.writer.write_message(0x0F)
        self.writer.send()
        tag, (metadata,) = self.reader.read_message()
        if tag != 0x70:
            self.tx.close()
            raise BoltFailure(**metadata)


class Bolt2(Bolt1):

    protocol_version = (2, 0)


class Bolt3(Bolt2):

    protocol_version = (3, 0)

    def hello(self, auth):
        user, password = auth
        self.writer.write_message(0x01, {"user_agent": self.user_agent,
                                         "scheme": "basic",
                                         "principal": user,
                                         "credentials": password})
        self.writer.send()
        tag, (metadata,) = self.reader.read_message()
        if tag != 0x70:
            self.tx.close()
            raise BoltFailure(**metadata)
        self.server_agent = metadata.get("server")
        self.connection_id = metadata.get("connection_id")

    def goodbye(self):
        self.writer.write_message(0x02)
        self.writer.send()


class Bolt4x0(Bolt3):

    protocol_version = (4, 0)


class BoltFailure(Exception):

    def __init__(self, message, code):
        super(BoltFailure, self).__init__(message)
        self.code = code

    def __str__(self):
        return "[%s] %s" % (self.code, super(BoltFailure, self).__str__())


def main():
    bolt = Bolt.open()
    print(bolt.protocol_version)
    print(bolt.server_agent)
    print(bolt.connection_id)
    bolt.reset()
    bolt.close()


if __name__ == "__main__":
    main()
