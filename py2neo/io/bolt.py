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

from py2neo.internal.connectors import get_connection_data
from py2neo.io.packstream import MessageReader, MessageWriter
from py2neo.io.rxtx import Receiver, Transmitter


class Bolt(object):

    __subclasses = None

    protocol_version = None

    @classmethod
    def get_subclass(cls, protocol_version):
        if cls.__subclasses is None:
            cls.__subclasses = {}
            for k in cls.__subclasses__():
                assert issubclass(k, cls)
                cls.__subclasses[k.protocol_version] = k
        return cls.__subclasses.get(protocol_version)

    @classmethod
    def open(cls, uri=None, **settings):
        # TODO
        connection_data = get_connection_data(uri, **settings)
        s = cls.connect(("localhost", 7687))
        s = cls.secure(s)
        rx = Receiver(s)
        tx = Transmitter(s)
        protocol_version = cls.handshake(rx, tx)
        subclass = cls.get_subclass(protocol_version)
        if subclass is None:
            raise RuntimeError("Unable to agree supported protocol version")
        bolt = subclass(rx, tx)
        bolt.hello(auth=("neo4j", "password"), user_agent="foo/1.0")
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
        tx.transmit()
        v = bytearray(rx.read(4))
        return v[-1], v[-2]

    def __init__(self, rx, tx):
        self.rx = rx
        self.tx = tx

    def close(self):
        self.tx.close()

    def hello(self, auth, user_agent):
        pass


class Bolt1(Bolt):

    protocol_version = (1, 0)


class Bolt2(Bolt):

    protocol_version = (2, 0)


class Bolt3(Bolt):

    protocol_version = (3, 0)

    def __init__(self, rx, tx):
        super(Bolt3, self).__init__(rx, tx)
        self.reader = MessageReader(rx)
        self.writer = MessageWriter(tx)

    def hello(self, auth, user_agent):
        user, password = auth
        self.writer.write_message(0x01, {"user_agent": user_agent,
                                         "scheme": "basic",
                                         "principal": user,
                                         "credentials": password})
        self.writer.send()
        print(self.reader.read_message())


class Bolt4x0(Bolt):

    protocol_version = (4, 0)


def main():
    bolt = Bolt.open()
    print(bolt.protocol_version)
    bolt.close()


if __name__ == "__main__":
    main()
