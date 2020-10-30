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
from socket import AF_INET, AF_INET6, SOCK_STREAM

from pytest import fixture, raises

from py2neo.wiring import Wire, Address, WireError


class FakeSocket(object):

    def __init__(self, in_packets=(), out_packets=()):
        self._in_packets = deque(in_packets)
        self._in_buffer = bytearray()
        self._out_packets = out_packets
        self._closed = False

    def settimeout(self, value):
        pass

    def recv(self, n_bytes, flags=None):
        while not self._in_buffer:
            try:
                data = self._in_packets.popleft()
            except IndexError:
                return b""
            else:
                self._in_buffer.extend(data)
        value, self._in_buffer = self._in_buffer[:n_bytes], self._in_buffer[n_bytes:]
        return value

    def send(self, b, flags=None):
        if self._closed:
            raise OSError("Socket closed")
        self._out_packets.append(bytes(b))
        return len(b)

    def sendall(self, b, flags=None):
        if self._closed:
            raise OSError("Socket closed")
        self._out_packets.append(bytes(b))

    def close(self):
        self._closed = True


@fixture
def fake_reader():
    def reader(packets):
        s = FakeSocket(packets)
        return Wire(s)
    return reader


@fixture
def fake_writer():
    def writer(into):
        s = FakeSocket(out_packets=into)
        return Wire(s)
    return writer


class MockSocket(object):

    fail_on_connect = False
    fail_on_recv = False

    def __init__(self, family=AF_INET, type=SOCK_STREAM, proto=0, fileno=None):
        self.__family = family
        self.__type = type
        self.__proto = proto
        self.__fileno = fileno
        self.__timeout = None
        self.__peer = None
        self.on_connect = None

    def settimeout(self, value):
        self.__timeout = value

    def setsockopt(self, level, optname, value, optlen=None):
        pass

    def connect(self, address):
        if self.fail_on_connect:
            raise OSError("Connection refused to %r" % (address,))
        else:
            self.__peer = address

    def getpeername(self):
        return self.__peer

    def recv(self, bufsize, flags=None):
        if self.fail_on_recv:
            raise OSError("Connection broken")
        else:
            raise NotImplementedError


@fixture
def mock_socket(monkeypatch):
    monkeypatch.setattr("socket.socket", MockSocket)
    return MockSocket


def test_wire_open_simple(mock_socket):
    wire = Wire.open(("localhost", 7687))
    assert wire.remote_address == ("localhost", 7687)


def test_wire_open_with_keep_alive(mock_socket):
    wire = Wire.open(("localhost", 7687), keep_alive=True)
    assert wire.remote_address == ("localhost", 7687)


def test_wire_open_with_connect_error(mock_socket):
    mock_socket.fail_on_connect = True
    try:
        with raises(WireError):
            _ = Wire.open(("localhost", 7687))
    finally:
        mock_socket.fail_on_connect = False


def test_wire_read_with_recv_error(mock_socket):
    mock_socket.fail_on_recv = True
    try:
        wire = Wire.open(("localhost", 7687))
        with raises(WireError):
            _ = wire.read(1)
    finally:
        mock_socket.fail_on_recv = False


def test_byte_reader_read_when_enough_available(fake_reader):
    reader = fake_reader([b"hello, world"])
    data = reader.read(12)
    assert data == b"hello, world"


def test_byte_reader_read_when_extra_available(fake_reader):
    reader = fake_reader([b"hello, world"])
    data = reader.read(5)
    assert data == b"hello"


def test_byte_reader_read_when_multiple_packets_available(fake_reader):
    reader = fake_reader([b"hello, world"])
    data = reader.read(12)
    assert data == b"hello, world"


def test_byte_reader_read_when_not_enough_available(fake_reader):
    reader = fake_reader([b"hello"])
    with raises(OSError):
        _ = reader.read(12)


def test_byte_writer_write_once(fake_writer):
    into = []
    writer = fake_writer(into)
    writer.write(b"hello, world")
    writer.send()
    assert into == [b"hello, world"]


def test_byte_writer_write_twice(fake_writer):
    into = []
    writer = fake_writer(into)
    writer.write(b"hello,")
    writer.write(b" world")
    writer.send()
    assert into == [b"hello, world"]


def test_byte_writer_close(fake_writer):
    into = []
    writer = fake_writer(into)
    writer.close()
    with raises(OSError):
        assert writer.send()


def test_address_parse_ipv4():
    parsed = Address.parse("127.0.0.1:7687")
    assert parsed.family == AF_INET
    assert parsed.host == "127.0.0.1"
    assert parsed.port_number == 7687


def test_address_parse_ipv6():
    parsed = Address.parse("[::1]:7687")
    assert parsed.family == AF_INET6
    assert parsed.host == "::1"
    assert parsed.port_number == 7687
