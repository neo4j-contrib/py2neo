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

from pytest import fixture, raises

from py2neo.connect.wire import Wire


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
