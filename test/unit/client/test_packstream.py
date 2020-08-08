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


import random
from collections import OrderedDict
from math import isnan
from struct import pack_into

from pytest import raises

from py2neo.client.packstream import packed, UnpackStream, Structure


class FakeString(str):

    def __init__(self, size):
        super(FakeString, self).__init__()
        self.size = size

    def encode(self, encoding="utf-8", errors="strict"):
        return FakeBytes(self.size)


class FakeBytes(bytes):

    def __init__(self, size):
        super(FakeBytes, self).__init__()
        self.size = size

    def __len__(self):
        return self.size


class FakeByteArray(bytearray):

    def __init__(self, size):
        super(FakeByteArray, self).__init__()
        self.size = size

    def __len__(self):
        return self.size


class FakeList(list):

    def __init__(self, size):
        super(FakeList, self).__init__()
        self.size = size

    def __len__(self):
        return self.size


class FakeDict(OrderedDict):

    def __init__(self, size):
        super(FakeDict, self).__init__()
        self.size = size

    def __len__(self):
        return self.size


def pack_and_unpack(value):
    b = packed(value)
    unpacked = UnpackStream(b).unpack()
    return b, unpacked


def assert_packable(value, b):
    assert pack_and_unpack(value) == (b, value)


def test_null():
    assert_packable(None, b"\xC0")


def test_boolean_true():
    assert_packable(True, b"\xC3")


def test_boolean_false():
    assert_packable(False, b"\xC2")


def test_inline_integer():
    for i in range(-16, 128):
        assert_packable(i, bytes(bytearray([i % 0x100])))


def test_8bit_integer():
    for i in range(-128, -16):
        assert_packable(i, bytes(bytearray([0xC8, i % 0x100])))


def test_16bit_negative_integer():
    for i in range(-0x8000, -0x80):
        data = bytearray([0xC9, 0, 0])
        pack_into(">h", data, 1, i)
        assert_packable(i, data)


def test_16bit_positive_integer():
    for i in range(0x80, 0x8000):
        data = bytearray([0xC9, 0, 0])
        pack_into(">h", data, 1, i)
        assert_packable(i, data)


def test_32bit_negative_integer():
    for i in range(-0x80000000, -0x8000, 100001):
        data = bytearray([0xCA, 0, 0, 0, 0])
        pack_into(">i", data, 1, i)
        assert_packable(i, data)


def test_32bit_positive_integer():
    for i in range(0x8000, 0x80000000, 100001):
        data = bytearray([0xCA, 0, 0, 0, 0])
        pack_into(">i", data, 1, i)
        assert_packable(i, data)


def test_64bit_negative_integer():
    for i in range(-0x8000000000000000, -0x80000000, 1000000000000001):
        data = bytearray([0xCB, 0, 0, 0, 0, 0, 0, 0, 0])
        pack_into(">q", data, 1, i)
        assert_packable(i, data)


def test_64bit_positive_integer():
    for i in range(0x80000000, 0x8000000000000000, 1000000000000001):
        data = bytearray([0xCB, 0, 0, 0, 0, 0, 0, 0, 0])
        pack_into(">q", data, 1, i)
        assert_packable(i, data)


def test_extra_large_positive_integer():
    n = 0x100000000000000000
    with raises(ValueError):
        pack_and_unpack(n)


def test_extra_large_negative_integer():
    n = -0x100000000000000000
    with raises(ValueError):
        pack_and_unpack(n)


def test_float():
    random.seed(0)
    for _ in range(10000):
        n = random.uniform(-1e10, 1e10)
        data = bytearray([0xC1, 0, 0, 0, 0, 0, 0, 0, 0])
        pack_into(">d", data, 1, n)
        assert_packable(n, data)


def test_float_positive_zero():
    n = float("+0.0")
    data = bytearray([0xC1, 0, 0, 0, 0, 0, 0, 0, 0])
    pack_into(">d", data, 1, n)
    assert_packable(n, data)


def test_float_negative_zero():
    n = float("-0.0")
    data = bytearray([0xC1, 0, 0, 0, 0, 0, 0, 0, 0])
    pack_into(">d", data, 1, n)
    assert_packable(n, data)


def test_float_positive_infinity():
    n = float("+inf")
    data = bytearray([0xC1, 0, 0, 0, 0, 0, 0, 0, 0])
    pack_into(">d", data, 1, n)
    assert_packable(n, data)


def test_float_negative_infinity():
    n = float("-inf")
    data = bytearray([0xC1, 0, 0, 0, 0, 0, 0, 0, 0])
    pack_into(">d", data, 1, n)
    assert_packable(n, data)


def test_float_nan():
    n = float("nan")
    data = bytearray([0xC1, 0, 0, 0, 0, 0, 0, 0, 0])
    pack_into(">d", data, 1, n)
    b, unpacked = pack_and_unpack(n)
    assert b == data
    assert isnan(unpacked)


def test_inline_string():
    for n in range(16):
        s = "A" * n
        data = bytearray([0x80 + n]) + s.encode("utf-8")
        assert_packable(s, data)


def test_small_string():
    for n in range(16, 256):
        s = "A" * n
        data = bytearray([0xD0, n]) + s.encode("utf-8")
        assert_packable(s, data)


def test_medium_string():
    n = 0x100
    s = "A" * n
    data = bytearray([0xD1, 0, 0]) + s.encode("utf-8")
    pack_into(">H", data, 1, n)
    assert_packable(s, data)


def test_large_string():
    n = 0x10000
    s = "A" * n
    data = bytearray([0xD2, 0, 0, 0, 0]) + s.encode("utf-8")
    pack_into(">I", data, 1, n)
    assert_packable(s, data)


def test_extra_large_string():
    s = FakeString(0x100000000)
    with raises(ValueError):
        pack_and_unpack(s)


def test_small_byte_array():
    for n in range(16, 256):
        b = bytearray(n)
        data = bytearray([0xCC, n]) + b
        assert_packable(b, data)


def test_medium_byte_array():
    n = 0x100
    b = bytearray(n)
    data = bytearray([0xCD, 0, 0]) + b
    pack_into(">H", data, 1, n)
    assert_packable(b, data)


def test_large_byte_array():
    n = 0x10000
    b = bytearray(n)
    data = bytearray([0xCE, 0, 0, 0, 0]) + b
    pack_into(">I", data, 1, n)
    assert_packable(b, data)


def test_extra_large_byte_array():
    b = FakeByteArray(0x100000000)
    with raises(ValueError):
        pack_and_unpack(b)


def test_inline_list():
    for n in range(16):
        s = [0] * n
        data = bytearray([0x90 + n]) + b"\x00" * n
        assert_packable(s, data)


def test_small_list():
    for n in range(16, 256):
        b = [0] * n
        data = bytearray([0xD4, n]) + b"\x00" * n
        assert_packable(b, data)


def test_medium_list():
    n = 0x100
    b = [0] * n
    data = bytearray([0xD5, 0, 0]) + b"\x00" * n
    pack_into(">H", data, 1, n)
    assert_packable(b, data)


def test_large_list():
    n = 0x10000
    b = [0] * n
    data = bytearray([0xD6, 0, 0, 0, 0]) + b"\x00" * n
    pack_into(">I", data, 1, n)
    assert_packable(b, data)


def test_extra_large_list():
    b = FakeList(0x100000000)
    with raises(ValueError):
        pack_and_unpack(b)


def test_inline_dict():
    for n in range(16):
        keys = ["%02X" % i for i in range(n)]
        s = OrderedDict.fromkeys(keys)
        data = bytearray([0xA0 + n]) + b"".join(b"\x82" + key.encode("utf-8") + b"\xC0"
                                                for key in keys)
        assert_packable(s, data)


def test_small_dict():
    for n in range(16, 256):
        keys = ["%02X" % i for i in range(n)]
        b = OrderedDict.fromkeys(keys)
        data = bytearray([0xD8, n]) + b"".join(b"\x82" + key.encode("utf-8") + b"\xC0"
                                               for key in keys)
        assert_packable(b, data)


def test_medium_dict():
    n = 0x100
    keys = ["%02X" % i for i in range(n)]
    b = OrderedDict.fromkeys(keys)
    data = bytearray([0xD9, 0, 0]) + b"".join(b"\x82" + key.encode("utf-8") + b"\xC0"
                                              for key in keys)
    pack_into(">H", data, 1, n)
    assert_packable(b, data)


def test_large_dict():
    n = 0x10000
    keys = ["%04X" % i for i in range(n)]
    b = OrderedDict.fromkeys(keys)
    data = bytearray([0xDA, 0, 0, 0, 0]) + b"".join(b"\x84" + key.encode("utf-8") + b"\xC0"
                                                    for key in keys)
    pack_into(">I", data, 1, n)
    assert_packable(b, data)


def test_extra_large_dict():
    b = FakeDict(0x100000000)
    with raises(ValueError):
        pack_and_unpack(b)


def test_struct():
    for n in range(16):
        fields = [0] * n
        s = Structure(0x7F, *fields)
        data = bytearray([0xB0 + n, 0x7F]) + b"\x00" * n
        assert_packable(s, data)


def test_extra_large_struct():
    fields = [0] * 16
    s = Structure(0x7F, *fields)
    with raises(ValueError):
        pack_and_unpack(s)
