#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2021, Nigel Small
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
from datetime import date, time, datetime, timedelta
from io import BytesIO
from math import isnan
from struct import pack_into as struct_pack_into

from neotime import Date, Time, DateTime, Duration
from pytest import mark, raises
from pytz import utc, FixedOffset

from py2neo.client.packstream import UnpackStream, pack_into, pack, Structure
from py2neo.compat import unicode_types
from py2neo.data.spatial import CartesianPoint, WGS84Point, Point


class FakeString(unicode_types[0]):

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


def pack_and_unpack(value, version=()):
    buffer = BytesIO()
    pack_into(buffer, value, version=version)
    b = buffer.getvalue()
    unpacked = UnpackStream(b).unpack()
    return b, unpacked


def assert_packable(value, b, protocol_version=()):
    assert pack_and_unpack(value, protocol_version) == (b, value)


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
        struct_pack_into(">h", data, 1, i)
        assert_packable(i, data)


def test_16bit_positive_integer():
    for i in range(0x80, 0x8000):
        data = bytearray([0xC9, 0, 0])
        struct_pack_into(">h", data, 1, i)
        assert_packable(i, data)


def test_32bit_negative_integer():
    for i in range(-0x80000000, -0x8000, 100001):
        data = bytearray([0xCA, 0, 0, 0, 0])
        struct_pack_into(">i", data, 1, i)
        assert_packable(i, data)


def test_32bit_positive_integer():
    for i in range(0x8000, 0x80000000, 100001):
        data = bytearray([0xCA, 0, 0, 0, 0])
        struct_pack_into(">i", data, 1, i)
        assert_packable(i, data)


def test_64bit_negative_integer():
    for i in range(-0x8000000000000000, -0x80000000, 1000000000000001):
        data = bytearray([0xCB, 0, 0, 0, 0, 0, 0, 0, 0])
        struct_pack_into(">q", data, 1, i)
        assert_packable(i, data)


def test_64bit_positive_integer():
    for i in range(0x80000000, 0x8000000000000000, 1000000000000001):
        data = bytearray([0xCB, 0, 0, 0, 0, 0, 0, 0, 0])
        struct_pack_into(">q", data, 1, i)
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
        struct_pack_into(">d", data, 1, n)
        assert_packable(n, data)


def test_float_positive_zero():
    n = float("+0.0")
    data = bytearray([0xC1, 0, 0, 0, 0, 0, 0, 0, 0])
    struct_pack_into(">d", data, 1, n)
    assert_packable(n, data)


def test_float_negative_zero():
    n = float("-0.0")
    data = bytearray([0xC1, 0, 0, 0, 0, 0, 0, 0, 0])
    struct_pack_into(">d", data, 1, n)
    assert_packable(n, data)


def test_float_positive_infinity():
    n = float("+inf")
    data = bytearray([0xC1, 0, 0, 0, 0, 0, 0, 0, 0])
    struct_pack_into(">d", data, 1, n)
    assert_packable(n, data)


def test_float_negative_infinity():
    n = float("-inf")
    data = bytearray([0xC1, 0, 0, 0, 0, 0, 0, 0, 0])
    struct_pack_into(">d", data, 1, n)
    assert_packable(n, data)


def test_float_nan():
    n = float("nan")
    data = bytearray([0xC1, 0, 0, 0, 0, 0, 0, 0, 0])
    struct_pack_into(">d", data, 1, n)
    b, unpacked = pack_and_unpack(n)
    assert b == data
    assert isnan(unpacked)


def test_inline_byte_string():
    s = b"hello, world"
    data = bytearray([0x8C]) + s
    b, unpacked = pack_and_unpack(s)
    assert b == data
    assert unpacked == u"hello, world"


def test_inline_unicode_string():
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
    struct_pack_into(">H", data, 1, n)
    assert_packable(s, data)


def test_large_string():
    n = 0x10000
    s = "A" * n
    data = bytearray([0xD2, 0, 0, 0, 0]) + s.encode("utf-8")
    struct_pack_into(">I", data, 1, n)
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
    struct_pack_into(">H", data, 1, n)
    assert_packable(b, data)


def test_large_byte_array():
    n = 0x10000
    b = bytearray(n)
    data = bytearray([0xCE, 0, 0, 0, 0]) + b
    struct_pack_into(">I", data, 1, n)
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
    struct_pack_into(">H", data, 1, n)
    assert_packable(b, data)


def test_large_list():
    n = 0x10000
    b = [0] * n
    data = bytearray([0xD6, 0, 0, 0, 0]) + b"\x00" * n
    struct_pack_into(">I", data, 1, n)
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
    struct_pack_into(">H", data, 1, n)
    assert_packable(b, data)


def test_large_dict():
    n = 0x10000
    keys = ["%04X" % i for i in range(n)]
    b = OrderedDict.fromkeys(keys)
    data = bytearray([0xDA, 0, 0, 0, 0]) + b"".join(b"\x84" + key.encode("utf-8") + b"\xC0"
                                                    for key in keys)
    struct_pack_into(">I", data, 1, n)
    assert_packable(b, data)


def test_extra_large_dict():
    b = FakeDict(0x100000000)
    with raises(ValueError):
        pack_and_unpack(b)


def test_dict_fails_with_non_string_key():
    buffer = BytesIO()
    with raises(TypeError):
        pack_into(buffer, {object(): 1})


@mark.parametrize("cls", [date, Date])
def test_date(cls):
    from neotime import Date
    b, unpacked = pack_and_unpack(cls(1970, 1, 1), version=(2, 0))
    assert b == b"\xB1D\x00"
    assert unpacked == Date(1970, 1, 1)


@mark.parametrize("cls", [time, Time])
def test_naive_time(cls):
    from neotime import Time
    b, unpacked = pack_and_unpack(cls(0, 0, 0), version=(2, 0))
    assert b == b"\xB1t\x00"
    assert unpacked == Time(0, 0, 0)


@mark.parametrize("cls", [time, Time])
def test_aware_time(cls):
    from neotime import Time
    b, unpacked = pack_and_unpack(cls(0, 0, 0, tzinfo=utc), version=(2, 0))
    assert b == b"\xB2T\x00\x00"
    assert unpacked == Time(0, 0, 0, tzinfo=utc)


@mark.parametrize("cls", [datetime, DateTime])
def test_naive_datetime(cls):
    from neotime import DateTime
    b, unpacked = pack_and_unpack(cls(1970, 1, 1, 0, 0, 0), version=(2, 0))
    assert b == b"\xB2d\x00\x00"
    assert unpacked == DateTime(1970, 1, 1, 0, 0, 0)


@mark.parametrize("cls", [datetime, DateTime])
def test_datetime_with_named_timezone(cls):
    from neotime import DateTime
    b, unpacked = pack_and_unpack(cls(1970, 1, 1, 0, 0, 0, tzinfo=utc), version=(2, 0))
    assert b == b"\xB3f\x00\x00\x83UTC"
    assert unpacked == DateTime(1970, 1, 1, 0, 0, 0, tzinfo=utc)


@mark.parametrize("cls", [datetime, DateTime])
def test_datetime_with_timezone_offset(cls):
    from neotime import DateTime
    b, unpacked = pack_and_unpack(cls(1970, 1, 1, 0, 0, 0, tzinfo=FixedOffset(1)),
                                  version=(2, 0))
    assert b == b"\xB3F\x00\x00\x3C"
    assert unpacked == DateTime(1970, 1, 1, 0, 0, 0, tzinfo=FixedOffset(1))


@mark.parametrize("cls", [timedelta, Duration])
def test_timedelta_and_duration(cls):
    from neotime import Duration
    b, unpacked = pack_and_unpack(cls(), version=(2, 0))
    assert b == b"\xB4E\x00\x00\x00\x00"
    assert unpacked == Duration()


@mark.parametrize("cls,srid", [(CartesianPoint, 7203), (WGS84Point, 4326)])
def test_2d_point(cls, srid):
    b, unpacked = pack_and_unpack(cls((0, 0)), version=(2, 0))
    assert b == b"\xB3X" + pack(srid) + b"\x00\x00"
    assert unpacked == cls((0, 0))


@mark.parametrize("cls,srid", [(CartesianPoint, 9157), (WGS84Point, 4979)])
def test_3d_point(cls, srid):
    b, unpacked = pack_and_unpack(cls((0, 0, 0)), version=(2, 0))
    assert b == b"\xB4Y" + pack(srid) + b"\x00\x00\x00"
    assert unpacked == cls((0, 0, 0))


def test_4d_point():
    with raises(ValueError):
        _ = pack(Point((0, 0, 0, 0)), version=(2, 0))


@mark.parametrize("version", [(1, 0), (2, 0)])
def test_packing_unknown_type(version):
    buffer = BytesIO()
    with raises(TypeError):
        pack_into(buffer, object(), version=version)


def test_unpacking_unknown_marker():
    with raises(ValueError):
        UnpackStream(b"\xDF").unpack()
