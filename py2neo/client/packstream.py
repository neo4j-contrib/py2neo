#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2020 "Neo4j,"
# Neo4j Sweden AB [http://neo4j.com]
#
# This file is part of Neo4j.
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


from __future__ import division

from codecs import decode
from collections import namedtuple
from struct import pack as struct_pack, unpack as struct_unpack

from py2neo.client import Hydrant
from py2neo.compat import Sequence, Mapping, bytes_types, integer_types, string_types, bstr


PACKED_UINT_8 = [struct_pack(">B", value) for value in range(0x100)]
PACKED_UINT_16 = [struct_pack(">H", value) for value in range(0x10000)]

UNPACKED_UINT_8 = {bytes(bytearray([x])): x for x in range(0x100)}
UNPACKED_UINT_16 = {struct_pack(">H", x): x for x in range(0x10000)}

UNPACKED_MARKERS = {b"\xC0": None, b"\xC2": False, b"\xC3": True}
UNPACKED_MARKERS.update({bytes(bytearray([z])): z for z in range(0x00, 0x80)})
UNPACKED_MARKERS.update({bytes(bytearray([z + 256])): z for z in range(-0x10, 0x00)})


INT64_MIN = -(2 ** 63)
INT64_MAX = 2 ** 63


EndOfStream = object()


class Structure:

    def __init__(self, tag, *fields):
        self.tag = tag
        self.fields = list(fields)

    def __repr__(self):
        return "Structure[#%02X](%s)" % (self.tag, ", ".join(map(repr, self.fields)))

    def __eq__(self, other):
        try:
            return self.tag == other.tag and self.fields == other.fields
        except AttributeError:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __len__(self):
        return len(self.fields)

    def __getitem__(self, key):
        return self.fields[key]

    def __setitem__(self, key, value):
        self.fields[key] = value


class Packer:

    def __init__(self, stream):
        self.stream = stream
        self._write = self.stream.write

    def pack_raw(self, data):
        self._write(data)

    def pack(self, value):
        return self._pack(value)

    def _pack(self, value):
        write = self._write

        # None
        if value is None:
            write(b"\xC0")  # NULL

        # Boolean
        elif value is True:
            write(b"\xC3")
        elif value is False:
            write(b"\xC2")

        # Float (only double precision is supported)
        elif isinstance(value, float):
            write(b"\xC1")
            write(struct_pack(">d", value))

        # Integer
        elif isinstance(value, integer_types):
            if -0x10 <= value < 0x80:
                write(PACKED_UINT_8[value % 0x100])
            elif -0x80 <= value < -0x10:
                write(b"\xC8")
                write(PACKED_UINT_8[value % 0x100])
            elif -0x8000 <= value < 0x8000:
                write(b"\xC9")
                write(PACKED_UINT_16[value % 0x10000])
            elif -0x80000000 <= value < 0x80000000:
                write(b"\xCA")
                write(struct_pack(">i", value))
            elif INT64_MIN <= value < INT64_MAX:
                write(b"\xCB")
                write(struct_pack(">q", value))
            else:
                raise ValueError("Integer %s out of range" % value)

        # String
        elif isinstance(value, string_types):
            encoded = bstr(value)
            self.pack_string_header(len(encoded))
            self.pack_raw(encoded)

        # Byte array
        elif isinstance(value, bytes_types):
            self.pack_byte_array_header(len(value))
            self.pack_raw(bytes(value))

        # List
        elif isinstance(value, list):
            self.pack_list_header(len(value))
            for item in value:
                self._pack(item)

        # Map
        elif isinstance(value, dict):
            self.pack_map_header(len(value))
            for key, item in value.items():
                self._pack(key)
                self._pack(item)

        # Structure
        elif isinstance(value, Structure):
            self.pack_struct(value.tag, value.fields)

        # Other
        else:
            raise TypeError("Values of type %s are not supported" % type(value))

    def pack_byte_array_header(self, size):
        write = self._write
        if size < 0x100:
            write(b"\xCC")
            write(PACKED_UINT_8[size])
        elif size < 0x10000:
            write(b"\xCD")
            write(PACKED_UINT_16[size])
        elif size < 0x100000000:
            write(b"\xCE")
            write(struct_pack(">I", size))
        else:
            raise ValueError("Byte array too large")

    def pack_string_header(self, size):
        write = self._write
        if size == 0x00:
            write(b"\x80")
        elif size == 0x01:
            write(b"\x81")
        elif size == 0x02:
            write(b"\x82")
        elif size == 0x03:
            write(b"\x83")
        elif size == 0x04:
            write(b"\x84")
        elif size == 0x05:
            write(b"\x85")
        elif size == 0x06:
            write(b"\x86")
        elif size == 0x07:
            write(b"\x87")
        elif size == 0x08:
            write(b"\x88")
        elif size == 0x09:
            write(b"\x89")
        elif size == 0x0A:
            write(b"\x8A")
        elif size == 0x0B:
            write(b"\x8B")
        elif size == 0x0C:
            write(b"\x8C")
        elif size == 0x0D:
            write(b"\x8D")
        elif size == 0x0E:
            write(b"\x8E")
        elif size == 0x0F:
            write(b"\x8F")
        elif size < 0x100:
            write(b"\xD0")
            write(PACKED_UINT_8[size])
        elif size < 0x10000:
            write(b"\xD1")
            write(PACKED_UINT_16[size])
        elif size < 0x100000000:
            write(b"\xD2")
            write(struct_pack(">I", size))
        else:
            raise ValueError("String too large")

    def pack_list_header(self, size):
        write = self._write
        if size == 0x00:
            write(b"\x90")
        elif size == 0x01:
            write(b"\x91")
        elif size == 0x02:
            write(b"\x92")
        elif size == 0x03:
            write(b"\x93")
        elif size == 0x04:
            write(b"\x94")
        elif size == 0x05:
            write(b"\x95")
        elif size == 0x06:
            write(b"\x96")
        elif size == 0x07:
            write(b"\x97")
        elif size == 0x08:
            write(b"\x98")
        elif size == 0x09:
            write(b"\x99")
        elif size == 0x0A:
            write(b"\x9A")
        elif size == 0x0B:
            write(b"\x9B")
        elif size == 0x0C:
            write(b"\x9C")
        elif size == 0x0D:
            write(b"\x9D")
        elif size == 0x0E:
            write(b"\x9E")
        elif size == 0x0F:
            write(b"\x9F")
        elif size < 0x100:
            write(b"\xD4")
            write(PACKED_UINT_8[size])
        elif size < 0x10000:
            write(b"\xD5")
            write(PACKED_UINT_16[size])
        elif size < 0x100000000:
            write(b"\xD6")
            write(struct_pack(">I", size))
        else:
            raise ValueError("List too large")

    def pack_map_header(self, size):
        write = self._write
        if size == 0x00:
            write(b"\xA0")
        elif size == 0x01:
            write(b"\xA1")
        elif size == 0x02:
            write(b"\xA2")
        elif size == 0x03:
            write(b"\xA3")
        elif size == 0x04:
            write(b"\xA4")
        elif size == 0x05:
            write(b"\xA5")
        elif size == 0x06:
            write(b"\xA6")
        elif size == 0x07:
            write(b"\xA7")
        elif size == 0x08:
            write(b"\xA8")
        elif size == 0x09:
            write(b"\xA9")
        elif size == 0x0A:
            write(b"\xAA")
        elif size == 0x0B:
            write(b"\xAB")
        elif size == 0x0C:
            write(b"\xAC")
        elif size == 0x0D:
            write(b"\xAD")
        elif size == 0x0E:
            write(b"\xAE")
        elif size == 0x0F:
            write(b"\xAF")
        elif size < 0x100:
            write(b"\xD8")
            write(PACKED_UINT_8[size])
        elif size < 0x10000:
            write(b"\xD9")
            write(PACKED_UINT_16[size])
        elif size < 0x100000000:
            write(b"\xDA")
            write(struct_pack(">I", size))
        else:
            raise ValueError("Map too large")

    def pack_struct(self, tag, fields):
        write = self._write
        size = len(fields)
        if size == 0x00:
            write(b"\xB0")
        elif size == 0x01:
            write(b"\xB1")
        elif size == 0x02:
            write(b"\xB2")
        elif size == 0x03:
            write(b"\xB3")
        elif size == 0x04:
            write(b"\xB4")
        elif size == 0x05:
            write(b"\xB5")
        elif size == 0x06:
            write(b"\xB6")
        elif size == 0x07:
            write(b"\xB7")
        elif size == 0x08:
            write(b"\xB8")
        elif size == 0x09:
            write(b"\xB9")
        elif size == 0x0A:
            write(b"\xBA")
        elif size == 0x0B:
            write(b"\xBB")
        elif size == 0x0C:
            write(b"\xBC")
        elif size == 0x0D:
            write(b"\xBD")
        elif size == 0x0E:
            write(b"\xBE")
        elif size == 0x0F:
            write(b"\xBF")
        else:
            raise ValueError("Structure too large")
        write(bytearray([tag]))
        for field in fields:
            self._pack(field)


class UnpackStream(object):

    def __init__(self, b):
        self._mem = memoryview(b)
        self._p = 0

    def unpack(self):
        if self._p < len(self._mem):
            marker = self._read_u8()
        else:
            raise ValueError("Nothing to unpack")  # TODO: better error

        # Tiny Integer
        if 0x00 <= marker <= 0x7F:
            return marker
        elif 0xF0 <= marker <= 0xFF:
            return marker - 0x100

        # Null
        elif marker == 0xC0:
            return None

        # Float
        elif marker == 0xC1:
            return self._read_f64be()

        # Boolean
        elif marker == 0xC2:
            return False
        elif marker == 0xC3:
            return True

        # Integer
        elif marker == 0xC8:
            return self._read_i8()
        elif marker == 0xC9:
            return self._read_i16be()
        elif marker == 0xCA:
            return self._read_i32be()
        elif marker == 0xCB:
            return self._read_i64be()

        # Bytes
        elif marker == 0xCC:
            size = self._read_u8()
            return self._read(size)
        elif marker == 0xCD:
            size = self._read_u16be()
            return self._read(size)
        elif marker == 0xCE:
            size = self._read_u32be()
            return self._read(size)

        else:
            marker_high = marker & 0xF0
            # String
            if marker_high == 0x80:  # TINY_STRING
                return decode(self._read(marker & 0x0F), "utf-8")
            elif marker == 0xD0:  # STRING_8:
                size = self._read_u8()
                return decode(self._read(size), "utf-8")
            elif marker == 0xD1:  # STRING_16:
                size = self._read_u16be()
                return decode(self._read(size), "utf-8")
            elif marker == 0xD2:  # STRING_32:
                size = self._read_u32be()
                return decode(self._read(size), "utf-8")

            # List
            elif 0x90 <= marker <= 0x9F or 0xD4 <= marker <= 0xD7:
                return list(self._unpack_list_items(marker))

            # Dictionary
            elif 0xA0 <= marker <= 0xAF or 0xD8 <= marker <= 0xDB:
                return self._unpack_dictionary(marker)

            # Structure
            elif 0xB0 <= marker <= 0xBF:
                size, tag = self._unpack_structure_header(marker)
                value = Structure(tag, *([None] * size))
                for i in range(len(value)):
                    value[i] = self.unpack()
                return value

            elif marker == 0xDF:  # END_OF_STREAM:
                return EndOfStream

            else:
                raise ValueError("Unknown PackStream marker %02X" % marker)

    def _unpack_list_items(self, marker):
        marker_high = marker & 0xF0
        if marker_high == 0x90:
            size = marker & 0x0F
            if size == 0:
                return
            elif size == 1:
                yield self.unpack()
            else:
                for _ in range(size):
                    yield self.unpack()
        elif marker == 0xD4:  # LIST_8:
            size = self._read_u8()
            for _ in range(size):
                yield self.unpack()
        elif marker == 0xD5:  # LIST_16:
            size = self._read_u16be()
            for _ in range(size):
                yield self.unpack()
        elif marker == 0xD6:  # LIST_32:
            size = self._read_u32be()
            for _ in range(size):
                yield self.unpack()
        elif marker == 0xD7:  # LIST_STREAM:
            item = None
            while item is not EndOfStream:
                item = self.unpack()
                if item is not EndOfStream:
                    yield item
        else:
            return

    def _unpack_dictionary(self, marker):
        marker_high = marker & 0xF0
        if marker_high == 0xA0:
            size = marker & 0x0F
            value = {}
            for _ in range(size):
                key = self.unpack()
                value[key] = self.unpack()
            return value
        elif marker == 0xD8:  # MAP_8:
            size = self._read_u8()
            value = {}
            for _ in range(size):
                key = self.unpack()
                value[key] = self.unpack()
            return value
        elif marker == 0xD9:  # MAP_16:
            size = self._read_u16be()
            value = {}
            for _ in range(size):
                key = self.unpack()
                value[key] = self.unpack()
            return value
        elif marker == 0xDA:  # MAP_32:
            size = self._read_u32be()
            value = {}
            for _ in range(size):
                key = self.unpack()
                value[key] = self.unpack()
            return value
        elif marker == 0xDB:  # MAP_STREAM:
            value = {}
            key = None
            while key is not EndOfStream:
                key = self.unpack()
                if key is not EndOfStream:
                    value[key] = self.unpack()
            return value
        else:
            return None

    def _unpack_structure_header(self, marker):
        marker_high = marker & 0xF0
        if marker_high == 0xB0:  # TINY_STRUCT
            signature = self._read_u8()
            return marker & 0x0F, signature
        else:
            raise ValueError("Expected structure, found marker %02X" % marker)

    def _read(self, n=1):
        q = self._p + n
        m = self._mem[self._p:q]
        self._p = q
        return m.tobytes()

    def _read_u8(self):
        q = self._p + 1
        n, = struct_unpack(">B", self._mem[self._p:q])
        self._p = q
        return n

    def _read_u16be(self):
        q = self._p + 2
        n, = struct_unpack(">H", self._mem[self._p:q])
        self._p = q
        return n

    def _read_u32be(self):
        q = self._p + 4
        n, = struct_unpack(">I", self._mem[self._p:q])
        self._p = q
        return n

    def _read_i8(self):
        q = self._p + 1
        z, = struct_unpack(">b", self._mem[self._p:q])
        self._p = q
        return z

    def _read_i16be(self):
        q = self._p + 2
        z, = struct_unpack(">h", self._mem[self._p:q])
        self._p = q
        return z

    def _read_i32be(self):
        q = self._p + 4
        z, = struct_unpack(">i", self._mem[self._p:q])
        self._p = q
        return z

    def _read_i64be(self):
        q = self._p + 8
        z, = struct_unpack(">q", self._mem[self._p:q])
        self._p = q
        return z

    def _read_f64be(self):
        q = self._p + 8
        r, = struct_unpack(">d", self._mem[self._p:q])
        self._p = q
        return r


def packed(*values):
    from io import BytesIO
    b = BytesIO()
    packer = Packer(b)
    for value in values:
        packer.pack(value)
    return b.getvalue()


class MessageReader(object):

    def __init__(self, wire):
        self.wire = wire

    def _read_chunk(self):
        size, = struct_unpack(">H", self.wire.read(2))
        if size:
            return self.wire.read(size)
        else:
            return b""

    def read_message(self):
        chunks = []
        more = True
        while more:
            chunk = self._read_chunk()
            if chunk:
                chunks.append(chunk)
            elif chunks:
                more = False
        try:
            message = b"".join(chunks)
        except TypeError:
            # Python 2 compatibility
            message = bytearray(b"".join(map(bytes, chunks)))
        _, n = divmod(message[0], 0x10)
        tag = message[1]
        unpacker = UnpackStream(message[2:])
        fields = tuple(unpacker.unpack() for _ in range(n))
        return tag, fields


class MessageWriter(object):

    def __init__(self, wire):
        self.wire = wire

    def _write_chunk(self, data):
        size = len(data)
        self.wire.write(struct_pack(">H", size))
        self.wire.write(data)

    def write_message(self, tag, *fields):
        data = bytearray([0xB0 + len(fields), tag])
        for field in fields:
            data.extend(packed(field))
        for offset in range(0, len(data), 32767):
            end = offset + 32767
            self._write_chunk(data[offset:end])
        self._write_chunk(b"")

    def send(self):
        return self.wire.send()


class PackStreamHydrant(Hydrant):

    def __init__(self, graph):
        self.graph = graph

    def hydrate(self, keys, values, entities=None, version=None):
        """ Convert PackStream values into native values.
        """
        if version is None:
            v = (1, 0)
        elif isinstance(version, tuple):
            v = version
        else:
            v = (version, 0)
        if entities is None:
            entities = {}
        return tuple(self._hydrate(value, entities.get(keys[i]), v)
                     for i, value in enumerate(values))

    def _hydrate(self, obj, inst=None, version=None):
        from neotime import Duration, Date, Time, DateTime
        from pytz import FixedOffset, timezone
        from py2neo.data import Node, Relationship, Path
        from py2neo.data.spatial import Point

        unbound_relationship = namedtuple("UnboundRelationship", ["id", "type", "properties"])
        unix_epoch_date = Date(1970, 1, 1)
        unix_epoch_date_ordinal = unix_epoch_date.to_ordinal()

        def hydrate_object(o):
            if isinstance(o, Structure):
                tag = o.tag if isinstance(o.tag, bytes) else bytes(bytearray([o.tag]))
                try:
                    f = functions[tag]
                except KeyError:
                    # If we don't recognise the structure type, just return it as-is
                    return o
                else:
                    return f(*o.fields)
            elif isinstance(o, list):
                return list(map(hydrate_object, o))
            elif isinstance(o, dict):
                return {key: hydrate_object(value) for key, value in o.items()}
            else:
                return o

        def hydrate_node(identity, labels, properties):
            return Node.hydrate(self.graph, identity, labels, hydrate_object(properties), into=inst)

        def hydrate_relationship(identity, start_node_id, end_node_id, r_type, properties):
            return Relationship.hydrate(self.graph, identity, start_node_id, end_node_id,
                                        r_type, hydrate_object(properties), into=inst)

        def hydrate_path(nodes, relationships, sequence):
            nodes = [Node.hydrate(self.graph, n_id, n_label, hydrate_object(n_properties))
                     for n_id, n_label, n_properties in nodes]
            u_rels = []
            for r_id, r_type, r_properties in relationships:
                u_rel = unbound_relationship(r_id, r_type, hydrate_object(r_properties))
                u_rels.append(u_rel)
            return Path.hydrate(self.graph, nodes, u_rels, sequence)

        def hydrate_date(days):
            """ Hydrator for `Date` values.

            :param days:
            :return: Date
            """
            return Date.from_ordinal(unix_epoch_date_ordinal + days)

        def hydrate_time(nanoseconds, tz=None):
            """ Hydrator for `Time` and `LocalTime` values.

            :param nanoseconds:
            :param tz:
            :return: Time
            """
            seconds, nanoseconds = map(int, divmod(nanoseconds, 1000000000))
            minutes, seconds = map(int, divmod(seconds, 60))
            hours, minutes = map(int, divmod(minutes, 60))
            seconds = (1000000000 * seconds + nanoseconds) / 1000000000
            t = Time(hours, minutes, seconds)
            if tz is None:
                return t
            tz_offset_minutes, tz_offset_seconds = divmod(tz, 60)
            zone = FixedOffset(tz_offset_minutes)
            return zone.localize(t)

        def hydrate_datetime(seconds, nanoseconds, tz=None):
            """ Hydrator for `DateTime` and `LocalDateTime` values.

            :param seconds:
            :param nanoseconds:
            :param tz:
            :return: datetime
            """
            minutes, seconds = map(int, divmod(seconds, 60))
            hours, minutes = map(int, divmod(minutes, 60))
            days, hours = map(int, divmod(hours, 24))
            seconds = (1000000000 * seconds + nanoseconds) / 1000000000
            t = DateTime.combine(Date.from_ordinal(unix_epoch_date_ordinal + days), Time(hours, minutes, seconds))
            if tz is None:
                return t
            if isinstance(tz, int):
                tz_offset_minutes, tz_offset_seconds = divmod(tz, 60)
                zone = FixedOffset(tz_offset_minutes)
            else:
                zone = timezone(tz)
            return zone.localize(t)

        def hydrate_duration(months, days, seconds, nanoseconds):
            """ Hydrator for `Duration` values.

            :param months:
            :param days:
            :param seconds:
            :param nanoseconds:
            :return: `duration` namedtuple
            """
            return Duration(months=months, days=days, seconds=seconds, nanoseconds=nanoseconds)

        def hydrate_point(srid, *coordinates):
            """ Create a new instance of a Point subclass from a raw
            set of fields. The subclass chosen is determined by the
            given SRID; a ValueError will be raised if no such
            subclass can be found.
            """
            try:
                point_class, dim = Point.class_for_srid(srid)
            except KeyError:
                point = Point(coordinates)
                point.srid = srid
                return point
            else:
                if len(coordinates) != dim:
                    raise ValueError("SRID %d requires %d coordinates (%d provided)" % (srid, dim, len(coordinates)))
                return point_class(coordinates)

        functions = {
            b"N": hydrate_node,
            b"R": hydrate_relationship,
            b"P": hydrate_path,
        }
        if version >= (2, 0):
            functions.update({
                b"D": hydrate_date,
                b"T": hydrate_time,         # time zone offset
                b"t": hydrate_time,         # no time zone
                b"F": hydrate_datetime,     # time zone offset
                b"f": hydrate_datetime,     # time zone name
                b"d": hydrate_datetime,     # no time zone
                b"E": hydrate_duration,
                b"X": hydrate_point,
                b"Y": hydrate_point,
            })

        return hydrate_object(obj)

    def dehydrate(self, data, version=None):
        """ Dehydrate to PackStream.
        """
        from datetime import date, time, datetime, timedelta
        from neotime import Duration, Date, Time, DateTime
        from pytz import utc
        from py2neo.data.spatial import Point

        unix_epoch_date = Date(1970, 1, 1)

        if version is None:
            v = (1, 0)
        elif isinstance(version, tuple):
            v = version
        else:
            v = (version, 0)

        def dehydrate_object(x):
            t = type(x)
            if t in functions:
                f = functions[t]
                return f(x)
            elif x is None or x is True or x is False or isinstance(x, float) or isinstance(x, string_types):
                return x
            elif isinstance(x, integer_types):
                if x < INT64_MIN or x > INT64_MAX:
                    raise ValueError("Integers must be within the signed 64-bit range")
                return x
            elif isinstance(x, bytearray):
                return x
            elif isinstance(x, Mapping):
                d = {}
                for key in x:
                    if not isinstance(key, string_types):
                        raise TypeError("Dictionary keys must be strings")
                    d[key] = dehydrate_object(x[key])
                return d
            elif isinstance(x, Sequence):
                return list(map(dehydrate_object, x))
            else:
                raise TypeError("PackStream parameters of type %s are not supported" % type(x).__name__)

        def dehydrate_date(value):
            """ Dehydrator for `date` values.

            :param value:
            :type value: Date
            :return:
            """
            return Structure(ord(b"D"), value.toordinal() - unix_epoch_date.toordinal())

        def dehydrate_time(value):
            """ Dehydrator for `time` values.

            :param value:
            :type value: Time
            :return:
            """
            if isinstance(value, Time):
                nanoseconds = int(value.ticks * 1000000000)
            elif isinstance(value, time):
                nanoseconds = (3600000000000 * value.hour + 60000000000 * value.minute +
                               1000000000 * value.second + 1000 * value.microsecond)
            else:
                raise TypeError("Value must be a neotime.Time or a datetime.time")
            if value.tzinfo:
                return Structure(ord(b"T"), nanoseconds, value.tzinfo.utcoffset(value).seconds)
            else:
                return Structure(ord(b"t"), nanoseconds)

        def dehydrate_datetime(value):
            """ Dehydrator for `datetime` values.

            :param value:
            :type value: datetime
            :return:
            """

            def seconds_and_nanoseconds(dt):
                if isinstance(dt, datetime):
                    dt = DateTime.from_native(dt)
                zone_epoch = DateTime(1970, 1, 1, tzinfo=dt.tzinfo)
                t = dt.to_clock_time() - zone_epoch.to_clock_time()
                return t.seconds, t.nanoseconds

            tz = value.tzinfo
            if tz is None:
                # without time zone
                value = utc.localize(value)
                seconds, nanoseconds = seconds_and_nanoseconds(value)
                return Structure(ord(b"d"), seconds, nanoseconds)
            elif hasattr(tz, "zone") and tz.zone:
                # with named time zone
                seconds, nanoseconds = seconds_and_nanoseconds(value)
                return Structure(ord(b"f"), seconds, nanoseconds, tz.zone)
            else:
                # with time offset
                seconds, nanoseconds = seconds_and_nanoseconds(value)
                return Structure(ord(b"F"), seconds, nanoseconds, tz.utcoffset(value).seconds)

        def dehydrate_duration(value):
            """ Dehydrator for `duration` values.

            :param value:
            :type value: Duration
            :return:
            """
            return Structure(ord(b"E"), value.months, value.days, value.seconds, int(1000000000 * value.subseconds))

        def dehydrate_timedelta(value):
            """ Dehydrator for `timedelta` values.

            :param value:
            :type value: timedelta
            :return:
            """
            months = 0
            days = value.days
            seconds = value.seconds
            nanoseconds = 1000 * value.microseconds
            return Structure(ord(b"E"), months, days, seconds, nanoseconds)

        def dehydrate_point(value):
            """ Dehydrator for Point data.

            :param value:
            :type value: Point
            :return:
            """
            dim = len(value)
            if dim == 2:
                return Structure(ord(b"X"), value.srid, *value)
            elif dim == 3:
                return Structure(ord(b"Y"), value.srid, *value)
            else:
                raise ValueError("Cannot dehydrate Point with %d dimensions" % dim)

        functions = {}  # graph types cannot be used as parameters
        if v >= (2, 0):
            functions.update({
                Date: dehydrate_date,
                date: dehydrate_date,
                Time: dehydrate_time,
                time: dehydrate_time,
                DateTime: dehydrate_datetime,
                datetime: dehydrate_datetime,
                Duration: dehydrate_duration,
                timedelta: dehydrate_timedelta,
                Point: dehydrate_point,
            })
            functions.update({
                cls: dehydrate_point
                for cls in Point.__subclasses__()
            })

        return dehydrate_object(data)
