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


from codecs import decode
from collections import namedtuple
from struct import pack as struct_pack, unpack as struct_unpack

from py2neo.internal.compat import Sequence, Mapping, bytes_types, integer_types, string_types, bstr


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
                raise OverflowError("Integer %s out of range" % value)

        # String
        elif isinstance(value, string_types):
            encoded = bstr(value)
            self.pack_string_header(len(encoded))
            self.pack_raw(encoded)

        # Bytes
        elif isinstance(value, bytes_types):
            self.pack_bytes_header(len(value))
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
            raise ValueError("Values of type %s are not supported" % type(value))

    def pack_bytes_header(self, size):
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
            raise OverflowError("Bytes header size out of range")

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
            raise OverflowError("String header size out of range")

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
            raise OverflowError("List header size out of range")

    def pack_list_stream_header(self):
        self._write(b"\xD7")

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
            raise OverflowError("Map header size out of range")

    def pack_map_stream_header(self):
        self._write(b"\xDB")

    def pack_struct(self, signature, fields):
        if len(signature) != 1 or not isinstance(signature, bytes):
            raise ValueError("Structure signature must be a single byte value")
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
            raise OverflowError("Structure size out of range")
        write(signature)
        for field in fields:
            self._pack(field)

    def pack_end_of_stream(self):
        self._write(b"\xDF")


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

    def __init__(self, rx):
        self.rx = rx

    def _read_chunk(self):
        size, = struct_unpack(">H", self.rx.read(2))
        if size:
            return self.rx.read(size)
        else:
            return b""

    def read_message(self):
        message = bytearray()
        more = True
        while more:
            chunk = self._read_chunk()
            if chunk:
                message.extend(chunk)
            else:
                more = False
        _, n = divmod(message[0], 0x10)
        tag = message[1]
        unpacker = UnpackStream(message[2:])
        fields = tuple(unpacker.unpack() for _ in range(n))
        return tag, fields


class MessageWriter(object):

    def __init__(self, tx):
        self._tx = tx

    def _write_chunk(self, data):
        size = len(data)
        self._tx.write(struct_pack(">H", size))
        self._tx.write(data)

    def write_message(self, tag, *fields):
        data = bytearray([0xB0 + len(fields), tag])
        for field in fields:
            data.extend(packed(field))
        for offset in range(0, len(data), 32767):
            end = offset + 32767
            self._write_chunk(data[offset:end])
        self._write_chunk(b"")

    def send(self):
        return self._tx.send()


class PackStreamHydrator(object):

    unbound_relationship = namedtuple("UnboundRelationship", ["id", "type", "properties"])

    def __init__(self, version, graph, entities=None):
        self.graph = graph
        self.version = version if isinstance(version, tuple) else (version, 0)
        self.entities = entities or {}
        self.hydration_functions = {}
        self.dehydration_functions = {}
        if self.version >= (2, 0):
            from py2neo.internal.hydration.spatial import (
                hydration_functions as spatial_hydration_functions,
                dehydration_functions as spatial_dehydration_functions,
            )
            from py2neo.internal.hydration.temporal import (
                hydration_functions as temporal_hydration_functions,
                dehydration_functions as temporal_dehydration_functions,
            )
            self.hydration_functions.update(temporal_hydration_functions())
            self.hydration_functions.update(spatial_hydration_functions())
            self.dehydration_functions.update(temporal_dehydration_functions())
            self.dehydration_functions.update(spatial_dehydration_functions())

    def hydrate(self, keys, values):
        """ Convert PackStream values into native values.
        """
        return tuple(self._hydrate_object(value, self.entities.get(keys[i]))
                     for i, value in enumerate(values))

    def _hydrate_object(self, obj, inst=None):
        from py2neo.data import Node, Relationship
        if isinstance(obj, Structure):
            tag = obj.tag if isinstance(obj.tag, bytes) else bytes(bytearray([obj.tag]))
            fields = obj.fields
            if tag == b"N":
                return Node.hydrate(self.graph, fields[0], fields[1], self._hydrate_object(fields[2]), into=inst)
            elif tag == b"R":
                return Relationship.hydrate(self.graph, fields[0], fields[1], fields[2], fields[3],
                                            self._hydrate_object(fields[4]), into=inst)
            elif tag == b"P":
                return self._hydrate_path(*fields)
            else:
                try:
                    f = self.hydration_functions[tag]
                except KeyError:
                    # If we don't recognise the structure type, just return it as-is
                    return obj
                else:
                    return f(*map(self._hydrate_object, obj.fields))
        elif isinstance(obj, list):
            return list(map(self._hydrate_object, obj))
        elif isinstance(obj, dict):
            return {key: self._hydrate_object(value) for key, value in obj.items()}
        else:
            return obj

    def _hydrate_path(self, nodes, relationships, sequence):
        from py2neo.data import Node, Path
        nodes = [Node.hydrate(self.graph, n_id, n_label, self._hydrate_object(n_properties))
                 for n_id, n_label, n_properties in nodes]
        u_rels = []
        for r_id, r_type, r_properties in relationships:
            u_rel = self.unbound_relationship(r_id, r_type, self._hydrate_object(r_properties))
            u_rels.append(u_rel)
        return Path.hydrate(self.graph, nodes, u_rels, sequence)

    def dehydrate(self, data):
        """ Dehydrate to PackStream.
        """
        t = type(data)
        if t in self.dehydration_functions:
            f = self.dehydration_functions[t]
            return f(data)
        elif data is None or data is True or data is False or isinstance(data, float) or isinstance(data, string_types):
            return data
        elif isinstance(data, integer_types):
            if data < INT64_MIN or data > INT64_MAX:
                raise ValueError("Integers must be within the signed 64-bit range")
            return data
        elif isinstance(data, bytearray):
            return data
        elif isinstance(data, Mapping):
            d = {}
            for key in data:
                if not isinstance(key, string_types):
                    raise TypeError("Dictionary keys must be strings")
                d[key] = self.dehydrate(data[key])
            return d
        elif isinstance(data, Sequence):
            return list(map(self.dehydrate, data))
        else:
            raise TypeError("Neo4j does not support PackStream parameters of type %s" % type(data).__name__)
