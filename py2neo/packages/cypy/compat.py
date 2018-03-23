#!/usr/bin/env python
# coding: utf-8

# Copyright 2002-2018, Neo4j
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


try:
    unicode

except NameError:

    # Python 3
    atomic_types = (bool, bytearray, bytes, float, int, str)

    bytes_types = (bytearray, bytes)
    integer_types = (int,)
    unicode_types = (str,)
    utf8_types = ()

    def bstr(value, encoding="utf-8"):
        """ Convert a value to a byte string, held in a Python `bytearray` object.
        """
        if isinstance(value, bytearray):
            return value
        elif isinstance(value, bytes):
            return bytearray(value)
        elif isinstance(value, str):
            return bytearray(value.encode(encoding=encoding))
        else:
            try:
                return bytearray(value.__bytes__())
            except AttributeError:
                return bytearray(str(value).encode(encoding=encoding))

    def ustr(value, encoding="utf-8"):
        """ Convert a value to a Unicode string, held in a Python `str` object.
        """
        if isinstance(value, str):
            return value
        elif isinstance(value, (bytes, bytearray)):
            return value.decode(encoding=encoding)
        else:
            try:
                return value.__str__()
            except AttributeError:
                return str(value, encoding=encoding)

else:

    # Python 2
    atomic_types = (bool, bytearray, float, int, long, str, unicode)

    bytes_types = (bytearray,)
    integer_types = (int, long)
    unicode_types = (unicode,)
    utf8_types = (str,)

    def bstr(value, encoding="utf-8"):
        """ Convert a value to byte string, held in a Python `bytearray` object.
        """
        if isinstance(value, bytearray):
            return value
        elif isinstance(value, bytes):
            return bytearray(value)
        elif isinstance(value, unicode):
            return bytearray(value.encode(encoding=encoding))
        else:
            try:
                return bytearray(value.__bytes__())
            except AttributeError:
                return bytearray(unicode(value).encode(encoding=encoding))

    def ustr(value, encoding="utf-8"):
        """ Convert a value to a Unicode string, held in a Python `unicode` object.
        """
        if isinstance(value, unicode):
            return value
        elif isinstance(value, (bytes, bytearray)):
            return value.decode(encoding=encoding)
        else:
            try:
                return value.__unicode__()
            except AttributeError:
                return str(value).decode(encoding=encoding)
