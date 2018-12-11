#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2018, Nigel Small
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


from __future__ import absolute_import

try:
    from configparser import SafeConfigParser
except ImportError:
    from ConfigParser import SafeConfigParser

try:
    from collections.abc import Sequence, Set, Mapping
except ImportError:
    from collections import Sequence, Set, Mapping

try:
    from urllib.parse import urlparse, urlsplit
except ImportError:
    from urlparse import urlparse, urlsplit

try:
    from urllib.request import urlretrieve
except ImportError:
    from urllib import urlretrieve

from io import StringIO
import os
from sys import version_info


if version_info >= (3,):
    # Python 3

    atomic_types = (bool, bytearray, bytes, float, int, str)
    bytes_types = (bytearray, bytes)
    integer_types = (int,)
    list_types = (list, map)
    numeric_types = (int, float)
    string_types = (bytes, str)
    unicode_types = (str,)
    utf8_types = ()

    long = int
    uchr = chr

    def bstr(s, encoding="utf-8"):
        """ Convert a value to a byte string, held in a Python `bytearray` object.
        """
        if isinstance(s, bytearray):
            return s
        elif isinstance(s, bytes):
            return bytearray(s)
        elif isinstance(s, str):
            return bytearray(s.encode(encoding=encoding))
        else:
            try:
                return bytearray(s.__bytes__())
            except AttributeError:
                return bytearray(str(s).encode(encoding=encoding))

    def ustr(s, encoding="utf-8"):
        """ Convert a value to a Unicode string, held in a Python `str` object.
        """
        if isinstance(s, str):
            return s
        elif isinstance(s, (bytes, bytearray)):
            return s.decode(encoding=encoding)
        else:
            try:
                return s.__str__()
            except AttributeError:
                return str(s, encoding=encoding)

    def xstr(s, encoding="utf-8"):
        """ Convert argument to string type returned by __str__.
        """
        if isinstance(s, str):
            return s
        elif isinstance(s, bytes):
            return s.decode(encoding)
        else:
            return str(s)

    class PropertiesParser(SafeConfigParser):

        def read_properties(self, filename, section=None):
            if not section:
                basename = os.path.basename(filename)
                if basename.endswith(".properties"):
                    section = basename[:-11]
                else:
                    section = basename
            with open(filename) as f:
                data = f.read()
            self.read_string("[%s]\n%s" % (section, data), filename)

else:
    # Python 2

    atomic_types = (bool, bytearray, float, int, long, str, unicode)
    bytes_types = (bytearray,)
    integer_types = (int, long)
    list_types = (list,)
    numeric_types = (int, long, float)
    string_types = (str, unicode)
    unicode_types = (unicode,)
    utf8_types = (str,)

    long = long
    uchr = unichr

    def bstr(s, encoding="utf-8"):
        """ Convert a value to byte string, held in a Python `bytearray` object.
        """
        if isinstance(s, bytearray):
            return s
        elif isinstance(s, bytes):
            return bytearray(s)
        elif isinstance(s, unicode):
            return bytearray(s.encode(encoding=encoding))
        else:
            try:
                return bytearray(s.__bytes__())
            except AttributeError:
                return bytearray(unicode(s).encode(encoding=encoding))

    def ustr(s, encoding="utf-8"):
        """ Convert a value to a Unicode string, held in a Python `unicode` object.
        """
        if isinstance(s, unicode):
            return s
        elif isinstance(s, (bytes, bytearray)):
            return s.decode(encoding=encoding)
        else:
            try:
                return s.__unicode__()
            except AttributeError:
                return str(s).decode(encoding=encoding)

    def xstr(s, encoding="utf-8"):
        """ Convert argument to string type returned by __str__.
        """
        if isinstance(s, str):
            return s
        else:
            return unicode(s).encode(encoding)

    class PropertiesParser(SafeConfigParser):

        def read_properties(self, filename, section=None):
            if not section:
                basename = os.path.basename(filename)
                if basename.endswith(".properties"):
                    section = basename[:-11]
                else:
                    section = basename
            data = StringIO()
            data.write("[%s]\n" % section)
            with codecs.open(filename, encoding="utf-8") as f:
                data.write(f.read())
            data.seek(0, os.SEEK_SET)
            self.readfp(data)
