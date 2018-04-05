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


from click import secho

from py2neo.cypher.writing import cypher_str, cypher_repr
from py2neo.internal.compat import ustr, integer_types, string_types


def measure_cypher_str(value):
    """ Measure the display size of a value after conversion into a
    Cypher string.
    """
    lines = cypher_str(value).splitlines(False)
    return max(map(len, lines)) if lines else 0, len(lines)


class DataTable(list):
    """ A fixed table of data.
    """

    def __init__(self, records, keys=None):
        super(DataTable, self).__init__(map(tuple, records))
        if keys is None:
            try:
                k = records.keys()
            except AttributeError:
                raise ValueError("Missing keys")
        else:
            k = list(map(ustr, keys))
        width = len(k)
        t = [set() for _ in range(width)]
        o = [False] * width
        w = [0] * width
        for record in self:
            for i, value in enumerate(record):
                if value is None:
                    o[i] = True
                else:
                    t[i].add(type(value))
                    w0, h0 = measure_cypher_str(value)
                    if w0 > w[i]:
                        w[i] = w0
        f = []
        for i, _ in enumerate(k):
            f.append({
                "type": t[i].pop() if len(t[i]) == 1 else tuple(t[i]),
                "optional": o[i],
                "width": w[i],
            })
        self._keys = k
        self._fields = f

    def keys(self):
        return list(self._keys)

    def field(self, key):
        if isinstance(key, integer_types):
            return self._fields[key]
        elif isinstance(key, string_types):
            try:
                index = self._keys.index(key)
            except ValueError:
                raise KeyError(key)
            else:
                return self._fields[index]
        else:
            raise TypeError(key)

    def write_separated_values(self, separator, file=None, header=None, limit=None, newline="\r\n", quote="\""):
        """ Write the data to a separated file.

        :param separator:
        :param file
        :param header:
        :param limit:
        :param newline:
        :param quote:
        :return:
        """
        escaped_quote = quote + quote
        quotable = separator + newline + quote
        header_styles = {}
        if header and isinstance(header, dict):
            header_styles.update(header)

        def write_value(value, **styles):
            if value is None:
                return
            if isinstance(value, string_types):
                value = ustr(value)
                if any(ch in value for ch in quotable):
                    value = quote + value.replace(quote, escaped_quote) + quote
            else:
                value = cypher_repr(value)
            secho(value, file, nl=False, **styles)

        def write_line(values, **styles):
            for i, value in enumerate(values):
                if i > 0:
                    secho(separator, file, nl=False, **styles)
                write_value(value, **styles)
            secho(newline, file, nl=False, **styles)

        if header:
            write_line(self.keys(), **header_styles)
        count = 0
        for count, record in enumerate(self, start=1):
            write_line(record)
            if count == limit:
                break
        return count

    def write_csv(self, file=None, header=None, limit=None):
        """ Write the data as RFC4180-compatible comma-separated values.

        :param file
        :param header:
        :param limit:
        :return:
        """
        return self.write_separated_values(",", file, header, limit)

    def write_tsv(self, file=None, header=None, limit=None):
        """ Write the data as tab-separated values.

        :param file
        :param header:
        :param limit:
        :return:
        """
        return self.write_separated_values("\t", file, header, limit)
