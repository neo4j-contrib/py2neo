#!/usr/bin/env python
# coding: utf-8

# Copyright 2011-2017, Nigel Small
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


import click
import sys

if sys.version_info >= (3,):
    BOOLEAN = bool
    INTEGER = int
    FLOAT = float
    BYTES = (bytes, bytearray)
    STRING = str
    LIST = list
    MAP = dict
else:
    BOOLEAN = bool
    INTEGER = (int, long)
    FLOAT = float
    BYTES = bytearray
    STRING = unicode
    LIST = list
    MAP = dict


class TableValueSystem(object):

    NULL = u""
    TRUE = u"true"
    FALSE = u"false"

    def encode(self, value):
        if value is None:
            return self.NULL
        elif isinstance(value, BOOLEAN):
            return self.TRUE if value else self.FALSE
        elif isinstance(value, FLOAT):
            return u"{:.02f}".format(value)
        else:
            return STRING(value)

    def size(self, value):
        lines = self.encode(value).splitlines(False)
        width = max(map(len, lines)) if lines else 0
        height = len(lines)
        return width, height


class Table(object):

    def __init__(self, keys, padding=1, field_separator=u"|", auto_align=True, header=1):
        self._value_system = TableValueSystem()
        self._keys = keys
        self._widths = []
        for key in keys:
            width, _ = self._value_system.size(key)
            self._widths.append(width)
        self._padding = padding
        self._field_separator = field_separator
        self._auto_align = auto_align
        self._header = header
        self._rows = []

    @property
    def value_system(self):
        return self._value_system

    @property
    def widths(self):
        return self._widths

    def size(self):
        return len(self._rows)

    def append(self, values):
        row = TableRow(self, self._padding, self._field_separator, self._auto_align)
        for column, value in enumerate(values):
            row.put(column, value)
        self._rows.append(row)

    def echo(self, header_style):
        if self._header:
            header_row = TableRow(self, self._padding, self._field_separator, self._auto_align)
            for column, key in enumerate(self._keys):
                header_row.put(column, key)
            header_row.echo(**header_style)
            click.secho(self._field_separator.join(u"-" * (self._widths[i] + 2 * self._padding)
                                                   for i, key in enumerate(self._keys)),
                        nl=False)
            click.echo(u"\r\n", nl=False)
        for row in self._rows:
            row.echo()


class TableRow(object):

    def __init__(self, table, padding=1, field_separator=u"|", auto_align=True):
        self._table = table
        self._padding = padding
        self._field_separator = field_separator
        self._auto_align = auto_align
        self._types = [None for _ in self._table.widths]
        self._lines = [[u"" for _ in self._table.widths]]

    def put(self, column, value):
        self._types[column] = type(value)
        lines = self._table.value_system.encode(value).splitlines(False)
        width, height = self._table.value_system.size(value)
        self._table.widths[column] = max(width, self._table.widths[column])
        while height > len(self._lines):
            self._lines.append([u"" for _ in self._table.widths])
        for row, line in enumerate(lines):
            self._lines[row][column] = line

    def echo(self, **style):
        padding = u" " * self._padding
        for line in self._lines:
            last_column = len(line) - 1
            for column, text in enumerate(line):
                if column > 0:
                    click.secho(self._field_separator, nl=False)
                if self._auto_align and (self._types[column] == INTEGER or self._types[column] == FLOAT):
                    justified_text = text.rjust(self._table.widths[column])
                else:
                    justified_text = text.ljust(self._table.widths[column])
                final_text = padding + justified_text + padding
                if column == last_column:
                    final_text = final_text.rstrip()
                click.secho(final_text, nl=False, **style)
            click.echo(u"\r\n", nl=False)
