#!/usr/bin/env python
# coding: utf-8

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


import click

from py2neo.internal.compat import numeric_types, ustr


class Table(object):

    @classmethod
    def stringify(cls, value):
        if value is None:
            return u""
        elif isinstance(value, bool):
            return u"true" if value else u"false"
        elif isinstance(value, float):
            return u"{:.02f}".format(value)
        else:
            return ustr(value)

    @classmethod
    def calc_size(cls, value):
        lines = cls.stringify(value).splitlines(False)
        width = max(map(len, lines)) if lines else 0
        height = len(lines)
        return width, height

    def __init__(self, keys, padding=1, field_separator=u"|", auto_align=True, header=1):
        self._keys = keys
        self._widths = []
        for key in keys:
            width, _ = self.calc_size(key)
            self._widths.append(width)
        self._padding = padding
        self._field_separator = field_separator
        self._auto_align = auto_align
        self._header = header
        self._rows = []

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
        lines = self._table.stringify(value).splitlines(False)
        width, height = self._table.calc_size(value)
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
                if self._auto_align and (self._types[column] in numeric_types):
                    justified_text = text.rjust(self._table.widths[column])
                else:
                    justified_text = text.ljust(self._table.widths[column])
                final_text = padding + justified_text + padding
                if column == last_column:
                    final_text = final_text.rstrip()
                click.secho(final_text, nl=False, **style)
            click.echo(u"\r\n", nl=False)


class ResultWriter(object):

    def write_header(self, result):
        """ Write a header for `result.

        :param result: data source
        """
        pass

    def write(self, result, limit):
        """ Write up to `limit` records from `result`.

        :param result: data source
        :param limit: maximum number of elements to write
        :returns: number of elements written
        """
        pass


class TabularResultWriter(ResultWriter):

    def write(self, result, limit):
        table = Table(result.keys())
        for count, record in enumerate(result, start=1):
            table.append(record.values())
            if count == limit:
                break
        table.echo(header_style={"fg": "cyan", "bold": True})
        click.echo()
        return table.calc_size()


class CSVResultWriter(ResultWriter):

    def write_header(self, result):
        click.secho(u",".join(result.keys()), nl=False, fg="cyan", bold=True)
        click.echo(u"\r\n", nl=False)

    def write(self, result, limit):
        count = 0
        for count, record in enumerate(result, start=1):
            self.write_record(record)
            if count == limit:
                break
        return count

    def write_record(self, record):
        for i, value in enumerate(record.values()):
            if i > 0:
                click.echo(u",", nl=False)
            self.write_value(value)
        click.echo(u"\r\n", nl=False)

    def write_value(self, value):
        if value is None:
            return
        if isinstance(value, unicode_types):
            if u',' in value or u'"' in value or u"\r" in value or u"\n" in value:
                escaped_value = u'"' + value.replace(u'"', u'""') + u'"'
                click.echo(escaped_value, nl=False)
            else:
                click.echo(cypher_repr(value, quote=u'"'), nl=False)
        else:
            click.echo(cypher_str(value), nl=False)


class TSVResultWriter(ResultWriter):

    def write_header(self, result):
        click.secho(u"\t".join(result.keys()), nl=False, fg="cyan", bold=True)
        click.echo(u"\r\n", nl=False)

    def write(self, result, limit):
        count = 0
        for count, record in enumerate(result, start=1):
            self.write_record(record)
            if count == limit:
                break
        return count

    def write_record(self, record):
        for i, value in enumerate(record.values()):
            if i > 0:
                click.echo(u"\t", nl=False)
            self.write_value(value)
        click.echo(u"\r\n", nl=False)

    def write_value(self, value):
        if isinstance(value, unicode_types):
            click.echo(cypher_repr(value, quote=u'"'), nl=False)
        else:
            click.echo(cypher_str(value), nl=False)
