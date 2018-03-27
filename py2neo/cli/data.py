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


import sys

import click
from py2neo.cypher.encoding import cypher_repr, cypher_str

from .table import Table


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
        return table.size()


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
        if isinstance(value, STRING):
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
        if isinstance(value, STRING):
            click.echo(cypher_repr(value, quote=u'"'), nl=False)
        else:
            click.echo(cypher_str(value), nl=False)
