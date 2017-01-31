#!/usr/bin/env python
# -*- encoding: utf-8 -*-

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


from os import write as os_write
from sys import stdin, stdout, stderr

from py2neo.compat import number, unicode


NATURAL = u"\x1b[0m"

BLACK = u"\x1b[30m"
RED = u"\x1b[31m"
GREEN = u"\x1b[32m"
YELLOW = u"\x1b[33m"
BLUE = u"\x1b[34m"
MAGENTA = u"\x1b[35m"
CYAN = u"\x1b[36m"
WHITE = u"\x1b[37m"

BRIGHT_BLACK = u"\x1b[30;1m"
BRIGHT_RED = u"\x1b[31;1m"
BRIGHT_GREEN = u"\x1b[32;1m"
BRIGHT_YELLOW = u"\x1b[33;1m"
BRIGHT_BLUE = u"\x1b[34;1m"
BRIGHT_MAGENTA = u"\x1b[35;1m"
BRIGHT_CYAN = u"\x1b[36;1m"
BRIGHT_WHITE = u"\x1b[37;1m"


def colour(s, code):
    assert isinstance(s, unicode)
    assert isinstance(code, unicode)
    return code + s + NATURAL


def black(s):
    return colour(s, BLACK)


def red(s):
    return colour(s, RED)


def green(s):
    return colour(s, GREEN)


def yellow(s):
    return colour(s, YELLOW)


def blue(s):
    return colour(s, BLUE)


def magenta(s):
    return colour(s, MAGENTA)


def cyan(s):
    return colour(s, CYAN)


def white(s):
    return colour(s, WHITE)


def bright_black(s):
    return colour(s, BRIGHT_BLACK)


def bright_red(s):
    return colour(s, BRIGHT_RED)


def bright_green(s):
    return colour(s, BRIGHT_GREEN)


def bright_yellow(s):
    return colour(s, BRIGHT_YELLOW)


def bright_blue(s):
    return colour(s, BRIGHT_BLUE)


def bright_magenta(s):
    return colour(s, BRIGHT_MAGENTA)


def bright_cyan(s):
    return colour(s, BRIGHT_CYAN)


def bright_white(s):
    return colour(s, BRIGHT_WHITE)


class Table(object):

    def __init__(self, console, str_function=str, auto_align=False, header_rows=0, header_columns=0):
        self.console = console
        self._rows = []
        self._widths = []
        self.str_function = str_function
        self.auto_align = auto_align
        self.header_rows = header_rows
        self.header_columns = header_columns

    def append(self, values):
        self._rows.append(tuple(values))

    def _calc_widths(self):
        s = self.str_function
        w = self._widths
        w[:] = ()
        for values in self._rows:
            while len(w) < len(values):
                w.append(0)
            for i, value in enumerate(values):
                w[i] = max(w[i], len(s(value)))

    def write(self):
        self._calc_widths()
        widths = self._widths
        if not widths:
            return

        if self.console.ch_out.colour:
            colour = cyan
        else:
            colour = lambda x: x

        table = []

        def row_join(values, is_header_row):
            r = []
            for i, value in enumerate(values):
                if i > 0:
                    if i == self.header_columns:
                        gap = u" : "
                        if not is_header_row:
                            gap = colour(gap)
                    else:
                        gap = u"  "
                    r.append(gap)
                r.append(value)
            return u"".join(r)

        s = self.str_function
        for y, values in enumerate(self._rows):
            is_header_row = y < self.header_rows
            row = []
            for x, value in enumerate(values):
                if self.auto_align and isinstance(value, number):
                    template = u"{:>%d}" % widths[x]
                else:
                    template = u"{:<%d}" % widths[x]
                cell = template.format(s(value))
                if not is_header_row and x < self.header_columns:
                    cell = colour(cell)
                row.append(cell)
            if is_header_row:
                table.append(colour(row_join(row, is_header_row)))
                if y == self.header_rows - 1:
                    hr = []
                    for width in self._widths:
                        hr.append(width * u"-")
                    table.append(colour(row_join(hr, is_header_row)))
            else:
                table.append(row_join(row, is_header_row))
        self.console.write(u"\n".join(table))


class Channel(object):

    encoding = "utf-8"
    file_no = None

    _write = None

    def __init__(self, file, colour=True):
        self.file = file
        try:
            file_no = file.fileno()
        except IOError:
            self._write = self.file.write
        else:
            self._write = lambda s: os_write(file_no, s)
        try:
            self.encoding = file.encoding or self.encoding
        except AttributeError:
            pass
        self.colour = colour and file.isatty()

    def write_bytes(self, s):
        assert isinstance(s, bytes)
        self._write(s)

    def write(self, s):
        assert isinstance(s, unicode)
        self.write_bytes(s.encode(self.encoding))


class Console(object):

    def __init__(self, in_stream=None, out_stream=None, err_stream=None, colour=True):
        self.in_stream = in_stream or stdin
        self.ch_out = Channel(out_stream or stdout, colour)
        self.ch_err = Channel(err_stream or stderr, colour)
        self.colour = colour

    def write(self, s=u"", end=u"\n"):
        self.ch_out.write(s)
        self.ch_out.write(end)

    def write_metadata(self, s=u"", end=u"\n"):
        if self.ch_out.colour:
            s = cyan(s)
        self.write(s, end=end)

    def write_err(self, s=u"", end=u"\n"):
        self.ch_err.write(s)
        self.ch_err.write(end)

    def write_help(self, s=u"", end=u"\n"):
        self.write_err(s, end)

    def write_error(self, s=u"", end=u"\n"):
        if self.ch_err.colour:
            s = red(s)
        self.write_err(s, end)
