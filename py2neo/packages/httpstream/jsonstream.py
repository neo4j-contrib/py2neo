#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright 2012-2013 Nigel Small
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

""" Incremental JSON parser.
"""


try:
    from builtins import chr as _chr
except ImportError:
    from __builtin__ import unichr as _chr
from itertools import groupby
from string import whitespace
try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO


__all__ = ["JSONStream", "assembled", "grouped"]


class AwaitingData(BaseException):
    """ Raised when data is temporarily unavailable.
    """

    def __init__(self, *args, **kwargs):
        super(AwaitingData, self).__init__(*args, **kwargs)


class EndOfStream(BaseException):
    """ Raised when stream is exhausted.
    """

    def __init__(self, *args, **kwargs):
        super(EndOfStream, self).__init__(*args, **kwargs)


class UnexpectedCharacter(ValueError):
    """ Raised when a unexpected character is encountered.
    """

    def __init__(self, *args, **kwargs):
        super(UnexpectedCharacter, self).__init__(*args, **kwargs)


class Tokeniser(object):

    ESCAPE_SEQUENCES = {
        '"': u'"',
        '\\': u'\\',
        '/': u'/',
        'b': u'\b',
        'f': u'\f',
        'n': u'\n',
        'r': u'\r',
        't': u'\t',
    }

    def __init__(self):
        self.start()

    def _assert_writable(self):
        if not self._writable:
            raise IOError("Stream is not writable")

    def write(self, data):
        """Write raw JSON data to the decoder stream.
        """
        self._assert_writable()
        read_pos = self.data.tell()
        self.data.seek(self._write_pos)
        self.data.write(data)
        self._write_pos = self.data.tell()
        self.data.seek(read_pos)

    def start(self):
        self.data = StringIO()
        self._write_pos = 0
        self._writable = True

    def end(self):
        self._writable = False

    def _peek(self):
        """Return next available character without
        advancing pointer.
        """
        pos = self.data.tell()
        ch = self.data.read(1)
        self.data.seek(pos)
        if ch:
            return ch
        elif self._writable:
            raise AwaitingData()
        else:
            raise EndOfStream()

    def _read(self):
        """Read the next character.
        """
        ch = self.data.read(1)
        if ch:
            return ch
        elif self._writable:
            raise AwaitingData()
        else:
            raise EndOfStream()

    def _skip_whitespace(self):
        while True:
            pos = self.data.tell()
            ch = self.data.read(1)
            if ch == '':
                break
            if ch not in whitespace:
                self.data.seek(pos)
                break

    def _read_literal(self, literal):
        pos = self.data.tell()
        try:
            for expected in literal:
                actual = self._read()
                if actual != expected:
                    raise UnexpectedCharacter(actual)
        except AwaitingData:
            self.data.seek(pos)
            raise AwaitingData()
        return literal

    def _read_digit(self):
        pos = self.data.tell()
        try:
            digit = self._read()
            if digit not in "0123456789":
                self.data.seek(pos)
                raise UnexpectedCharacter(digit)
        except AwaitingData:
            self.data.seek(pos)
            raise AwaitingData()
        return digit

    def _read_string(self):
        pos = self.data.tell()
        src, value = [self._read_literal('"')], []
        try:
            while True:
                ch = self._read()
                src.append(ch)
                if ch == '\\':
                    ch = self._read()
                    src.append(ch)
                    if ch in self.ESCAPE_SEQUENCES:
                        value.append(self.ESCAPE_SEQUENCES[ch])
                    elif ch == 'u':
                        n = 0
                        for i in range(4):
                            ch = self._read()
                            src.append(ch)
                            n = 16 * n + int(ch, 16)
                        value.append(_chr(n))
                    else:
                        raise UnexpectedCharacter(ch)
                elif ch == '"':
                    break
                else:
                    value.append(ch)
        except AwaitingData:
            self.data.seek(pos)
            raise AwaitingData()
        return "".join(src), u"".join(value)

    def _read_number(self):
        pos = self.data.tell()
        src = []
        has_fractional_part = False
        try:
            # check for sign
            ch = self._peek()
            if ch == '-':
                src.append(self._read())
            # read integer part
            ch = self._read_digit()
            src.append(ch)
            if ch != '0':
                while True:
                    try:
                        src.append(self._read_digit())
                    except (UnexpectedCharacter, EndOfStream):
                        break
            try:
                ch = self._peek()
            except EndOfStream:
                pass
            # read fractional part
            if ch == '.':
                has_fractional_part = True
                src.append(self._read())
                while True:
                    try:
                        src.append(self._read_digit())
                    except (UnexpectedCharacter, EndOfStream):
                        break
        except AwaitingData:
            # number potentially incomplete: need to wait for
            # further data or end of stream
            self.data.seek(pos)
            raise AwaitingData()
        src = "".join(src)
        if has_fractional_part:
            return src, float(src)
        else:
            return src, int(src)

    def read(self):
        self._skip_whitespace()
        ch = self._peek()
        if ch in ',:[]{}':
            return self._read(), None
        elif ch == 'n':
            return self._read_literal('null'), None
        elif ch == 't':
            return self._read_literal('true'), True
        elif ch == 'f':
            return self._read_literal('false'), False
        elif ch == '"':
            return self._read_string()
        elif ch in '-0123456789':
            return self._read_number()
        else:
            raise UnexpectedCharacter(ch)


# Token constants used for expectation management
VALUE = 0x01
OPENING_BRACKET = 0x02
CLOSING_BRACKET = 0x04
OPENING_BRACE = 0x08
CLOSING_BRACE = 0x10
COMMA = 0x20
COLON = 0x40


class JSONStream(object):
    """ Streaming JSON decoder.
    """

    def __init__(self, source):
        self.tokeniser = Tokeniser()
        self.source = iter(source)
        self.path = []
        self._expectation = VALUE | OPENING_BRACKET | OPENING_BRACE

    def _assert_expecting(self, token, src):
        if not self._expectation & token:
            raise UnexpectedCharacter(src)

    def _in_array(self):
        return self.path and isinstance(self.path[-1], int)

    def _in_object(self):
        return self.path and not isinstance(self.path[-1], int)

    def _has_key(self):
        if self.path:
            top = self.path[-1]
            if top is None:
                return False
            elif isinstance(self.path[-1], int):
                return None
            else:
                return True
        else:
            return None

    def _next_value(self, src, value):
        self._assert_expecting(VALUE, src)
        if self._in_array():
            # array value
            out = tuple(self.path), value
            self.path[-1] += 1
            self._expectation = COMMA | CLOSING_BRACKET
        elif self._in_object():
            if self._has_key():
                # object value
                out = tuple(self.path), value
                self.path[-1] = None
                self._expectation = COMMA | CLOSING_BRACE
            else:
                # object key
                out = None
                self.path[-1] = value
                self._expectation = COLON
        else:
            # simple value
            out = tuple(self.path), value
        return out

    def _handle_comma(self, src):
        self._assert_expecting(COMMA, src)
        self._expectation = VALUE | OPENING_BRACKET | OPENING_BRACE

    def _handle_colon(self, src):
        self._assert_expecting(COLON, src)
        self._expectation = VALUE | OPENING_BRACKET | OPENING_BRACE

    def _open_array(self, src):
        self._assert_expecting(OPENING_BRACKET, src)
        self.path.append(0)
        self._expectation = (VALUE | OPENING_BRACKET | CLOSING_BRACKET |
                             OPENING_BRACE)

    def _close_array(self, src):
        self._assert_expecting(CLOSING_BRACKET, src)
        self.path.pop()
        if self._in_array():
            self.path[-1] += 1
            self._expectation = COMMA | CLOSING_BRACKET
        elif self._in_object():
            self.path[-1] = None
            self._expectation = COMMA | CLOSING_BRACE
        else:
            self._expectation = VALUE | OPENING_BRACKET | OPENING_BRACE

    def _open_object(self, src):
        self._assert_expecting(OPENING_BRACE, src)
        self.path.append(None)
        self._expectation = VALUE | CLOSING_BRACE

    def _close_object(self, src):
        self._assert_expecting(CLOSING_BRACE, src)
        self.path.pop()
        if self._in_array():
            self.path[-1] += 1
            self._expectation = COMMA | CLOSING_BRACKET
        elif self._in_object():
            self.path[-1] = None
            self._expectation = COMMA | CLOSING_BRACE
        else:
            self._expectation = VALUE | OPENING_BRACKET | OPENING_BRACE

    def __iter__(self):
        while True:
            try:
                try:
                    self.tokeniser.write(next(self.source))
                except StopIteration:
                    self.tokeniser.end()
                while True:
                    try:
                        src, value = self.tokeniser.read()
                        if src == ',':
                            self._handle_comma(src)
                        elif src == ':':
                            self._handle_colon(src)
                        elif src == '[':
                            yield tuple(self.path), []
                            self._open_array(src)
                        elif src == ']':
                            self._close_array(src)
                        elif src == '{':
                            yield tuple(self.path), {}
                            self._open_object(src)
                        elif src == '}':
                            self._close_object(src)
                        else:
                            out = self._next_value(src, value)
                            if out:
                                yield out
                    except AwaitingData:
                        break
            except EndOfStream:
                break


def _merged(obj, key, value):
    """ Returns object with value merged at a position described by iterable
    key. The key describes a navigable path through the object hierarchy with
    integer items describing list indexes and other types of items describing
    dictionary keys.

        >>> obj = None
        >>> obj = _merged(obj, ("drink",), "lemonade")
        >>> obj
        {'drink': 'lemonade'}
        >>> obj = _merged(obj, ("cutlery", 0), "knife")
        >>> obj = _merged(obj, ("cutlery", 1), "fork")
        >>> obj = _merged(obj, ("cutlery", 2), "spoon")
        >>> obj
        {'cutlery': ['knife', 'fork', 'spoon'], 'drink': 'lemonade'}

    """
    if key:
        k = key[0]
        if isinstance(k, int):
            if isinstance(obj, list):
                obj = list(obj)
            else:
                obj = []
            while len(obj) <= k:
                obj.append(None)
        else:
            if isinstance(obj, dict):
                obj = dict(obj)
            else:
                obj = {}
            obj.setdefault(k, None)
        obj[k] = _merged(obj[k], key[1:], value)
        return obj
    else:
        return value


def assembled(iterable):
    """ Returns a JSON-derived value from a set of key-value pairs as produced
    by the JSONStream process. This operates in a similar way to the built-in
    `dict` function. Internally, this uses the `merged` function on each pair
    to build the return value.

        >>> data = [
        ...     (("drink",), "lemonade"),
        ...     (("cutlery", 0), "knife"),
        ...     (("cutlery", 1), "fork"),
        ...     (("cutlery", 2), "spoon"),
        ... ]
        >>> assembled(data)
        {'cutlery': ['knife', 'fork', 'spoon'], 'drink': 'lemonade'}

    :param iterable: key-value pairs to be merged into assembled value
    """
    obj = None
    for key, value in iterable:
        obj = _merged(obj, key, value)
    return obj


def _group(iterable, level):
    for key, value in iterable:
        yield key[level:], value


def grouped(iterable, level=1):
    def _group_key(item):
        key, value = item
        if len(key) >= level:
            return key[0:level]
        else:
            return None
    for key, value in groupby(iterable, _group_key):
        if key is not None:
            yield key, _group(value, level)
