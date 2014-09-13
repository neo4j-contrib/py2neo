#!/usr/bin/env python
# -*- encoding: utf-8 -*-


from __future__ import unicode_literals

import sys

try:
    unicode
except NameError:
    # Python 3
    def ustr(s, encoding="utf-8"):
        if s is None:
            return ""
        elif isinstance(s, str):
            return s
        try:
            return s.decode(encoding)
        except AttributeError:
            return str(s)
else:
    # Python 2
    def ustr(s, encoding="utf-8"):
        if s is None:
            return ""
        elif isinstance(s, str):
            return s.decode(encoding)
        else:
            return unicode(s)

try:
    long
except NameError:
    # Python 3
    is_integer = lambda x: isinstance(x, int)
    is_numeric = lambda x: isinstance(x, (int, float, complex))
else:
    # Python 2
    is_integer = lambda x: isinstance(x, (int, long))
    is_numeric = lambda x: isinstance(x, (int, float, long, complex))


def autojust(value, size):
    if is_numeric(value):
        return ustr(value).rjust(size)
    else:
        return ustr(value).ljust(size)


class TextTable(object):

    def __init__(self, header):
        self.__header = list(map(ustr, header))
        self.__rows = []
        self.__widths = list(map(len, self.__header))
        self.__repr = None

    def __repr__(self):
        if self.__repr is None:
            widths = self.__widths
            lines = [
                " " + " │ ".join(value.ljust(widths[i])
                                 for i, value in enumerate(self.__header)) + "\n",
                "─" + "─┼─".join("─" * widths[i]
                                 for i, value in enumerate(self.__header)) + "─\n",
            ]
            for row in self.__rows:
                lines.append(" " + " │ ".join(autojust(value, widths[i])
                                              for i, value in enumerate(row)) + "\n")
            self.__repr = "".join(lines)
            if sys.version_info < (3,):
                self.__repr = self.__repr.encode("utf-8")
        return self.__repr

    def append(self, row):
        row = list(row)
        self.__rows.append(row)
        self.__widths = [max(self.__widths[i], len(ustr(value))) for i, value in enumerate(row)]
        self.__repr = None
