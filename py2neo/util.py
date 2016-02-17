#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2015, Nigel Small
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

"""
Utility module
"""


from itertools import cycle, islice
from os import linesep
from os.path import basename
import re
from sys import stdout
from textwrap import dedent
from threading import local
import warnings
from weakref import WeakValueDictionary


# Word separation patterns for re-casing strings.
WORD_FIRST = re.compile("(.)([A-Z][a-z]+)")
WORD_ALL = re.compile("([a-z0-9])([A-Z])")

BASE62_DIGITS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"


def base62(x):
    if x == 0:
        return "0"
    digits = []
    while x:
        x, d = divmod(x, 62)
        digits.insert(0, BASE62_DIGITS[d])
    return "".join(digits)


def snake_case(s):
    words = s.replace("_", " ").replace("-", " ").split()
    return "_".join(word.lower() for word in words)


def relationship_case(s):
    s1 = WORD_FIRST.sub(r"\1_\2", s)
    return WORD_ALL.sub(r"\1_\2", s1).upper()


def round_robin(*iterables):
    """ Cycle through a number of iterables, returning
        the next item from each in turn.

        round_robin('ABC', 'D', 'EF') --> A D E B F C

        Original recipe credited to George Sakkis
        Python 2/3 cross-compatibility tweak by Nigel Small
    """
    pending = len(iterables)
    nexts = cycle(iter(it) for it in iterables)
    while pending:
        try:
            for n in nexts:
                yield next(n)
        except StopIteration:
            pending -= 1
            nexts = cycle(islice(nexts, pending))


def deprecated(message):
    """ Decorator for deprecating functions and methods.

    ::

        @deprecated("'foo' has been deprecated in favour of 'bar'")
        def foo(x):
            pass

    """
    def f__(f):
        def f_(*args, **kwargs):
            warnings.warn(message, category=DeprecationWarning, stacklevel=2)
            return f(*args, **kwargs)
        f_.__name__ = f.__name__
        f_.__doc__ = f.__doc__
        f_.__dict__.update(f.__dict__)
        return f_
    return f__


VERSION = re.compile("(\d+\.\d+(\.\d+)?)")


def version_tuple(string):
    numbers = VERSION.match(string)
    version = [int(n) for n in numbers.group(0).split(".")]
    extra = string[len(numbers.group(0)):]
    while extra.startswith(".") or extra.startswith("-"):
        extra = extra[1:]
    if extra:
        version += [extra]
    return tuple(version)


def is_collection(obj):
    """ Returns true for any iterable which is not a string or byte sequence.
    """
    try:
        if isinstance(obj, unicode):
            return False
    except NameError:
        pass
    if isinstance(obj, bytes):
        return False
    try:
        iter(obj)
    except TypeError:
        return False
    try:
        hasattr(None, obj)
    except TypeError:
        return True
    return False


has_all = lambda iterable, items: all(item in iterable for item in items)


def raise_from(exception, cause):
    exception.__cause__ = cause
    raise exception


class ThreadLocalWeakValueDictionary(WeakValueDictionary, local):
    pass


class BaseCommander(object):

    epilog = None

    def __init__(self, out=None):
        self.out = out or stdout

    def write(self, s):
        self.out.write(s)

    def write_line(self, s):
        self.out.write(s)
        self.out.write(linesep)

    def usage(self, script):
        if self.__doc__:
            self.write_line(dedent(self.__doc__).strip().format(script=basename(script)))
            if self.epilog:
                self.write_line("")
                self.write_line(self.epilog)
        else:
            self.write_line("usage: ?")

    def execute(self, *args):
        try:
            command, command_args = args[1], args[2:]
        except IndexError:
            self.usage(args[0])
        else:
            try:
                method = getattr(self, command)
            except AttributeError:
                self.write_line("Unknown command %r" % command)
            else:
                method(*args[1:])

    def parser(self, script):
        from argparse import ArgumentParser
        return ArgumentParser(prog=script, epilog=self.epilog)
