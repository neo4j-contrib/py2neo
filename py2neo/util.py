#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright 2011-2013, Nigel Small
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

""" Utility module
"""


import sys

__PY3K = sys.version_info[0] >= 3

import re
import time
import warnings

try:
    from urllib.parse import quote as _quote
except ImportError:
    from urllib import quote as _quote

from itertools import cycle, islice


if __PY3K:
    is_string = lambda value: isinstance(value, str)
else:
    is_string = lambda value: isinstance(value, (str, unicode))


def quote(string, safe='/'):
    """ Quote a string for use in URIs.
    """
    try:
        return _quote(string, safe.encode("utf-8"))
    except UnicodeEncodeError:
        return string


def numberise(n):
    """ Convert a value to an integer if possible. If not, simply return
        the input value.
    """
    if n == "NaN":
        return None
    try:
        return int(n)
    except ValueError:
        return n


def execution_time(func, *args, **kwargs):
    if sys.platform == "win32":
        timer = time.clock
    else:
        timer = time.time
    t0 = timer()
    try:
        func(*args, **kwargs)
    finally:
        return timer() - t0


def compact(obj):
    """ Return a copy of an object with all :py:const:`None` values removed.
    """
    if isinstance(obj, dict):
        return dict((key, value) for key, value in obj.items() if value is not None)
    else:
        return obj.__class__(value for value in obj if value is not None)


def flatten(*values):
    for value in values:
        if hasattr(value, "__iter__"):
            for val in value:
                yield val
        else:
            yield value


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



class PropertyCache(object):

    def __init__(self, properties=None, max_age=None):
        self._properties = {}
        self.max_age = max_age
        self._last_updated_time = None
        if properties:
            self.update(properties)

    def __repr__(self):
        return repr(self._properties)

    def __nonzero__(self):
        return bool(self._properties)

    def __len__(self):
        return len(self._properties)

    def __getitem__(self, item):
        return self._properties[item]

    def __setitem__(self, item, value):
        self._properties[item] = value

    def __delitem__(self, item):
        del self._properties[item]

    def __iter__(self):
        return self._properties.__iter__()

    def __contains__(self, item):
        return item in self._properties

    @property
    def expired(self):
        if self._last_updated_time and self.max_age:
            return time.time() - self._last_updated_time > self.max_age
        else:
            return None

    @property
    def needs_update(self):
        return not self._properties or self.expired

    def clear(self):
        self.update(None)

    def update(self, properties):
        self._properties.clear()
        if properties:
            self._properties.update(properties)
        self._last_updated_time = time.time()

    def get(self, key, default=None):
        return self._properties.get(key, default)

    def get_all(self):
        return self._properties


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
    if numbers:
        version = [int(n) for n in numbers.group(0).split(".")]
        extra = string[len(numbers.group(0)):]
        while extra.startswith(".") or extra.startswith("-"):
            extra = extra[1:]
    else:
        version = []
        extra = string
    while len(version) < 3:
        version += [0]
    version += [extra]
    return tuple(version)


def is_collection(obj):
    """ Returns true for any iterable which is not a string or byte sequence.
    """
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
