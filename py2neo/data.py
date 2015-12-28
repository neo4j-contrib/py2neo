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


from .compat import integer, string, unicode, ustr

# Maximum and minimum integers supported up to Java 7.
# Java 8 also supports unsigned long which can extend
# to (2 ** 64 - 1) but Neo4j is not yet on Java 8.
JAVA_INTEGER_MIN_VALUE = -2 ** 63
JAVA_INTEGER_MAX_VALUE = 2 ** 63 - 1


def coerce_atomic_property(x):
    if isinstance(x, unicode):
        return x
    elif isinstance(x, string):
        return ustr(x)
    elif isinstance(x, bool):
        return x
    elif isinstance(x, integer):
        if JAVA_INTEGER_MIN_VALUE <= x <= JAVA_INTEGER_MAX_VALUE:
            return x
        else:
            raise ValueError("Integer value out of range: %s" % x)
    elif isinstance(x, float):
        return x
    else:
        raise TypeError("Properties of type %s are not supported" % x.__class__.__name__)


def coerce_property(x):
    if isinstance(x, (tuple, list, set, frozenset)):
        collection = []
        cls = None
        for item in x:
            item = coerce_atomic_property(item)
            t = type(item)
            if cls is None:
                cls = t
            elif t != cls:
                raise TypeError("List properties must be homogenous "
                                "(found %s in list of %s)" % (t.__name__, cls.__name__))
            collection.append(item)
        return x.__class__(collection)
    else:
        return coerce_atomic_property(x)


class PropertySet(dict):
    """ A dictionary subclass that equates None with a non-existent key.
    """

    def __init__(self, iterable=None, **kwargs):
        dict.__init__(self)
        self.update(iterable, **kwargs)

    def __eq__(self, other):
        if not isinstance(other, PropertySet):
            other = PropertySet(other)
        return dict.__eq__(self, other)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        value = 0
        for k, v in self.items():
            if isinstance(v, list):
                value ^= hash((k, tuple(v)))
            else:
                value ^= hash((k, v))
        return value

    def __getitem__(self, key):
        return dict.get(self, key)

    def __setitem__(self, key, value):
        if value is None:
            try:
                dict.__delitem__(self, key)
            except KeyError:
                pass
        else:
            dict.__setitem__(self, key, coerce_property(value))

    def setdefault(self, key, default=None):
        if key in self:
            value = self[key]
        elif default is None:
            value = None
        else:
            value = dict.setdefault(self, key, default)
        return value

    def update(self, iterable=None, **kwargs):
        for key, value in dict(iterable or {}, **kwargs).items():
            self[key] = value


class PropertyContainer(object):
    """ Base class for objects that contain a set of properties,
    """

    def __init__(self, **properties):
        self._properties = PropertySet(properties)

    def __bool__(self):
        return bool(self._properties)

    def __nonzero__(self):
        return bool(self._properties)

    def __len__(self):
        return len(self._properties)

    def __contains__(self, key):
        return key in self._properties

    def __getitem__(self, key):
        return self._properties.__getitem__(key)

    def __setitem__(self, key, value):
        self._properties.__setitem__(key, value)

    def __delitem__(self, key):
        self._properties.__delitem__(key)

    def __iter__(self):
        return iter(self._properties)

    def clear(self):
        self._properties.clear()

    def get(self, key, default=None):
        return self._properties.get(key, default)

    def keys(self):
        return self._properties.keys()

    def setdefault(self, key, default=None):
        return self._properties.setdefault(key, default)

    def update(self, iterable=None, **kwargs):
        self._properties.update(iterable, **kwargs)

    def values(self):
        return self._properties.values()
