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


from __future__ import absolute_import

from itertools import cycle, islice

from py2neo.internal.compat import Set, bytes_types, string_types


def is_collection(obj):
    """ Returns true for any iterable which is not a string or byte sequence.
    """
    if isinstance(obj, bytes_types) or isinstance(obj, string_types):
        return False
    try:
        iter(obj)
    except TypeError:
        return False
    else:
        return True


def iter_items(iterable):
    """ Iterate through all items (key-value pairs) within an iterable
    dictionary-like object. If the object has a `keys` method, this is
    used along with `__getitem__` to yield each pair in turn. If no
    `keys` method exists, each iterable element is assumed to be a
    2-tuple of key and value.
    """
    if hasattr(iterable, "keys"):
        for key in iterable.keys():
            yield key, iterable[key]
    else:
        for key, value in iterable:
            yield key, value


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


class SetView(Set):

    def __init__(self, collection):
        self.__collection = collection

    def __eq__(self, other):
        return frozenset(self) == frozenset(other)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __len__(self):
        return len(self.__collection)

    def __iter__(self):
        return iter(self.__collection)

    def __contains__(self, element):
        return element in self.__collection

    def difference(self, other):
        cls = self.__class__
        return cls(frozenset(self).difference(frozenset(other)))


class ReactiveSet(set):
    """ A :class:`set` that can trigger callbacks for each element added
    or removed.
    """

    def __init__(self, iterable=(), on_add=None, on_remove=None):
        self._on_add = on_add
        self._on_remove = on_remove
        elements = set(iterable)
        set.__init__(self, elements)
        if callable(self._on_add):
            self._on_add(*elements)

    def __ior__(self, other):
        elements = other - self
        set.__ior__(self, other)
        if callable(self._on_add):
            self._on_add(*elements)
        return self

    def __iand__(self, other):
        elements = self ^ other
        set.__iand__(self, other)
        if callable(self._on_remove):
            self._on_remove(*elements)
        return self

    def __isub__(self, other):
        elements = self & other
        set.__isub__(self, other)
        if callable(self._on_remove):
            self._on_remove(*elements)
        return self

    def __ixor__(self, other):
        added = other - self
        removed = self & other
        set.__ixor__(self, other)
        if callable(self._on_add):
            self._on_add(*added)
        if callable(self._on_remove):
            self._on_remove(*removed)
        return self

    def add(self, element):
        """ Add an element to the set.

        :triggers: `on_add`
        """
        if element not in self:
            set.add(self, element)
            if callable(self._on_add):
                self._on_add(element)

    def remove(self, element):
        """ Remove an element from the set.

        :triggers: `on_remove`
        """
        set.remove(self, element)
        if callable(self._on_remove):
            self._on_remove(element)

    def discard(self, element):
        """ Discard an element from the set.

        :triggers: `on_remove`
        """
        if element in self:
            set.discard(self, element)
            if callable(self._on_remove):
                self._on_remove(element)

    def pop(self):
        """ Remove an arbitrary element from the set.

        :triggers: `on_remove`
        """
        element = set.pop(self)
        if callable(self._on_remove):
            self._on_remove(element)
        return element

    def clear(self):
        """ Remove all elements from the set.

        :triggers: `on_remove`
        """
        elements = set(self)
        set.clear(self)
        if callable(self._on_remove):
            self._on_remove(*elements)
