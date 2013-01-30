#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright 2011-2012 Nigel Small
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

""" Generic graph-based, local data structures.

    Path: a linear sequence of nodes, connected by relationships

"""


from .util import round_robin


class Path(object):

    def __init__(self, node, *rels_and_nodes):
        self._nodes = [node]
        self._nodes.extend(rels_and_nodes[1::2])
        if len(rels_and_nodes) % 2 != 0:
            # If a trailing relationship is supplied, add a dummy end node
            self._nodes.append(None)
        self._relationships = list(rels_and_nodes[0::2])

    def __repr__(self):
        out = ", ".join(repr(item) for item in round_robin(self._nodes, self._relationships))
        return "Path({0})".format(out)

    def __str__(self):
        out = []
        for i, rel in enumerate(self._relationships):
            out.append(str(self._nodes[i]))
            out.append("-")
            out.append(str(rel))
            out.append("->")
        out.append(str(self._nodes[-1]))
        return "".join(out)

    def __nonzero__(self):
        return bool(self._relationships)

    def __len__(self):
        return len(self._relationships)

    def __eq__(self, other):
        return self._nodes == other._nodes and \
               self._relationships == other._relationships

    def __ne__(self, other):
        return self._nodes != other._nodes or \
               self._relationships != other._relationships

    def __getitem__(self, item):
        size = len(self._relationships)
        def adjust(value, default=None):
            if value is None:
                return default
            if value < 0:
                return value + size
            else:
                return value
        if isinstance(item, slice):
            if item.step is not None:
                raise ValueError("Steps not supported in path slicing")
            start, stop = adjust(item.start, 0), adjust(item.stop, size)
            path = Path(self._nodes[start])
            for i in range(start, stop):
                path._relationships.append(self._relationships[i])
                path._nodes.append(self._nodes[i + 1])
            return path
        else:
            i = int(item)
            if i < 0:
                i += len(self._relationships)
            return Path(self._nodes[i], self._relationships[i], self._nodes[i + 1])

    def __iter__(self):
        def relationship_tuples():
            for i, rel in enumerate(self._relationships):
                yield self._nodes[i], rel, self._nodes[i + 1]
        return iter(relationship_tuples())

    def order(self):
        return len(self._nodes)

    def size(self):
        return len(self._relationships)

    @property
    def nodes(self):
        """ Return a list of all the nodes which make up this path.
        """
        return self._nodes

    @property
    def relationships(self):
        """ Return a list of all the relationships which make up this path.
        """
        return self._relationships

    @classmethod
    def join(cls, left, rel, right):
        if isinstance(left, Path):
            left = left[:]
        else:
            left = Path(left)
        if isinstance(right, Path):
            right = right[:]
        else:
            right = Path(right)
        left._relationships.append(rel)
        left._nodes.extend(right._nodes)
        left._relationships.extend(right._relationships)
        return left
