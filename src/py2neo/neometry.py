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

    Graph: an in-memory graph object, comprised of nodes and edges
    Path: a linear sequence of nodes, connected by edges

"""


import json


class Graph(object):

    def __init__(self):
        Graph.clear(self)

    def __repr__(self):
        out = []
        for key, value in self._nodes.items():
            if value is None:
                out.append("({0})".format(key))
            else:
                out.append("({0}:={1})".format(key, json.dumps(value, separators=(",", ":"))))
        for (start, rel, end), value in self.edges().items():
            if value is None:
                out.append("({0})-[:{1}]->({2})".format(start, rel, end))
            else:
                out.append("({0})-[:{1}:={3}]->({2})".format(start, rel, end, json.dumps(value, separators=(",", ":"))))
        return "\n".join(out)

    def __eq__(self, other):
        return self._nodes == other._nodes and self._edges == other._edges

    def __ne__(self, other):
        return self._nodes != other._nodes or self._edges != other._edges

    def __len__(self):
        return len(self._edges)

    def __nonzero__(self):
        return bool(self._edges)

    def __contains__(self, node):
        """ Return :py:const:`True` if the node identified by `key` exists
            within this graph.
        """
        return node in self._edges

    def __getitem__(self, node):
        self._assert_exists(node)
        return self._nodes[node]

    def __setitem__(self, node, value):
        """ Insert or update node identified by `key`, setting the value to
            `value`.
        """
        self._nodes[node] = value
        if node not in self._edges:
            self._edges[node] = {}

    def __delitem__(self, node):
        pass
        # delete if related?
        # clear start if none left

    def __iter__(self):
        return iter(self._nodes)

    def order(self):
        return len(self._nodes)

    def size(self):
        return len(self._edges)

    def clear(self):
        self._nodes = {}
        self._edges = {}

    def copy(self):
        graph = Graph()
        graph._nodes = copy.deepcopy(self._nodes)
        graph._edges = copy.deepcopy(self._edges)
        return graph

    def _assert_exists(self, node):
        """ Raise an exception if a node identified by `key` does not exist
            within this graph.
        """
        if node not in self._edges:
            raise KeyError("Node \"{0}\" not found".format(node))

    def get(self, node, default=None):
        """ Return the value of the node identified by `key`.
        """
        try:
            return self._nodes[node]
        except KeyError:
            return default

    def nodes(self, value=None):
        """ Return a dictionary of all nodes, filtered by the criteria specified.
        """
        if value is None:
            return self._nodes
        else:
            return dict((k, v) for k, v in self._nodes.items() if value == v)

    def edges(self, start=None, relationship=None, end=None, value=None):
        """ Return a dictionary of all edges, filtered by the criteria specified.
        """
        if end is not None:
            self._assert_exists(end)
        if start is not None:
            self._assert_exists(start)
            return dict(
                ((start, r, e), v)
                for e in self._edges[start]
                for r, v in self._edges[start][e].items()
                if end is None or end == e
                if relationship is None or relationship == r
                if value is None or value == v
            )
        else:
            return dict(
                ((s, r, e), v)
                for s in self._edges
                for e in self._edges[s]
                for r, v in self._edges[s][e].items()
                if end is None or end == e
                if relationship is None or relationship == r
                if value is None or value == v
            )

    def relate(self, start, relationship, end, value=None):
        """ Establish a relationship between two nodes, optionally assigning
            a value to that relationship.
        """
        self._assert_exists(start)
        self._assert_exists(end)
        if end not in self._edges[start]:
            self._edges[start][end] = {}
        self._edges[start][end][relationship] = value

    def unrelate(self, start=None, relationship=None, end=None, value=None):
        """ Disestablish one or more relationships which match the criteria
            specified.
        """
        for s, r, e in self.edges(start, relationship, end, value):
            del self._edges[s][e][r]


class Path(object):

    def __init__(self, node, *edges_and_nodes):
        if len(edges_and_nodes) % 2 != 0:
            raise ValueError("Edges and nodes must come in pairs")
        self._nodes = [node]
        self._nodes.extend(edges_and_nodes[1::2])
        self._edges = list(edges_and_nodes[0::2])

    def __repr__(self):
        out = []
        for i, edge in enumerate(self._edges):
            out.append("(")
            out.append(json.dumps(self._nodes[i], separators=(",", ":")))
            out.append(")")
            out.append("-[:")
            out.append(str(edge))
            out.append("]->")
        out.append("(")
        out.append(json.dumps(self._nodes[-1], separators=(",", ":")))
        out.append(")")
        return "".join(out)

    def __nonzero__(self):
        return bool(self._edges)

    def __len__(self):
        return len(self._edges)

    def __eq__(self, other):
        return self._nodes == other._nodes and \
               self._edges == other._edges

    def __ne__(self, other):
        return self._nodes != other._nodes or \
               self._edges != other._edges

    def __getitem__(self, item):
        size = len(self._edges)
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
                path._edges.append(self._edges[i])
                path._nodes.append(self._nodes[i + 1])
            return path
        else:
            i = int(item)
            if i < 0:
                i += len(self._edges)
            return Path(self._nodes[i], self._edges[i], self._nodes[i + 1])

    def __iter__(self):
        def edge_tuples():
            for i, edge in enumerate(self._edges):
                yield self._nodes[i], edge, self._nodes[i + 1]
        return iter(edge_tuples())

    def order(self):
        return len(self._nodes)

    def size(self):
        return len(self._edges)

    @property
    def nodes(self):
        """ Return a list of all the nodes which make up this path.
        """
        return self._nodes

    @property
    def edges(self):
        """ Return a list of all the edges which make up this path.
        """
        return self._edges

    @classmethod
    def join(cls, left, edge, right):
        if isinstance(left, Path):
            left = left[:]
        else:
            left = Path(left)
        if isinstance(right, Path):
            right = right[:]
        else:
            right = Path(right)
        left._edges.append(edge)
        left._nodes.extend(right._nodes)
        left._edges.extend(right._edges)
        return left
