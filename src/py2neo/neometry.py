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

"""

"""


class Graph(object):

    def __init__(self):
        self.clear()

    def __len__(self):
        return len(self._edges)

    def __nonzero__(self):
        return bool(self._edges)

    def __contains__(self, key):
        """ Return :py:const:`True` if the node identified by `key` exists
            within this graph.
        """
        return key in self._edges

    def __getitem__(self, key):
        self._assert_exists(key)
        return self._nodes[key]

    def __setitem__(self, key, value):
        """ Insert or update node identified by `key`, setting the value to
            `value`.
        """
        self._nodes[key] = value
        if key not in self._edges:
            self._edges[key] = {}

    def __delitem__(self, key):
        pass

    def __iter__(self):
        return iter(self._nodes)

    def clear(self):
        self._nodes = {}
        self._edges = {}

    def copy(self):
        pass

    def _assert_exists(self, key):
        """ Raise an exception if a node identified by `key` does not exist
            within this graph.
        """
        if key not in self._edges:
            raise KeyError("Node '{0}' not found".format(key))

    def get(self, key, default=None):
        """ Return the value of the node identified by `key`.
        """
        try:
            return self._nodes[key]
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

    def find_all_paths(self, *waypoints):
        if len(waypoints) < 2:
            raise ValueError("At least two waypoints must be given for a path")
        def _find_all_paths(start, end, path):
            path = path + [start]
            if start == end:
                return [path]
            if start not in self._edges:
                return []
            paths = []
            for node in self._edges[start]:
                if node not in path:
                    new_paths = _find_all_paths(node, end, path)
                    for new_path in new_paths:
                        paths.append(new_path)
            return paths
        paths = []
        waypoints = list(waypoints)
        end = waypoints.pop(0)
        while waypoints:
            start, end = end, waypoints.pop(0)
            p = _find_all_paths(start, end, [])
            if p:
                if paths:
                    new_paths = []
                    for old_path in paths:
                        for new_path in p:
                            new_paths.append(old_path[:-1] + new_path)
                    paths = new_paths
                else:
                    paths = p
            else:
                return []
        return set([tuple(path) for path in paths])

    def find_shortest_path(self, *waypoints):
        if len(waypoints) < 2:
            raise ValueError("At least two waypoints must be given for a path")
        def _find_shortest_path(start, end, path):
            path = path + [start]
            if start == end:
                return path
            if start not in self._edges:
                return None
            shortest = None
            for node in self._edges[start]:
                if node not in path:
                    new_path = _find_shortest_path(node, end, path)
                    if new_path:
                        if not shortest or len(new_path) < len(shortest):
                            shortest = new_path
            return shortest
        path = []
        waypoints = list(waypoints)
        end = waypoints.pop(0)
        while waypoints:
            start, end = end, waypoints.pop(0)
            path = _find_shortest_path(start, end, path[:-1])
            if path is None:
                return None
        return tuple(path), [self._nodes[node] for node in path]
