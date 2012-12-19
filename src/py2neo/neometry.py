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

from itertools import product


class Graph(object):

    def __init__(self):
        self._nodes = {}
        self._structure = {}

    def __len__(self):
        return len(self._structure)

    def __nonzero__(self):
        return bool(self._structure)

    def __contains__(self, key):
        """ Return :py:const:`True` if the node identified by `key` exists
            within this graph.
        """
        return key in self._structure

    def __getitem__(self, key):
        """ Return the value of the node identified by `key`.
        """
        self._assert_exists(key)
        return self._nodes[key]

    def __setitem__(self, key, value):
        """ Insert or update node identified by `key`, setting the value to
            `value`.
        """
        if key not in self._structure:
            self._nodes[key] = value
            self._structure[key] = {}

    def __delitem__(self, key):
        pass
        # has connections?

    def _assert_exists(self, key):
        """ Raise an exception if a node identified by `key` does not exist
            within this graph.
        """
        if key not in self._structure:
            raise KeyError("Node '{0}' not found".format(key))

    def match(self, subject=None, predicate=None, object=None, value=None):
        """ Match and return a dictionary of all relationships which fulfil the
            criteria specified.
        """
        if object:
            self._assert_exists(object)
        if subject:
            self._assert_exists(subject)
            return dict(
                ((subject, p, o), v)
                for o in self._structure[subject]
                for p, v in self._structure[subject][o].items()
                if object is None or object == o
                if predicate is None or predicate == p
                if value is None or value == v
            )
        else:
            return dict(
                ((s, p, o), v)
                for s in self._structure
                for o in self._structure[s]
                for p, v in self._structure[s][o].items()
                if object is None or object == o
                if predicate is None or predicate == p
                if value is None or value == v
            )

    def relate(self, subject, predicate, object, value=None):
        """ Establish a relationship between two nodes, optionally assigning
            a value to that relationship.
        """
        self._assert_exists(subject)
        self._assert_exists(object)
        if object not in self._structure[subject]:
            self._structure[subject][object] = {}
        self._structure[subject][object][predicate] = value

    def remove(self, subject=None, predicate=None, object=None, value=None):
        """ Disestablish one or more relationships which match the criteria
            specified.
        """
        for s, p, o in self.match(subject, predicate, object, value):
            del self._structure[s][o][p]

    def find_all_paths(self, *waypoints):
        if len(waypoints) < 2:
            raise ValueError("At least two waypoints must be given for a path")
        def _find_all_paths(start, end, path):
            path = path + [start]
            if start == end:
                return [path]
            if start not in self._structure:
                return []
            paths = []
            for node in self._structure[start]:
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
            if start not in self._structure:
                return None
            shortest = None
            for node in self._structure[start]:
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
