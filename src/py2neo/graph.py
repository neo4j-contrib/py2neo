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
        self._nodes = {}
        self._structure = {}

    def __contains__(self, key):
        return key in self._structure

    def __getitem__(self, key):
        self._assert_exists(key)
        return self._nodes[key]

    def __setitem__(self, key, value):
        if key not in self._structure:
            self._nodes[key] = value
            self._structure[key] = {}

    def __delitem__(self, key):
        pass
        # has connections?

    def _assert_exists(self, node):
        if node not in self._structure:
            raise KeyError("Node '{0}' not found".format(node))

    def relate(self, start, relationship, end):
        self._assert_exists(start)
        self._assert_exists(end)
        if end not in self._structure[start]:
            self._structure[start][end] = {}
        if isinstance(relationship, tuple):
            self._structure[start][end][relationship[0]] = relationship[1]
        else:
            self._structure[start][end][relationship] = None

    def find_all_paths(self, start, end):
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
        return _find_all_paths(start, end, [])

    def find_shortest_path(self, start, end):
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
        return _find_shortest_path(start, end, [])


def __test__():
    graph = Graph()
    graph["Neo"] = {"name": "Neo"}
    graph["Morpheus"] = {"name": "Morpheus"}
    graph["Trinity"] = {"name": "Trinity"}
    graph["Cypher"] = {"name": "Cypher"}
    graph["Smith"] = {"name": "Agent Smith"}
    graph["Architect"] = {"name": "The Architect"}
    graph["Barney"] = {"name": "Barney the Dinosaur"}
    graph.relate("Neo", "KNOWS", "Morpheus")
    graph.relate("Neo", "KNOWS", "Trinity")
    graph.relate("Neo", "LOVES", "Trinity")
    graph.relate("Neo", ("LOVES", {"amount": "lots"}), "Trinity")
    graph.relate("Morpheus", "KNOWS", "Trinity")
    graph.relate("Morpheus", "KNOWS", "Cypher")
    graph.relate("Trinity", "KNOWS", "Cypher")
    graph.relate("Cypher", "KNOWS", "Smith")
    graph.relate("Smith", "CODED_BY", "Architect")
    graph.relate("Neo", "KILLS", "Smith")
    print graph._nodes
    print graph._structure
    print graph.find_all_paths("Neo", "Architect")
    print graph.find_shortest_path("Neo", "Architect")


if __name__ == "__main__":
    __test__()
