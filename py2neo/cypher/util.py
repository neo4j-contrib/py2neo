#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright 2011-2014, Nigel Small
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


from py2neo.error import GraphError


class StartOrMatch(object):

    def __init__(self, graph):
        if graph is None:
            raise GraphError("All auto-generated Cypher statements require a Graph instance")
        self.graph = graph
        self.nodes = []
        self.relationships = []

    def __len__(self):
        return len(self.nodes) + len(self.relationships)

    def node(self, name, selector):
        self.nodes.append((name, selector))
        return self

    def relationship(self, name, selector):
        self.relationships.append((name, selector))
        return self

    @property
    def string(self):
        if self.nodes or self.relationships:
            if self.graph.supports_start_clause:
                s = []
                for name, selector in self.nodes:
                    s.append("%s=node(%s)" % (name, selector))
                for name, selector in self.relationships:
                    s.append("%s=rel(%s)" % (name, selector))
                return "START " + ",".join(s) + "\n"
            else:
                s = []
                for name, selector in self.nodes:
                    if selector == "*":
                        s.append("MATCH (%s)\n" % name)
                    else:
                        s.append("MATCH (%s) WHERE id(%s)=%s\n" % (name, name, selector))
                for name, selector in self.relationships:
                    if selector == "*":
                        s.append("MATCH ()-[%s]->()\n" % name)
                    else:
                        s.append("MATCH ()-[%s]->() WHERE id(%s)=%s\n" % (name, name, selector))
                return "".join(s)
        else:
            return ""
