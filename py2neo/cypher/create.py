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


from py2neo.core import Node, NodePointer, Path, Relationship
from py2neo.cypher.lang import cypher_escape
from py2neo.util import ustr


def _(*args):
    return "".join("_" + ustr(arg) for arg in args)


class CreateStatement(object):

    def __init__(self, graph):
        self.graph = graph
        self.supports_node_labels = self.graph.supports_node_labels
        self.entities = []
        self.names = []
        self.start_clause = []
        self.create_clause = []
        self.return_clause = []
        self.params = {}

    def __repr__(self):
        return self.string

    @property
    def string(self):
        clauses = []
        if self.start_clause:
            clauses.append("START " + ",".join(self.start_clause))
        if self.create_clause:
            clauses.append("CREATE " + ",".join(self.create_clause))
        if self.return_clause:
            clauses.append("RETURN " + ",".join(self.return_clause))
        return "\n".join(clauses)

    def post(self):
        return self.graph.cypher.post(self.string, self.params)

    def execute(self):
        if not self.entities:
            return []
        raw = self.post().content
        columns = raw["columns"]
        data = raw["data"]
        dehydrated = dict(zip(columns, data[0]))
        for i, entity in enumerate(self.entities):
            node_names, rel_names = self.names[i]
            if isinstance(entity, Node):
                metadata = dehydrated[node_names[0]]
                entity.bind(metadata["self"], metadata)
            elif isinstance(entity, Relationship):
                metadata = dehydrated[rel_names[0]]
                entity.bind(metadata["self"], metadata)
            elif isinstance(entity, Path):
                for j, node in enumerate(entity.nodes):
                    metadata = dehydrated[node_names[j]]
                    node.bind(metadata["self"], metadata)
                for j, rel in enumerate(entity.rels):
                    metadata = dehydrated[rel_names[j]]
                    rel.bind(metadata["self"], metadata)
        return tuple(self.entities)

    def create(self, entity):
        entity = self.graph.cast(entity)
        index = len(self.entities)
        name = _(index)
        if isinstance(entity, Node):
            self.names.append(self.create_node(name, entity))
        elif isinstance(entity, Path):
            self.names.append(self.create_path(name, entity))
        else:
            raise TypeError("Cannot create entity of type %s" % type(entity).__name__)
        self.entities.append(entity)

    def create_node(self, name, node):
        if node.bound:
            self.start_clause.append("{name}=node({{{name}}})".format(name=name))
            self.params[name] = node._id
        else:
            template = "{name}"
            kwargs = {"name": name}
            if node.labels and self.supports_node_labels:
                template += "{labels}"
                kwargs["labels"] = "".join(":" + cypher_escape(label) for label in node.labels)
            if node.properties:
                template += " {{{name}}}"
                self.params[name] = node.properties
            self.create_clause.append("(" + template.format(**kwargs) + ")")
        self.return_clause.append(name)
        return [name], []

    def create_rel(self, name, rel, *node_names):
        if rel.bound:
            self.start_clause.append("{name}=rel({{{name}}})".format(name=name))
            self.params[name] = rel._id
        else:
            if rel.properties:
                template = "({0})-[{1}:{2} {{{1}}}]->({3})"
                self.params[name] = rel.properties
            else:
                template = "({0})-[{1}:{2}]->({3})"
            self.create_clause.append(template.format(node_names[0], name, cypher_escape(rel.type), node_names[1]))
        self.return_clause.append(name)
        return [], [name]

    def create_path(self, name, path):
        node_names = [None] * len(path.nodes)
        rel_names = [None] * len(path.rels)
        for i, node in enumerate(path.nodes):
            if isinstance(node, NodePointer):
                node_names[i] = _(node.address)
                # Switch out node with object from elsewhere in entity list
                nodes = list(path.nodes)
                node = self.entities[node.address]
                if not isinstance(node, Node):
                    raise ValueError("Pointer does not refer to a node")
                nodes[i] = node
                path.__nodes = tuple(nodes)
            else:
                node_names[i] = name + "n" + ustr(i)
                self.create_node(node_names[i], node)
        for i, rel in enumerate(path.rels):
            rel_names[i] = name + "r" + ustr(i)
            self.create_rel(rel_names[i], rel, node_names[i], node_names[i + 1])
        return node_names, rel_names
