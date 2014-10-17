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


from py2neo.core import Graph, Node, NodePointer, Path, Relationship, Rev
from py2neo.cypher.lang import cypher_escape
from py2neo.util import ustr


__all__ = ["CreateStatement"]


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
        self.create_unique_clause = []
        self.return_clause = []
        self.parameters = {}

    def __repr__(self):
        return self.string

    @property
    def string(self):
        clauses = []
        if self.start_clause:
            clauses.append("START " + ",".join(self.start_clause))
        if self.create_clause:
            clauses.append("CREATE " + ",".join(self.create_clause))
        if self.create_unique_clause:
            clauses.append("CREATE UNIQUE " + ",".join(self.create_unique_clause))
        if self.return_clause:
            clauses.append("RETURN " + ",".join(self.return_clause))
        return "\n".join(clauses)

    def post(self):
        return self.graph.cypher.post(self.string, self.parameters)

    def execute(self):
        if not self.entities:
            return ()
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
        entity = Graph.cast(entity)
        index = len(self.entities)
        name = _(index)
        if isinstance(entity, Node):
            self.names.append(self.create_node(entity, name))
        elif isinstance(entity, Path):
            self.names.append(self.create_path(entity, name))
        else:
            raise TypeError("Cannot create entity of type %s" % type(entity).__name__)
        self.entities.append(entity)

    def create_unique(self, entity):
        entity = Graph.cast(entity)
        index = len(self.entities)
        name = _(index)
        if isinstance(entity, Path):
            self.names.append(self.create_unique_path(entity, name))
        else:
            raise TypeError("Cannot create unique entity of type %s" % type(entity).__name__)
        self.entities.append(entity)

    def _has_entity(self, entity):
        return any(e is entity for e in self.entities)

    def _node_pattern(self, node, name, full):
        template = "{name}"
        kwargs = {"name": name}
        if full:
            if node.labels and self.supports_node_labels:
                template += "{labels}"
                kwargs["labels"] = "".join(":" + cypher_escape(label) for label in node.labels)
            if node.properties:
                template += " {{{name}}}"
                self.parameters[name] = node.properties
        return "(" + template.format(**kwargs) + ")"

    def create_node(self, node, name):
        if node.bound:
            kwargs = {"name": name}
            self.start_clause.append("{name}=node({{{name}}})".format(**kwargs))
            self.parameters[name] = node._id
        else:
            self.create_clause.append(self._node_pattern(node, name, full=True))
        self.return_clause.append(name)
        return [name], []

    def _create_path_nodes(self, path, name, unique):
        node_names = []
        for i, node in enumerate(path.nodes):
            if isinstance(node, NodePointer):
                node_names.append(_(node.address))
                # Switch out node with object from elsewhere in entity list
                nodes = list(path.nodes)
                try:
                    node = self.entities[node.address]
                except IndexError:
                    raise IndexError("Node pointer out of range")
                if not isinstance(node, Node):
                    raise ValueError("Pointer does not refer to a node")
                nodes[i] = node
                path._Path__nodes = tuple(nodes)
            elif self._has_entity(node):
                node_name = _(self.entities.index(node))
                node_names.append(node_name)
            elif unique and not node.bound:
                node_name = name + "n" + ustr(i)
                node_names.append(node_name)
                self.return_clause.append(node_name)
            else:
                node_name = name + "n" + ustr(i)
                node_names.append(node_name)
                self.create_node(node, node_name)
        return node_names

    def _create_path(self, path, name, unique):
        node_names = self._create_path_nodes(path, name, unique)
        rel_names = []
        for i, rel in enumerate(path.rels):
            rel_name = name + "r" + ustr(i)
            rel_names.append(rel_name)
            if rel.bound:
                self.start_clause.append("{name}=rel({{{name}}})".format(name=rel_name))
                self.parameters[rel_name] = rel._id
            else:
                if rel.properties:
                    template = "{start}-[{name}:{type} {{{name}}}]->{end}"
                    self.parameters[rel_name] = rel.properties
                else:
                    template = "{start}-[{name}:{type}]->{end}"
                start_index, end_index = i, i + 1
                if isinstance(rel,  Rev):
                    start_index, end_index = end_index, start_index
                start_node = path.nodes[start_index]
                end_node = path.nodes[end_index]
                start = self._node_pattern(start_node, node_names[start_index],
                                           full=(unique and not start_node.bound and not self._has_entity(start_node)))
                end = self._node_pattern(end_node, node_names[end_index],
                                         full=(unique and not end_node.bound and not self._has_entity(end_node)))
                kwargs = {"start": start, "name": rel_name,
                          "type": cypher_escape(rel.type), "end": end}
                if unique:
                    self.create_unique_clause.append(template.format(**kwargs))
                else:
                    self.create_clause.append(template.format(**kwargs))
            self.return_clause.append(rel_name)
        return node_names, rel_names

    def create_path(self, path, name):
        return self._create_path(path, name, unique=False)

    def create_unique_path(self, path, name):
        if len(path) == 0:
            raise ValueError("Cannot create unique path with zero length")
        if not any(node.bound or self._has_entity(node) for node in path.nodes):
            raise ValueError("At least one node must be bound to create a unique path")
        return self._create_path(path, name, unique=True)
