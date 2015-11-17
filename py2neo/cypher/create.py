#!/usr/bin/env python
# -*- encoding: utf-8 -*-

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


from py2neo.compat import ustr, xstr
from py2neo.core import Graph, Node, NodePointer, Path, Relationship, Rev
from py2neo.cypher.lang import cypher_escape


__all__ = ["CreateStatement"]


def _(*args):
    return "".join("_" + ustr(arg) for arg in args)


class CreateStatement(object):
    """ Builder for a Cypher CREATE/CREATE UNIQUE statement.
    """

    #: The graph against which this statement is to be executed.
    graph = None

    #: The parameters to inject into this statement.
    parameters = None

    def __init__(self, graph):
        self.graph = graph
        self.entities = []
        self.names = []
        self.initial_match_clause = []
        self.create_clause = []
        self.create_unique_clause = []
        self.return_clause = []
        self.parameters = {}

    def __repr__(self):
        return self.string

    def __str__(self):
        return xstr(self.__unicode__())

    def __unicode__(self):
        return self.string

    def __contains__(self, entity):
        return any(e is entity for e in self.entities)

    @property
    def string(self):
        """ The full Cypher statement as a string.
        """
        clauses = list(self.initial_match_clause)
        if self.create_clause:
            clauses.append("CREATE " + ",".join(self.create_clause))
        if self.create_unique_clause:
            clauses.append("CREATE UNIQUE " + ",".join(self.create_unique_clause))
        if self.return_clause:
            clauses.append("RETURN " + ",".join(self.return_clause))
        return "\n".join(clauses).strip()

    def execute(self):
        """ Execute this statement.

        :return: A tuple of created entities.

        """
        if not self.entities:
            return ()
        raw = self.graph.cypher.post(self.string, self.parameters)
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
                entity.start_node().bind(metadata["start"])
                entity.end_node().bind(metadata["end"])
            elif isinstance(entity, Path):
                for j, node in enumerate(entity.nodes()):
                    metadata = dehydrated[node_names[j]]
                    node.bind(metadata["self"], metadata)
                for j, rel in enumerate(entity.relationships()):
                    metadata = dehydrated[rel_names[j]]
                    rel.bind(metadata["self"], metadata)
        return tuple(self.entities)

    def create(self, entity):
        """ Append an entity to the CREATE clause of this statement.

        :arg entity: The entity to create.

        """
        entity = Graph.cast(entity)
        index = len(self.entities)
        name = _(index)
        if isinstance(entity, Node):
            self.names.append(self._create_node(entity, name))
        elif isinstance(entity, Relationship):
            self.names.append(self._create_relationship(entity, name, unique=False))
        elif isinstance(entity, Path):
            self.names.append(self._create_path(entity, name, unique=False))
        else:
            raise TypeError("Cannot create entity of type %s" % type(entity).__name__)
        self.entities.append(entity)

    def create_unique(self, entity):
        """ Append an entity to the CREATE UNIQUE clause of this statement.

        :arg entity: The entity to add.

        """
        entity = Graph.cast(entity)
        index = len(self.entities)
        name = _(index)
        if isinstance(entity, Relationship):
            if not any(node.bound or node in self for node in entity.nodes()):
                raise ValueError("At least one node must be bound to create a unique relationship")
            self.names.append(self._create_relationship(entity, name, unique=True))
        elif isinstance(entity, Path):
            if len(entity) == 0:
                raise ValueError("Cannot create unique path with zero length")
            if not any(node.bound or node in self for node in entity.nodes):
                raise ValueError("At least one node must be bound to create a unique path")
            self.names.append(self._create_path(entity, name, unique=True))
        else:
            raise TypeError("Cannot create unique entity of type %s" % type(entity).__name__)
        self.entities.append(entity)

    def _node_pattern(self, node, name, full):
        template = "{name}"
        kwargs = {"name": name}
        if full:
            if node.labels:
                template += "{labels}"
                kwargs["labels"] = "".join(":" + cypher_escape(label) for label in node.labels())
            if node:
                template += " {{{name}}}"
                self.parameters[name] = dict(node)
        return "(" + template.format(**kwargs) + ")"

    def _create_node(self, node, name):
        if node.bound:
            self.initial_match_clause.append("MATCH ({0}) "
                                             "WHERE id({0})={{{0}}}".format(name))
            self.parameters[name] = node._id
        else:
            self.create_clause.append(self._node_pattern(node, name, full=True))
        self.return_clause.append(name)
        return [name], []

    def _create_relationship_nodes(self, relationship, name, unique):
        nodes = [relationship.start_node(), relationship.end_node()]
        node_names = []
        for i, node in enumerate(nodes):
            if isinstance(node, NodePointer):
                node_names.append(_(node.address))
                # Switch out node with object from elsewhere in entity list
                try:
                    target_node = self.entities[node.address]
                except IndexError:
                    raise IndexError("Node pointer {%s} out of range" % node.address)
                if not isinstance(target_node, Node):
                    raise ValueError("Pointer {%s} does not refer to a node" % node.address)
                nodes[i] = target_node
            elif node in self:
                node_name = _(self.entities.index(node))
                node_names.append(node_name)
            elif unique and not node.bound:
                node_name = name + "n" + ustr(i)
                node_names.append(node_name)
                self.return_clause.append(node_name)
            else:
                node_name = name + "n" + ustr(i)
                node_names.append(node_name)
                self._create_node(node, node_name)
        relationship.__init__(nodes[0], relationship._type, nodes[1], **relationship)
        return node_names

    def _create_path_nodes(self, path, name, unique):
        node_names = []
        for i, node in enumerate(path.nodes()):
            if isinstance(node, NodePointer):
                node_names.append(_(node.address))
                # Switch out node with object from elsewhere in entity list
                nodes = list(path.nodes())
                try:
                    target_node = self.entities[node.address]
                except IndexError:
                    raise IndexError("Node pointer {%s} out of range" % node.address)
                if not isinstance(target_node, Node):
                    raise ValueError("Pointer {%s} does not refer to a node" % node.address)
                nodes[i] = target_node
                path._Path__nodes = tuple(nodes)
            elif node in self:
                node_name = _(self.entities.index(node))
                node_names.append(node_name)
            elif unique and not node.bound:
                node_name = name + "n" + ustr(i)
                node_names.append(node_name)
                self.return_clause.append(node_name)
            else:
                node_name = name + "n" + ustr(i)
                node_names.append(node_name)
                self._create_node(node, node_name)
        return node_names

    def _create_path(self, path, name, unique):
        node_names = self._create_path_nodes(path, name, unique)
        rel_names = []
        for i, rel in enumerate(path.relationships()):
            rel_name = name + "r" + ustr(i)
            rel_names.append(rel_name)
            if rel.bound:
                self.initial_match_clause.append("MATCH ()-[{0}]->() "
                                                 "WHERE id({0})={{{0}}}".format(rel_name))
                self.parameters[rel_name] = rel._id
            else:
                if rel:
                    template = "{start}-[{name}:{type} {{{name}}}]->{end}"
                    self.parameters[rel_name] = rel.properties
                else:
                    template = "{start}-[{name}:{type}]->{end}"
                start_index, end_index = i, i + 1
                if isinstance(rel,  Rev):
                    start_index, end_index = end_index, start_index
                start_node = path.nodes()[start_index]
                end_node = path.nodes()[end_index]
                start = self._node_pattern(start_node, node_names[start_index],
                                           full=(unique and not start_node.bound and start_node not in self))
                end = self._node_pattern(end_node, node_names[end_index],
                                         full=(unique and not end_node.bound and end_node not in self))
                kwargs = {"start": start, "name": rel_name,
                          "type": cypher_escape(rel.type()), "end": end}
                if unique:
                    self.create_unique_clause.append(template.format(**kwargs))
                else:
                    self.create_clause.append(template.format(**kwargs))
            self.return_clause.append(rel_name)
        return node_names, rel_names

    def _create_relationship(self, relationship, name, unique):
        node_names = self._create_relationship_nodes(relationship, name, unique)
        rel_name = name + "r"
        if relationship.bound:
            self.initial_match_clause.append("MATCH ()-[{0}]->() "
                                             "WHERE id({0})={{{0}}}".format(rel_name))
            self.parameters[rel_name] = relationship._id
        else:
            if relationship:
                template = "{start}-[{name}:{type} {{{name}}}]->{end}"
                self.parameters[rel_name] = dict(relationship)
            else:
                template = "{start}-[{name}:{type}]->{end}"
            start_node = relationship.start_node()
            end_node = relationship.end_node()
            start = self._node_pattern(start_node, node_names[0],
                                       full=(unique and not start_node.bound and start_node not in self))
            end = self._node_pattern(end_node, node_names[1],
                                     full=(unique and not end_node.bound and end_node not in self))
            kwargs = {"start": start, "name": rel_name,
                      "type": cypher_escape(relationship.type()), "end": end}
            if unique:
                self.create_unique_clause.append(template.format(**kwargs))
            else:
                self.create_clause.append(template.format(**kwargs))
        self.return_clause.append(rel_name)
        return node_names, [rel_name]
