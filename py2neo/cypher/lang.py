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


from __future__ import unicode_literals

import json

from py2neo.core import Node, Rel, Rev, Path, Relationship
from py2neo.util import is_collection


__all__ = ["Representation", "cypher_escape", "cypher_repr"]


class Representation(object):

    safe_first_chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz_"
    safe_chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz_"

    default_sequence_separator = ","
    default_key_value_separator = ":"

    def __init__(self, **kwargs):
        self.__buffer = []
        self.sequence_separator = kwargs.get("sequence_separator", self.default_sequence_separator)
        self.key_value_separator = kwargs.get("key_value_separator",
                                              self.default_key_value_separator)

    def __repr__(self):
        return "".join(self.__buffer)

    def write_value(self, value):
        self.__buffer.append(json.dumps(value))

    def write_identifier(self, identifier):
        if not identifier:
            raise ValueError("Invalid identifier")
        safe = (identifier[0] in self.safe_first_chars and
                all(ch in self.safe_chars for ch in identifier[1:]))
        if not safe:
            self.__buffer.append("`")
            self.__buffer.append(identifier.replace("`", "``"))
            self.__buffer.append("`")
        else:
            self.__buffer.append(identifier)

    def write_collection(self, collection):
        self.__buffer.append("[")
        link = ""
        for value in collection:
            self.__buffer.append(link)
            self.write(value)
            link = self.sequence_separator
        self.__buffer.append("]")

    def write_mapping(self, mapping):
        self.__buffer.append("{")
        link = ""
        for key, value in sorted(mapping.items()):
            self.__buffer.append(link)
            self.write_identifier(key)
            self.__buffer.append(self.key_value_separator)
            self.write(value)
            link = self.sequence_separator
        self.__buffer.append("}")

    def write_node(self, node, name=None):
        self.__buffer.append("(")
        if name:
            self.write_identifier(name)
        if node is not None:
            for label in sorted(node.labels):
                self.__buffer.append(":")
                self.write_identifier(label)
            if node.properties:
                if name or node.labels:
                    self.__buffer.append(" ")
                self.write_mapping(node.properties)
        self.__buffer.append(")")

    def write_rel(self, rel, name=None):
        if isinstance(rel, Rev):
            self.__buffer.append("<-[")
        else:
            self.__buffer.append("-[")
        if name:
            self.write_identifier(name)
        self.__buffer.append(":")
        self.write_identifier(rel.type)
        if rel.properties:
            self.__buffer.append(" ")
            self.write_mapping(rel.properties)
        if isinstance(rel, Rev):
            self.__buffer.append("]-")
        else:
            self.__buffer.append("]->")

    def write_relationship(self, relationship, name=None):
        self.write_node(relationship.start_node)
        self.write_rel(relationship.rel, name)
        self.write_node(relationship.end_node)

    def write_path(self, path):
        nodes = path.nodes
        self.write_node(nodes[0])
        for i, rel in enumerate(path.rels):
            self.write_rel(rel)
            self.write_node(nodes[i + 1])

    def write(self, obj):
        if obj is None:
            pass
        elif isinstance(obj, Node):
            self.write_node(obj)
        elif isinstance(obj, Rel):
            self.write_rel(obj)
        elif isinstance(obj, Relationship):
            self.write_relationship(obj)
        elif isinstance(obj, Path):
            self.write_path(obj)
        elif isinstance(obj, dict):
            self.write_mapping(obj)
        elif is_collection(obj):
            self.write_collection(obj)
        else:
            self.write_value(obj)


def cypher_escape(identifier):
    r = Representation()
    r.write_identifier(identifier)
    return repr(r)


def cypher_repr(obj):
    r = Representation()
    r.write(obj)
    return repr(r)
