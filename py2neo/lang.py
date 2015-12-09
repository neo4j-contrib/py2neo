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


from __future__ import unicode_literals

from io import StringIO
import json
import sys

from py2neo.compat import ustr, xstr, integer
from py2neo.core import Node, Path, Relationship
from py2neo.primitive import Record
from py2neo.util import is_collection

__all__ = list(map(xstr, ["Writer", "CypherWriter", "cypher_escape", "cypher_repr"]))


class Writer(object):

    def __init__(self, file=None):
        self.file = file or sys.stdout

    def write(self, obj):
        raise NotImplementedError("Method not implemented")


class TextTable(object):

    @classmethod
    def cell(cls, value, size):
        if value == "#" or isinstance(value, (integer, float, complex)):
            text = ustr(value).rjust(size)
        else:
            text = ustr(value).ljust(size)
        return text

    def __init__(self, header, border=False):
        self.__header = list(map(ustr, header))
        self.__rows = []
        self.__widths = list(map(len, self.__header))
        self.__repr = None
        self.border = border

    def __repr__(self):
        if self.__repr is None:
            widths = self.__widths
            if self.border:
                lines = [
                    " " + " | ".join(self.cell(value, widths[i]) for i, value in enumerate(self.__header)) + "\n",
                    "-" + "-+-".join("-" * widths[i] for i, value in enumerate(self.__header)) + "-\n",
                ]
                for row in self.__rows:
                    lines.append(" " + " | ".join(self.cell(value, widths[i]) for i, value in enumerate(row)) + "\n")
            else:
                lines = [
                    " ".join(self.cell(value, widths[i]) for i, value in enumerate(self.__header)) + "\n",
                ]
                for row in self.__rows:
                    lines.append(" ".join(self.cell(value, widths[i]) for i, value in enumerate(row)) + "\n")
            self.__repr = "".join(lines)
            if sys.version_info < (3,):
                self.__repr = self.__repr.encode("utf-8")
        return self.__repr

    def append(self, row):
        row = list(row)
        self.__rows.append(row)
        self.__widths = [max(self.__widths[i], len(ustr(value))) for i, value in enumerate(row)]
        self.__repr = None


class CypherWriter(Writer):
    """ Writer for Cypher data. This can be used to write to any
    file-like object, such as standard output::

        >>> from py2neo.lang import CypherWriter        >>> from py2neo import Node
        >>> import sys
        >>> writer = CypherWriter(sys.stdout)
        >>> writer.write(Node("Person", name="Alice"))
        (:Person {name:"Alice"})

    """

    safe_first_chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz_"
    safe_chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz_"

    default_sequence_separator = ","
    default_key_value_separator = ":"

    def __init__(self, file=None, **kwargs):
        Writer.__init__(self, file)
        self.sequence_separator = kwargs.get("sequence_separator", self.default_sequence_separator)
        self.key_value_separator = \
            kwargs.get("key_value_separator", self.default_key_value_separator)

    def write(self, obj):
        """ Write any entity, value or collection.
        """
        if obj is None:
            pass
        elif isinstance(obj, Node):
            self.write_node(obj)
        elif isinstance(obj, Relationship):
            self.write_relationship(obj, properties=obj)
        elif isinstance(obj, Path):
            self.write_path(obj)
        elif isinstance(obj, Record):
            self.write_record(obj)
        elif isinstance(obj, dict):
            self.write_map(obj)
        elif is_collection(obj):
            self.write_list(obj)
        else:
            self.write_value(obj)

    def write_value(self, value):
        """ Write a value.
        """
        self.file.write(ustr(json.dumps(value, ensure_ascii=False)))

    def write_identifier(self, identifier):
        """ Write an identifier.
        """
        if not identifier:
            raise ValueError("Invalid identifier")
        identifier = ustr(identifier)
        safe = (identifier[0] in self.safe_first_chars and
                all(ch in self.safe_chars for ch in identifier[1:]))
        if not safe:
            self.file.write("`")
            self.file.write(identifier.replace("`", "``"))
            self.file.write("`")
        else:
            self.file.write(identifier)

    def write_list(self, collection):
        """ Write a list.
        """
        self.file.write("[")
        link = ""
        for value in collection:
            self.file.write(link)
            self.write(value)
            link = self.sequence_separator
        self.file.write("]")

    def write_literal(self, text):
        """ Write literal text.
        """
        self.file.write(ustr(text))

    def write_map(self, mapping):
        """ Write a map.
        """
        self.file.write("{")
        link = ""
        for key, value in sorted(dict(mapping).items()):
            self.file.write(link)
            self.write_identifier(key)
            self.file.write(self.key_value_separator)
            self.write(value)
            link = self.sequence_separator
        self.file.write("}")

    def write_node(self, node, name=None, properties=None):
        """ Write a node.
        """
        self.file.write("(")
        if name:
            self.write_identifier(name)
        if node is not None:
            for label in sorted(node.labels()):
                self.write_literal(":")
                self.write_identifier(label)
            if properties is None:
                if node:
                    if name or node.labels():
                        self.file.write(" ")
                    self.write_map(dict(node))
            else:
                self.file.write(" ")
                self.write(properties)
        self.file.write(")")

    def write_relationship_detail(self, name=None, type=None, properties=None):
        """ Write a relationship (excluding nodes).
        """
        self.file.write("[")
        if name:
            self.write_identifier(name)
        if type:
            self.file.write(":")
            self.write_identifier(type)
        if properties:
            self.file.write(" ")
            self.write_map(properties)
        self.file.write("]")

    def write_relationship(self, relationship, name=None, properties=None):
        """ Write a relationship (including nodes).
        """
        self.write_node(relationship.start_node())
        self.file.write("-")
        self.write_relationship_detail(name, relationship.type(), properties)
        self.file.write("->")
        self.write_node(relationship.end_node())

    def write_path(self, path):
        """ Write a :class:`py2neo.Path`.
        """
        nodes = path.nodes()
        for i, relationship in enumerate(path):
            node = nodes[i]
            self.write_node(node)
            forward = relationship.start_node() == node
            if forward:
                self.file.write("-")
            else:
                self.file.write("<-")
            self.write_relationship_detail(type=relationship.type(), properties=relationship)
            if forward:
                self.file.write("->")
            else:
                self.file.write("-")
        self.write_node(nodes[-1])

    def write_record(self, record):
        out = ""
        keys = record.keys()
        if keys:
            table = TextTable(keys, border=True)
            table.append(record.values())
            out = repr(table)
        self.file.write(out)


def cypher_escape(identifier):
    """ Escape a Cypher identifier in backticks.

    ::

        >>> cypher_escape("this is a `label`")
        '`this is a ``label```'

    """
    string = StringIO()
    writer = CypherWriter(string)
    writer.write_identifier(identifier)
    return string.getvalue()


def cypher_repr(obj):
    """ Generate the Cypher representation of an object.
    """
    string = StringIO()
    writer = CypherWriter(string)
    writer.write(obj)
    return string.getvalue()
