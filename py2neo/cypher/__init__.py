#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2018, Nigel Small
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
This module provides a set of Cypher language utility functions that can
be useful when building Cypher statements and expressions.
"""

from __future__ import absolute_import

__all__ = [
    "cypher_escape",
    "cypher_repr",
    "cypher_str",
]

from collections import OrderedDict
from re import compile as re_compile
from unicodedata import category

from neotime import Date
from py2neo.internal.collections import SetView
from py2neo.internal.compat import uchr, ustr, numeric_types, string_types, unicode_types


ID_START = {u"_"} | {uchr(x) for x in range(0xFFFF)
                     if category(uchr(x)) in ("LC", "Ll", "Lm", "Lo", "Lt", "Lu", "Nl")}
ID_CONTINUE = ID_START | {uchr(x) for x in range(0xFFFF)
                          if category(uchr(x)) in ("Mn", "Mc", "Nd", "Pc", "Sc")}

DOUBLE_QUOTE = u'"'
SINGLE_QUOTE = u"'"

ESCAPED_DOUBLE_QUOTE = u'\\"'
ESCAPED_SINGLE_QUOTE = u"\\'"

X_ESCAPE = re_compile(r"(\\x([0-9a-f]{2}))")
DOUBLE_QUOTED_SAFE = re_compile(r"([ -!#-\[\]-~]+)")
SINGLE_QUOTED_SAFE = re_compile(r"([ -&(-\[\]-~]+)")


class LabelSetView(SetView):

    def __init__(self, elements=(), selected=(), **kwargs):
        super(LabelSetView, self).__init__(frozenset(elements))
        self.__selected = tuple(selected)
        self.__kwargs = kwargs

    def __repr__(self):
        if self.__selected:
            return "".join(":%s" % cypher_escape(e, **self.__kwargs) for e in self.__selected if e in self)
        else:
            return "".join(":%s" % cypher_escape(e, **self.__kwargs) for e in sorted(self))

    def __getattr__(self, element):
        if element in self.__selected:
            return self.__class__(self, self.__selected)
        else:
            return self.__class__(self, self.__selected + (element,))


class PropertyDictView(object):

    def __init__(self, items=(), selected=(), **kwargs):
        self.__items = dict(items)
        self.__selected = tuple(selected)
        self.__kwargs = kwargs

    def __repr__(self):
        if self.__selected:
            properties = OrderedDict((key, self.__items[key]) for key in self.__selected if key in self.__items)
        else:
            properties = OrderedDict((key, self.__items[key]) for key in sorted(self.__items))
        return cypher_repr(properties, **self.__kwargs)

    def __getattr__(self, key):
        if key in self.__selected:
            return self.__class__(self.__items, self.__selected)
        else:
            return self.__class__(self.__items, self.__selected + (key,))

    def __len__(self):
        return len(self.__items)

    def __iter__(self):
        return iter(self.__items)

    def __contains__(self, key):
        return key in self.__items


class PropertySelector(object):

    def __init__(self, items=(), default_value=None, **kwargs):
        self.__items = dict(items)
        self.__default_value = default_value
        self.__kwargs = kwargs

    def __getattr__(self, key):
        return cypher_str(self.__items.get(key, self.__default_value), **self.__kwargs)


class CypherEncoder(object):

    __default_instance = None

    def __new__(cls, *args, **kwargs):
        if not kwargs:
            if cls.__default_instance is None:
                cls.__default_instance = super(CypherEncoder, cls).__new__(cls)
            return cls.__default_instance
        return super(CypherEncoder, cls).__new__(cls)

    encoding = "utf-8"
    quote = None
    sequence_separator = u", "
    key_value_separator = u": "
    node_template = u"{id}{labels} {properties}"
    related_node_template = u"{name}"
    relationship_template = u"{type} {properties}"
    null = u"null"

    def __init__(self, encoding=None, quote=None, sequence_separator=None, key_value_separator=None,
                 node_template=None, related_node_template=None, relationship_template=None, null=None):
        if encoding:
            self.encoding = encoding
        if quote:
            self.quote = quote
        if sequence_separator:
            self.sequence_separator = sequence_separator
        if key_value_separator:
            self.key_value_separator = key_value_separator
        if node_template:
            self.node_template = node_template
        if related_node_template:
            self.related_node_template = related_node_template
        if relationship_template:
            self.relationship_template = relationship_template
        if null:
            self.null = null

    def encode_key(self, key):
        key = ustr(key)
        if not key:
            raise ValueError("Keys cannot be empty")
        if key[0] in ID_START and all(key[i] in ID_CONTINUE for i in range(1, len(key))):
            return key
        else:
            return u"`" + key.replace(u"`", u"``") + u"`"

    def encode_value(self, value):
        from py2neo.data import Node, Relationship, Path
        if value is None:
            return self.null
        if value is True:
            return u"true"
        if value is False:
            return u"false"
        if isinstance(value, numeric_types):
            return ustr(value)
        if isinstance(value, string_types):
            return self.encode_string(value)
        if isinstance(value, Node):
            return self.encode_node(value)
        if isinstance(value, Relationship):
            return self.encode_relationship(value)
        if isinstance(value, Path):
            return self.encode_path(value)
        if isinstance(value, list):
            return self.encode_list(value)
        if isinstance(value, dict):
            return self.encode_map(value)
        if isinstance(value, Date):
            return value.__str__()
        raise TypeError("Values of type %s.%s are not supported" %
                        (type(value).__module__, type(value).__name__))

    def encode_string(self, value):
        value = ustr(value)

        quote = self.quote
        if quote is None:
            num_single = value.count(u"'")
            num_double = value.count(u'"')
            quote = SINGLE_QUOTE if num_single <= num_double else DOUBLE_QUOTE

        if quote == SINGLE_QUOTE:
            escaped_quote = ESCAPED_SINGLE_QUOTE
            safe = SINGLE_QUOTED_SAFE
        elif quote == DOUBLE_QUOTE:
            escaped_quote = ESCAPED_DOUBLE_QUOTE
            safe = DOUBLE_QUOTED_SAFE
        else:
            raise ValueError("Unsupported quote character %r" % quote)

        if not value:
            return quote + quote

        parts = safe.split(value)
        for i in range(0, len(parts), 2):
            parts[i] = (X_ESCAPE.sub(u"\\\\u00\\2", parts[i].encode("unicode-escape").decode("utf-8")).
                        replace(quote, escaped_quote).replace(u"\\u0008", u"\\b").replace(u"\\u000c", u"\\f"))
        return quote + u"".join(parts) + quote

    def encode_list(self, values):
        return u"[" + self.sequence_separator.join(map(self.encode_value, values)) + u"]"

    def encode_map(self, values):
        return u"{" + self.sequence_separator.join(self.encode_key(key) + self.key_value_separator + self.encode_value(value)
                                                   for key, value in values.items()) + u"}"

    def encode_node(self, node):
        return self._encode_node(node, self.node_template)

    def encode_relationship(self, relationship):
        nodes = relationship.nodes
        return u"{}-{}->{}".format(
            self._encode_node(nodes[0], self.related_node_template),
            self._encode_relationship_detail(relationship, self.relationship_template),
            self._encode_node(nodes[-1], self.related_node_template),
        )

    def encode_path(self, path):
        encoded = []
        append = encoded.append
        nodes = path.nodes
        for i, relationship in enumerate(path.relationships):
            append(self._encode_node(nodes[i], self.related_node_template))
            related_nodes = relationship.nodes
            if self._node_id(related_nodes[0]) == self._node_id(nodes[i]):
                append(u"-")
                append(self._encode_relationship_detail(relationship, self.relationship_template))
                append(u"->")
            else:
                append(u"<-")
                append(self._encode_relationship_detail(relationship, self.relationship_template))
                append(u"-")
        append(self._encode_node(nodes[-1], self.related_node_template))
        return u"".join(encoded)

    @classmethod
    def _node_id(cls, node):
        return node.identity if hasattr(node, "identity") else node

    def _encode_node(self, node, template):
        return u"(" + template.format(
            id=u"" if node.identity is None else (u"_" + ustr(node.identity)),
            labels=LabelSetView(node.labels, encoding=self.encoding, quote=self.quote),
            properties=PropertyDictView(node, encoding=self.encoding, quote=self.quote),
            property=PropertySelector(node, u""),
            name=node.__name__,
        ).strip() + u")"

    def _encode_relationship_detail(self, relationship, template):
        return u"[" + template.format(
            id=u"" if relationship.identity is None else (u"_" + ustr(relationship.identity)),
            type=u":" + ustr(type(relationship).__name__),
            properties=PropertyDictView(relationship, encoding=self.encoding, quote=self.quote),
            property=PropertySelector(relationship, u""),
            name=relationship.__name__,
        ).strip() + u"]"


def cypher_escape(identifier, **kwargs):
    """ Return a Cypher identifier, with escaping if required.

    Simple Cypher identifiers, which just contain alphanumerics
    and underscores, can be represented as-is in expressions.
    Any which contain more esoteric characters, such as spaces
    or punctuation, must be escaped in backticks. Backticks
    themselves are escaped by doubling.

    ::

        >>> cypher_escape("simple_identifier")
        'simple_identifier'
        >>> cypher_escape("identifier with spaces")
        '`identifier with spaces`'
        >>> cypher_escape("identifier with `backticks`")
        '`identifier with ``backticks```'

    Identifiers are used in Cypher to denote named values, labels,
    relationship types and property keys. This function will typically
    be used to construct dynamic Cypher queries in places where
    parameters cannot be used.

        >>> "MATCH (a:{label}) RETURN id(a)".format(label=cypher_escape("Employee of the Month"))
        'MATCH (a:`Employee of the Month`) RETURN id(a)'

    :param identifier: any non-empty string
    """
    if not isinstance(identifier, string_types):
        raise TypeError(type(identifier).__name__)
    encoder = CypherEncoder(**kwargs)
    return encoder.encode_key(identifier)


def cypher_repr(value, **kwargs):
    """ Return the Cypher representation of a value.

    This function attempts to convert the supplied value into a Cypher
    literal form, as used in expressions.

    """
    encoder = CypherEncoder(**kwargs)
    return encoder.encode_value(value)


def cypher_str(value, **kwargs):
    """ Convert a Cypher value to a Python Unicode string.

    This function converts the supplied value into a string form, as
    used for human-readable output. This is generally identical to
    :meth:`.cypher_repr` except for with string values, which are
    returned as-is, instead of being enclosed in quotes with certain
    characters escaped.

    """
    if isinstance(value, unicode_types):
        return value
    elif isinstance(value, string_types):
        return value.decode(kwargs.get("encoding", "utf-8"))
    else:
        return cypher_repr(value, **kwargs)
