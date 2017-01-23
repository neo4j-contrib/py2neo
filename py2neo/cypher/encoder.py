#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2017, Nigel Small
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


from __future__ import absolute_import

from re import compile as re_compile
from unicodedata import category

from py2neo.compat import number, string, unicode, unichr
from py2neo.types import Node, Relationship, Walkable, LabelSetView, PropertyDictView, PropertySelector

NULL = u"null"
TRUE = u"true"
FALSE = u"false"

ID_START = {u"_"} | {unichr(x) for x in range(0xFFFF)
                     if category(unichr(x)) in ("LC", "Ll", "Lm", "Lo", "Lt", "Lu", "Nl")}
ID_CONTINUE = ID_START | {unichr(x) for x in range(0xFFFF)
                          if category(unichr(x)) in ("Mn", "Mc", "Nd", "Pc", "Sc")}

DOUBLE_QUOTE = u'"'
SINGLE_QUOTE = u"'"

ESCAPED_DOUBLE_QUOTE = u'\\"'
ESCAPED_SINGLE_QUOTE = u"\\'"

X_ESCAPE = re_compile(r"(\\x([0-9a-f]{2}))")
DOUBLE_QUOTED_SAFE = re_compile(r"([ -!#-\[\]-~]+)")
SINGLE_QUOTED_SAFE = re_compile(r"([ -&(-\[\]-~]+)")


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
    node_template = u"{labels} {properties}"
    related_node_template = u"{property.name}"
    relationship_template = u"{type} {properties}"

    def __init__(self, encoding=None, quote=None, sequence_separator=None, key_value_separator=None,
                 node_template=None, related_node_template=None, relationship_template=None):
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

    def encode_key(self, key):
        if isinstance(key, bytes):
            key = key.decode(self.encoding)
        assert isinstance(key, unicode)
        if not key:
            raise ValueError("Keys cannot be empty")
        if key[0] in ID_START and all(key[i] in ID_CONTINUE for i in range(1, len(key))):
            return key
        else:
            return u"`" + key.replace(u"`", u"``") + u"`"

    def encode_value(self, value):
        if value is None:
            return NULL
        if value is True:
            return TRUE
        if value is False:
            return FALSE
        if isinstance(value, number):
            return unicode(value)
        if isinstance(value, string):
            return self.encode_string(value)
        if isinstance(value, Node):
            return self.encode_node(value)
        if isinstance(value, Relationship):
            return self.encode_relationship(value)
        if isinstance(value, Walkable):
            return self.encode_path(value)
        if isinstance(value, list):
            return self.encode_list(value)
        if isinstance(value, dict):
            return self.encode_map(value)
        raise TypeError("Values of type %s are not supported" % value.__class__.__name__)

    def encode_string(self, value):
        if isinstance(value, bytes):
            value = value.decode(self.encoding)
        assert isinstance(value, unicode)

        quote = self.quote
        if quote is None:
            quote = DOUBLE_QUOTE if SINGLE_QUOTE in value and DOUBLE_QUOTE not in value else SINGLE_QUOTE

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
        return u"{}-{}->{}".format(
            self._encode_node(relationship.start_node(), self.related_node_template),
            self._encode_relationship_detail(relationship, self.relationship_template),
            self._encode_node(relationship.end_node(), self.related_node_template),
        )

    def encode_path(self, path):
        last_node = path.start_node()
        encoded = [self._encode_node(last_node, self.related_node_template)]
        append = encoded.append
        for relationship in path.relationships():
            if relationship.start_node() == last_node:
                append(u"-")
                append(self._encode_relationship_detail(relationship, self.relationship_template))
                append(u"->")
                last_node = relationship.end_node()
            else:
                append(u"<-")
                append(self._encode_relationship_detail(relationship, self.relationship_template))
                append(u"-")
                last_node = relationship.start_node()
            append(self._encode_node(last_node, self.related_node_template))
        return u"".join(encoded)

    def _encode_node(self, node, template):
        return u"(" + template.format(
            labels=LabelSetView(node.labels(), encoding=self.encoding, quote=self.quote),
            properties=PropertyDictView(node, encoding=self.encoding, quote=self.quote),
            property=PropertySelector(node, u""),
        ).strip() + u")"

    def _encode_relationship_detail(self, relationship, template):
        return u"[" + template.format(
            type=u":" + relationship.type(),
            properties=PropertyDictView(relationship, encoding=self.encoding, quote=self.quote),
            property=PropertySelector(relationship, u""),
        ).strip() + u"]"
