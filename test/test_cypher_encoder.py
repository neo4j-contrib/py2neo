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


from collections import OrderedDict
from unittest import TestCase

from py2neo.cypher import cypher_repr, cypher_escape
from py2neo.types import Node, Relationship, Path


class CypherEscapeTestCase(TestCase):

    def test_can_write_simple_identifier(self):
        escaped = cypher_escape("foo")
        assert escaped == "foo"

    def test_can_write_identifier_with_odd_chars(self):
        escaped = cypher_escape("foo bar")
        assert escaped == "`foo bar`"

    def test_can_write_identifier_containing_back_ticks(self):
        escaped = cypher_escape("foo `bar`")
        assert escaped == "`foo ``bar```"

    def test_cannot_write_empty_identifier(self):
        with self.assertRaises(ValueError):
            _ = cypher_escape("")

    def test_cannot_write_none_identifier(self):
        with self.assertRaises(TypeError):
            _ = cypher_escape(None)


class CypherNoneRepresentationTestCase(TestCase):

    def test_should_encode_none(self):
        encoded = cypher_repr(None)
        assert encoded == u"null"


class CypherBooleanRepresentationTestCase(TestCase):

    def test_should_encode_true(self):
        encoded = cypher_repr(True)
        assert encoded == u"true"

    def test_should_encode_false(self):
        encoded = cypher_repr(False)
        assert encoded == u"false"


class CypherIntegerRepresentationTestCase(TestCase):

    def test_should_encode_zero(self):
        encoded = cypher_repr(0)
        assert encoded == u"0"

    def test_should_encode_positive_integer(self):
        encoded = cypher_repr(123)
        assert encoded == u"123"

    def test_should_encode_negative_integer(self):
        encoded = cypher_repr(-123)
        assert encoded == u"-123"


class CypherFloatRepresentationTestCase(TestCase):

    def test_should_encode_zero(self):
        encoded = cypher_repr(0.0)
        assert encoded == u"0.0"

    def test_should_encode_positive_float(self):
        encoded = cypher_repr(123.456)
        assert encoded == u"123.456"

    def test_should_encode_negative_float(self):
        encoded = cypher_repr(-123.456)
        assert encoded == u"-123.456"


class CypherStringRepresentationTestCase(TestCase):

    def test_should_encode_bytes(self):
        encoded = cypher_repr(b"hello, world")
        assert encoded == u"'hello, world'"

    def test_should_encode_unicode(self):
        encoded = cypher_repr(u"hello, world")
        assert encoded == u"'hello, world'"

    def test_should_encode_bytes_with_escaped_chars(self):
        encoded = cypher_repr(b"hello, 'world'", quote=u"'")
        assert encoded == u"'hello, \\'world\\''"

    def test_should_encode_unicode_with_escaped_chars(self):
        encoded = cypher_repr(u"hello, 'world'", quote=u"'")
        assert encoded == u"'hello, \\'world\\''"

    def test_should_encode_empty_string(self):
        encoded = cypher_repr(u"")
        assert encoded == u"''"

    def test_should_encode_bell(self):
        encoded = cypher_repr(u"\a")
        assert encoded == u"'\\u0007'"

    def test_should_encode_backspace(self):
        encoded = cypher_repr(u"\b")
        assert encoded == u"'\\b'"

    def test_should_encode_form_feed(self):
        encoded = cypher_repr(u"\f")
        assert encoded == u"'\\f'"

    def test_should_encode_new_line(self):
        encoded = cypher_repr(u"\n")
        assert encoded == u"'\\n'"

    def test_should_encode_carriage_return(self):
        encoded = cypher_repr(u"\r")
        assert encoded == u"'\\r'"

    def test_should_encode_horizontal_tab(self):
        encoded = cypher_repr(u"\t")
        assert encoded == u"'\\t'"

    def test_should_encode_double_quote_when_single_quoted(self):
        encoded = cypher_repr(u"\"")
        assert encoded == u"'\"'"

    def test_should_encode_single_quote_when_single_quoted(self):
        encoded = cypher_repr(u"'", quote=u"'")
        assert encoded == u"'\\''"

    def test_should_encode_double_quote_when_double_quoted(self):
        encoded = cypher_repr(u"\"", quote=u"\"")
        assert encoded == u'"\\""'

    def test_should_encode_single_quote_when_double_quoted(self):
        encoded = cypher_repr(u"'", quote=u"\"")
        assert encoded == u'"\'"'

    def test_should_encode_2_byte_extended_character(self):
        encoded = cypher_repr(u"\xAB")
        assert encoded == u"'\\u00ab'"

    def test_should_encode_4_byte_extended_character(self):
        encoded = cypher_repr(u"\uABCD")
        assert encoded == u"'\\uabcd'"

    def test_should_encode_8_byte_extended_character(self):
        encoded = cypher_repr(u"\U0010ABCD")
        assert encoded == u"'\\U0010abcd'"

    def test_should_encode_complex_sequence(self):
        encoded = cypher_repr(u"'  '' '''")
        assert encoded == u"\"'  '' '''\""


class CypherListRepresentationTestCase(TestCase):

    def test_should_encode_list(self):
        encoded = cypher_repr([1, 2.0, u"three"])
        assert encoded == u"[1, 2.0, 'three']"

    def test_should_encode_empty_list(self):
        encoded = cypher_repr([])
        assert encoded == u"[]"


class CypherMapRepresentationTestCase(TestCase):

    def test_should_encode_map(self):
        encoded = cypher_repr(OrderedDict([("one", 1), ("two", 2.0), ("number three", u"three")]))
        assert encoded == u"{one: 1, two: 2.0, `number three`: 'three'}"

    def test_should_encode_empty_map(self):
        encoded = cypher_repr({})
        assert encoded == u"{}"


class CypherNodeRepresentationTestCase(TestCase):

    def test_should_encode_empty_node(self):
        a = Node()
        encoded = cypher_repr(a)
        assert encoded == u"({})"

    def test_should_encode_node_with_property(self):
        a = Node(name="Alice")
        encoded = cypher_repr(a)
        assert encoded == u"({name: 'Alice'})"

    def test_should_encode_node_with_label(self):
        a = Node("Person")
        encoded = cypher_repr(a)
        assert encoded == u"(:Person {})"

    def test_should_encode_node_with_label_and_property(self):
        a = Node("Person", name="Alice")
        encoded = cypher_repr(a)
        assert encoded == u"(:Person {name: 'Alice'})"


class CypherRelationshipRepresentationTestCase(TestCase):

    def test_can_encode_relationship(self):
        a = Node()
        b = Node()
        ab = Relationship(a, "TO", b)
        encoded = cypher_repr(ab)
        assert encoded == "()-[:TO {}]->()"

    def test_can_encode_relationship_with_names(self):
        a = Node("Person", name="Alice")
        b = Node("Person", name="Bob")
        ab = Relationship(a, "KNOWS", b)
        encoded = cypher_repr(ab)
        assert encoded == "(Alice)-[:KNOWS {}]->(Bob)"

    def test_can_encode_relationship_with_alternative_names(self):
        a = Node("Person", nom=u"Aimée")
        b = Node("Person", nom=u"Baptiste")
        ab = Relationship(a, u"CONNAÎT", b)
        encoded = cypher_repr(ab, related_node_template=u"{property.nom}")
        assert encoded == u"(Aimée)-[:CONNAÎT {}]->(Baptiste)"

    def test_can_encode_relationship_with_properties(self):
        a = Node("Person", name="Alice")
        b = Node("Person", name="Bob")
        ab = Relationship(a, "KNOWS", b, since=1999)
        encoded = cypher_repr(ab)
        assert encoded == "(Alice)-[:KNOWS {since: 1999}]->(Bob)"


class CypherPathRepresentationTestCase(TestCase):

    def test_can_write_path(self):
        alice, bob, carol, dave = Node(name="Alice"), Node(name="Bob"), \
                                  Node(name="Carol"), Node(name="Dave")
        path = Path(alice, "LOVES", bob, Relationship(carol, "HATES", bob), carol, "KNOWS", dave)
        encoded = cypher_repr(path)
        assert encoded == "(Alice)-[:LOVES {}]->(Bob)<-[:HATES {}]-(Carol)-[:KNOWS {}]->(Dave)"
