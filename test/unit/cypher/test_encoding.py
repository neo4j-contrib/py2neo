#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2020, Nigel Small
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


from unittest import TestCase

from neotime import Date, Time, DateTime, Duration
from pytest import raises

from py2neo.data import Node
from py2neo.cypher import cypher_escape, cypher_str, cypher_repr
from py2neo.cypher.encoding import LabelSetView, PropertyDictView, PropertySelector


class LabelSetViewTestCase(TestCase):

    def test_can_create_empty_view(self):
        view = LabelSetView([])
        self.assertEqual(repr(view), "")

    def test_can_create_single_label_view(self):
        view = LabelSetView(["A"])
        self.assertEqual(repr(view), ":A")

    def test_can_create_double_label_view(self):
        view = LabelSetView(["A", "B"])
        self.assertEqual(repr(view), ":A:B")

    def test_can_select_existing_in_view(self):
        view = LabelSetView(["A", "B"]).B
        self.assertEqual(repr(view), ":B")

    def test_can_select_non_existing_in_view(self):
        view = LabelSetView(["A", "B"]).C
        self.assertEqual(repr(view), "")

    def test_can_chain_select(self):
        view = LabelSetView(["A", "B", "C"]).B.C
        self.assertEqual(repr(view), ":B:C")

    def test_can_reselect_same(self):
        view = LabelSetView(["A", "B", "C"]).B.B.C
        self.assertEqual(repr(view), ":B:C")

    def test_length(self):
        view = LabelSetView(["A", "B", "C"])
        self.assertEqual(len(view), 3)

    def test_iterable(self):
        view = LabelSetView(["A", "B", "C"])
        self.assertSetEqual(set(view), {"A", "B", "C"})

    def test_containment(self):
        view = LabelSetView(["A", "B", "C"])
        self.assertIn("A", view)

    def test_non_containment(self):
        view = LabelSetView(["A", "B", "C"])
        self.assertNotIn("D", view)


class PropertyDictViewTestCase(TestCase):

    def test_can_create_empty_view(self):
        view = PropertyDictView({})
        self.assertEqual(repr(view), "{}")

    def test_can_create_single_property_view(self):
        view = PropertyDictView({"A": 1})
        self.assertEqual(repr(view), "{A: 1}")

    def test_can_create_double_property_view(self):
        view = PropertyDictView({"A": 1, "B": 2})
        self.assertEqual(repr(view), "{A: 1, B: 2}")

    def test_can_select_existing_in_view(self):
        view = PropertyDictView({"A": 1, "B": 2}).B
        self.assertEqual(repr(view), "{B: 2}")

    def test_can_select_non_existing_in_view(self):
        view = PropertyDictView({"A": 1, "B": 2}).C
        self.assertEqual(repr(view), "{}")

    def test_can_chain_select(self):
        view = PropertyDictView({"A": 1, "B": 2, "C": 3}).B.C
        self.assertEqual(repr(view), "{B: 2, C: 3}")

    def test_can_reselect_same(self):
        view = PropertyDictView({"A": 1, "B": 2, "C": 3}).B.B.C
        self.assertEqual(repr(view), "{B: 2, C: 3}")

    def test_length(self):
        view = PropertyDictView({"A": 1, "B": 2, "C": 3})
        self.assertEqual(len(view), 3)

    def test_iterable(self):
        view = PropertyDictView({"A": 1, "B": 2, "C": 3})
        self.assertEqual(set(view), {"A", "B", "C"})

    def test_containment(self):
        view = PropertyDictView({"A": 1, "B": 2, "C": 3})
        self.assertIn("A", view)

    def test_non_containment(self):
        view = PropertyDictView({"A": 1, "B": 2, "C": 3})
        self.assertNotIn("D", view)


class PropertySelectorTestCase(TestCase):

    def test_simple(self):
        selector = PropertySelector({"A": 1, "B": 2, "C": 3})
        self.assertEqual(selector.A, "1")

    def test_non_existent(self):
        selector = PropertySelector({"A": 1, "B": 2, "C": 3})
        self.assertEqual(selector.D, "null")


class NodeReprTestCase(TestCase):

    def test_empty(self):
        a = Node()
        r = cypher_repr(a)
        self.assertEqual("({})", r)

    def test_single_property(self):
        a = Node(name="Alice")
        r = cypher_repr(a)
        self.assertEqual("({name: 'Alice'})", r)

    def test_property_and_label(self):
        a = Node("Person", name="Alice")
        r = cypher_repr(a)
        self.assertEqual("(:Person {name: 'Alice'})", r)

    def test_date_property(self):
        a = Node(d=Date(1970, 1, 1))
        r = cypher_repr(a)
        self.assertEqual("({d: date('1970-01-01')})", r)

    def test_time_property(self):
        a = Node(t=Time(12, 34, 56))
        r = cypher_repr(a)
        self.assertEqual("({t: time('12:34:56.000000000')})", r)

    def test_datetime_property(self):
        a = Node(dt=DateTime(1970, 1, 1, 12, 34, 56))
        r = cypher_repr(a)
        self.assertEqual("({dt: datetime('1970-01-01T12:34:56.000000000')})", r)

    def test_duration_property(self):
        a = Node(dur=Duration(days=3))
        r = cypher_repr(a)
        self.assertEqual("({dur: duration('P3D')})", r)


class CypherEscapeTestCase(TestCase):

    def test_empty_string(self):
        value = ""
        with self.assertRaises(ValueError):
            _ = cypher_escape(value)

    def test_simple_string(self):
        value = "foo"
        escaped = "foo"
        self.assertEqual(escaped, cypher_escape(value))

    def test_string_with_space(self):
        value = "foo bar"
        escaped = "`foo bar`"
        self.assertEqual(escaped, cypher_escape(value))

    def test_string_with_backtick(self):
        value = "foo `bar`"
        escaped = "`foo ``bar```"
        self.assertEqual(escaped, cypher_escape(value))


def test_cypher_escape_on_non_string():
    with raises(TypeError):
        _ = cypher_escape(object())


def test_cypher_str_on_bytes():
    assert cypher_str(b"hello, world") == u"hello, world"
