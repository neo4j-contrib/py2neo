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


from unittest import TestCase

from py2neo.cypher.writing import LabelSetView, PropertyDictView, PropertySelector


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
