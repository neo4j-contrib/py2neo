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


from uuid import uuid4

from py2neo import Node
from test.cases import DatabaseTestCase


class MergeTestCase(DatabaseTestCase):

    def setUp(self):
        self.label = uuid4().hex

    def test_can_merge_on_label_only(self):
        merged = list(self.graph.merge(self.label))
        assert len(merged) == 1
        assert isinstance(merged[0], Node)
        assert merged[0].labels == {self.label}
        
    def test_can_merge_on_label_and_property(self):
        merged = list(self.graph.merge(self.label, "foo", "bar"))
        assert len(merged) == 1
        assert isinstance(merged[0], Node)
        assert merged[0].labels == {self.label}
        assert merged[0].properties == {"foo": "bar"}
        
    def test_cannot_merge_empty_label(self):
        with self.assertRaises(ValueError):
            list(self.graph.merge(""))

    def test_cannot_merge_with_non_textual_property_key(self):
        with self.assertRaises(TypeError):
            list(self.graph.merge(self.label, 123, 456))

    def test_cannot_merge_with_dict_property_key(self):
        with self.assertRaises(TypeError):
            list(self.graph.merge(self.label, {}))

    def test_cannot_merge_on_key_only(self):
        with self.assertRaises(ValueError):
            list(self.graph.merge(self.label, "foo"))

    def test_can_merge_one_on_label_and_property(self):
        merged = self.graph.merge_one(self.label, "foo", "bar")
        assert isinstance(merged, Node)
        assert merged.labels == {self.label}
        assert merged.properties == {"foo": "bar"}

