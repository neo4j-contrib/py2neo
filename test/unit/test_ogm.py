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

from test.fixtures.ogm import Film, MacGuffin, DerivedThing


class SubclassTestCase(TestCase):

    def test_class_primary_label_defaults_to_class_name(self):
        assert MacGuffin.__primarylabel__ == "MacGuffin"

    def test_class_primary_label_can_be_overridden(self):
        assert Film.__primarylabel__ == "Movie"

    def test_class_primary_key_defaults_to_id(self):
        assert MacGuffin.__primarykey__ == "__id__"

    def test_class_primary_key_can_be_overridden(self):
        assert Film.__primarykey__ == "title"

    def test_class_primary_key_is_inherited(self):
        assert DerivedThing.__primarykey__ == "my_key"


class InstanceTestCase(TestCase):

    def setUp(self):
        self.macguffin = MacGuffin()
        self.film = Film("Die Hard")

    def test_instance_primary_label_defaults_to_class_name(self):
        assert self.macguffin.__primarylabel__ == "MacGuffin"

    def test_instance_primary_label_can_be_overridden(self):
        assert self.film.__primarylabel__ == "Movie"

    def test_instance_primary_key_defaults_to_id(self):
        assert self.macguffin.__primarykey__ == "__id__"

    def test_instance_primary_key_can_be_overridden(self):
        assert self.film.__primarykey__ == "title"

    def test_instance_repr(self):
        assert repr(self.film).startswith("<Film")

    def test_instance_not_equal_to_non_graph_object(self):
        assert self.film != "this is not a graph object"


class InstanceSubgraphTestCase(TestCase):

    def setUp(self):
        self.film = Film("Die Hard")
        self.film_node = self.film.__ogm__.node

    def test_instance_subgraph_inherits_primary_label(self):
        assert self.film_node.__primarylabel__ == "Movie"

    def test_instance_subgraph_inherits_primary_key(self):
        assert self.film_node.__primarykey__ == "title"


class InstanceLabelTestCase(TestCase):

    def setUp(self):
        self.film = Film("Die Hard")
        self.film.awesome = True
        self.film.science_fiction = True
        self.film_node = self.film.__ogm__.node

    def test_instance_label_name_defaults_to_attribute_name_variant(self):
        assert self.film_node.has_label("Awesome")

    def test_instance_label_name_can_be_overridden(self):
        assert self.film_node.has_label("SciFi")
        assert not self.film_node.has_label("ScienceFiction")

    def test_instance_label_defaults_to_absent(self):
        assert not self.film_node.has_label("Musical")

    def test_setting_to_false_removes_label(self):
        self.film.awesome = False
        assert not self.film_node.has_label("Awesome")


class InstancePropertyTestCase(TestCase):

    def setUp(self):
        self.film = Film("Die Hard")
        self.film.year_of_release = 1988
        self.film_node = self.film.__ogm__.node

    def test_instance_property_key_defaults_to_attribute_name(self):
        assert "title" in self.film_node

    def test_instance_property_key_can_be_overridden(self):
        assert "released" in self.film_node
        assert "year_of_release" not in self.film_node
