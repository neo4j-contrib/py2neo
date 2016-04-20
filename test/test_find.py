#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2016, Nigel Small
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


from py2neo import Node
from test.util import Py2neoTestCase


class FindTestCase(Py2neoTestCase):

    def test_will_find_no_nodes_with_non_existent_label(self):
        label = next(self.unique_string)
        nodes = list(self.graph.find(label))
        assert nodes == []

    def test_can_find_nodes_with_label(self):
        label = next(self.unique_string)
        alice = Node(label, name="Alice")
        self.graph.create(alice)
        nodes = list(self.graph.find(label))
        assert alice in nodes

    def test_can_find_nodes_with_label_and_property(self):
        label = next(self.unique_string)
        alice = Node(label, name="Alice")
        self.graph.create(alice)
        nodes = list(self.graph.find(label, "name", "Alice"))
        assert alice in nodes

    def test_can_find_nodes_with_label_and_one_of_several_property_values(self):
        label = next(self.unique_string)
        alice = Node(label, name="Alice")
        bob = Node(label, name="Bob")
        carol = Node(label, name="Carol")
        self.graph.create(alice | bob | carol)
        nodes = list(self.graph.find(label, "name", ("Alice", "Bob", "Carol")))
        assert alice in nodes
        assert bob in nodes
        assert carol in nodes

    def test_cannot_find_empty_label(self):
        with self.assertRaises(ValueError):
            list(self.graph.find(""))


class FindOneTestCase(Py2neoTestCase):

    def test_can_find_one_node_with_label_and_property(self):
        label = next(self.unique_string)
        name = next(self.unique_string)
        thing = Node(label, name=name)
        self.graph.create(thing)
        found = self.graph.find_one(label, "name", name)
        assert found is thing

    def test_should_find_none_if_no_match(self):
        label = next(self.unique_string)
        name = next(self.unique_string)
        found = self.graph.find_one(label, "name", name)
        assert found is None
