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


from py2neo import order, size, Node, Relationship
from test.util import Py2neoTestCase


class TransactionMergeTestCase(Py2neoTestCase):

    def setUp(self):
        self.graph.delete_all()

    def test_can_merge_node_that_does_not_exist(self):
        alice = Node("Person", name="Alice")
        old_order = order(self.graph)
        self.graph.merge(alice)
        assert alice.__remote__
        assert self.graph.exists(alice)
        new_order = order(self.graph)
        assert new_order == old_order + 1

    def test_can_merge_node_that_does_exist(self):
        self.graph.create(Node("Person", name="Alice"))
        alice = Node("Person", name="Alice")
        old_order = order(self.graph)
        self.graph.merge(alice)
        assert alice.__remote__
        assert self.graph.exists(alice)
        new_order = order(self.graph)
        assert new_order == old_order

    def test_can_merge_bound_node(self):
        alice = Node("Person", name="Alice")
        self.graph.create(alice)
        old_order = order(self.graph)
        self.graph.merge(alice)
        assert alice.__remote__
        assert self.graph.exists(alice)
        new_order = order(self.graph)
        assert new_order == old_order

    def test_can_merge_node_without_label(self):
        node = Node()
        old_order = order(self.graph)
        self.graph.merge(node)
        assert node.__remote__
        assert self.graph.exists(node)
        new_order = order(self.graph)
        assert new_order == old_order + 1

    def test_can_merge_with_label_node_without_label(self):
        node = Node()
        old_order = order(self.graph)
        self.graph.merge(node, "Person")
        assert node.__remote__
        assert self.graph.exists(node)
        new_order = order(self.graph)
        assert new_order == old_order + 1

    def test_can_merge_node_that_does_not_exist_on_specific_label_and_key(self):
        alice = Node("Person", "Employee", name="Alice", age=33)
        old_order = order(self.graph)
        self.graph.merge(alice, "Person", "name")
        assert alice.__remote__
        assert self.graph.exists(alice)
        new_order = order(self.graph)
        assert new_order == old_order + 1

    def test_can_merge_node_that_does_exist_on_specific_label_and_key_with_extra_properties(self):
        self.graph.create(Node("Person", name="Alice"))
        alice = Node("Person", "Employee", name="Alice", age=33)
        old_order = order(self.graph)
        self.graph.merge(alice, "Person", "name")
        assert alice.__remote__
        assert self.graph.exists(alice)
        new_order = order(self.graph)
        assert new_order == old_order

    def test_can_merge_node_that_does_exist_on_specific_label_and_key_with_other_properties(self):
        self.graph.create(Node("Person", name="Alice", age=44))
        alice = Node("Person", "Employee", name="Alice", age=33)
        old_order = order(self.graph)
        self.graph.merge(alice, "Person", "name")
        assert alice.__remote__
        assert self.graph.exists(alice)
        new_order = order(self.graph)
        assert new_order == old_order

    def test_can_merge_relationship_that_does_not_exist(self):
        alice = Node("Person", name="Alice")
        bob = Node("Person", name="Bob")
        ab = Relationship(alice, "KNOWS", bob)
        old_order = order(self.graph)
        old_size = size(self.graph)
        self.graph.merge(ab)
        assert alice.__remote__
        assert bob.__remote__
        assert ab.__remote__
        assert self.graph.exists(alice | bob | ab)
        new_order = order(self.graph)
        new_size = size(self.graph)
        assert new_order == old_order + 2
        assert new_size == old_size + 1

    def test_can_merge_relationship_where_one_node_exists(self):
        alice = Node("Person", name="Alice")
        self.graph.create(alice)
        bob = Node("Person", name="Bob")
        ab = Relationship(alice, "KNOWS", bob)
        old_order = order(self.graph)
        old_size = size(self.graph)
        self.graph.merge(ab)
        assert alice.__remote__
        assert bob.__remote__
        assert ab.__remote__
        assert self.graph.exists(alice | bob | ab)
        new_order = order(self.graph)
        new_size = size(self.graph)
        assert new_order == old_order + 1
        assert new_size == old_size + 1

    def test_can_merge_relationship_where_all_exist(self):
        alice = Node("Person", name="Alice")
        self.graph.create(Relationship(alice, "KNOWS", Node("Person", name="Bob")))
        bob = Node("Person", name="Bob")
        ab = Relationship(alice, "KNOWS", bob)
        old_order = order(self.graph)
        old_size = size(self.graph)
        self.graph.merge(ab)
        assert alice.__remote__
        assert bob.__remote__
        assert ab.__remote__
        assert self.graph.exists(alice | bob | ab)
        new_order = order(self.graph)
        new_size = size(self.graph)
        assert new_order == old_order
        assert new_size == old_size

    def test_cannot_merge_non_walkable(self):
        with self.assertRaises(TypeError):
            self.graph.merge("this string is definitely not a walkable object")
