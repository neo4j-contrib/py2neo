#/usr/bin/env python
# -*- coding: utf-8 -*-

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

import sys

import pytest

PY3K = sys.version_info[0] >= 3


class MetadataPropertiesTestCase(object):

    @pytest.fixture(autouse=True)
    def setup(self, graph):
        self.graph = graph

    def test_can_get_properties_from_metadata(self):
        # create node
        props = {"name": "Alice", "age": 33}
        alice, = self.graph.create(props)
        # check properties are in metadata
        assert alice.__metadata__["data"] == props
        # update property
        alice["age"] = 34
        assert alice["age"] == 34
        # check metadata has not been updated
        assert alice.__metadata__["data"]["age"] == 33
        # refresh metadata
        alice.refresh()
        assert alice.__metadata__["data"]["age"] == 34


class PropertyContainerTestCase(object):

    @pytest.fixture(autouse=True)
    def setup(self, graph):
        self.graph = graph

    def test_property_count(self):
        alice, = self.graph.create({"name": "Alice", "age": 33})
        self.assertEqual(2, len(alice))

    def test_get_property(self):
        alice, = self.graph.create({"name": "Alice"})
        self.assertEqual("Alice", alice["name"])

    def test_get_list_property(self):
        thing, = self.graph.create({"counts": [0]})
        self.assertEqual([0], thing["counts"])

    def test_set_list_property(self):
        thing, = self.graph.create({"counts": [0]})
        thing["counts"] = [1]
        self.assertEqual([1], thing["counts"])

    def test_set_list_property_item(self):
        thing, = self.graph.create({"counts": [0]})
        thing["counts"][0] += 1
        # does not increment as local-only
        self.assertEqual([0], thing["counts"])

    def test_set_list_property_item_workaround(self):
        thing, = self.graph.create({"counts": [0]})
        tc = thing["counts"]
        tc[0] += 1
        thing["counts"] = tc
        self.assertEqual([1], thing["counts"])

    def test_get_property_with_odd_name(self):
        foo, = self.graph.create({""" !"#$%&'()*+,-./?""": "foo"})
        self.assertEqual("foo", foo[""" !"#$%&'()*+,-./?"""])

    def test_set_property(self):
        alice, = self.graph.create({})
        alice["name"] = "Alice"
        self.assertEqual("Alice", alice["name"])

    def test_set_property_with_odd_name(self):
        foo, = self.graph.create({})
        foo[""" !"#$%&'()*+,-./?"""] = "foo"
        self.assertEqual("foo", foo[""" !"#$%&'()*+,-./?"""])

    def test_del_property(self):
        alice, = self.graph.create({"name": "Alice"})
        del alice["name"]
        self.assertTrue(alice["name"] is None)

    def test_del_non_existent_property(self):
        alice, = self.graph.create({"name": "Alice"})
        del alice["foo"]
        self.assertEqual("Alice", alice["name"])
        self.assertTrue(alice["foo"] is None)

    def test_del_property_with_odd_name(self):
        foo, = self.graph.create({""" !"#$%&'()*+,-./?""": "foo"})
        del foo[""" !"#$%&'()*+,-./?"""]
        self.assertTrue(foo[""" !"#$%&'()*+,-./?"""] is None)

    def test_property_existence(self):
        alice, = self.graph.create({"name": "Alice"})
        self.assertTrue("name" in alice)

    def test_property_existence_with_odd_name(self):
        foo, = self.graph.create({""" !"#$%&'()*+,-./?""": "foo"})
        self.assertTrue(""" !"#$%&'()*+,-./?""" in foo)

    def test_property_non_existence(self):
        alice, = self.graph.create({"name": "Alice"})
        self.assertFalse("age" in alice)

    def test_property_non_existence_with_odd_name(self):
        foo, = self.graph.create({""" !"#$%&'()*+,-./?""": "foo"})
        self.assertFalse("age" in foo)

    def test_property_iteration(self):
        properties = {"name": "Alice", "age": 33, """ !"#$%&'()*+,-./?""": "foo"}
        alice, = self.graph.create(properties)
        count = 0
        for key in alice:
            self.assertEqual(properties[key], alice[key])
            count += 1
        self.assertEqual(len(properties), count)

    def test_set_properties(self):
        alice, = self.graph.create({"name": "Alice", "surname": "Smith"})
        alice.set_properties({"full_name": "Alice Smith", "age": 33})
        self.assertEqual(None, alice["name"])
        self.assertEqual(None, alice["surname"])
        self.assertEqual("Alice Smith", alice["full_name"])
        self.assertEqual(33, alice["age"])
