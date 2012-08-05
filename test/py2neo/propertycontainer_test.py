#/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright 2011-2012 Nigel Small
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
PY3K = sys.version_info[0] >= 3

__author__    = "Nigel Small <py2neo@nigelsmall.org>"
__copyright__ = "Copyright 2011-2012 Nigel Small"
__license__   = "Apache License, Version 2.0"

from py2neo import neo4j

import unittest


class PropertyContainerTestCase(unittest.TestCase):

    def setUp(self):
        self.graph_db = neo4j.GraphDatabaseService()

    def test_property_count(self):
        alice, = self.graph_db.create({"name": "Alice", "age": 33})
        self.assertEqual(2, len(alice))

    def test_get_property(self):
        alice, = self.graph_db.create({"name": "Alice"})
        self.assertEqual("Alice", alice["name"])

    def test_get_property_with_odd_name(self):
        foo, = self.graph_db.create({""" !"#$%&'()*+,-./?""": "foo"})
        self.assertEqual("foo", foo[""" !"#$%&'()*+,-./?"""])

    def test_set_property(self):
        alice, = self.graph_db.create({})
        alice["name"] = "Alice"
        self.assertEqual("Alice", alice["name"])

    def test_set_property_with_odd_name(self):
        foo, = self.graph_db.create({})
        foo[""" !"#$%&'()*+,-./?"""] = "foo"
        self.assertEqual("foo", foo[""" !"#$%&'()*+,-./?"""])

    def test_del_property(self):
        alice, = self.graph_db.create({"name": "Alice"})
        del alice["name"]
        self.assertIsNone(alice["name"])

    def test_del_property_with_odd_name(self):
        foo, = self.graph_db.create({""" !"#$%&'()*+,-./?""": "foo"})
        del foo[""" !"#$%&'()*+,-./?"""]
        self.assertIsNone(foo[""" !"#$%&'()*+,-./?"""])

    def test_property_existence(self):
        alice, = self.graph_db.create({"name": "Alice"})
        self.assertTrue("name" in alice)

    def test_property_existence_with_odd_name(self):
        foo, = self.graph_db.create({""" !"#$%&'()*+,-./?""": "foo"})
        self.assertTrue(""" !"#$%&'()*+,-./?""" in foo)

    def test_property_non_existence(self):
        alice, = self.graph_db.create({"name": "Alice"})
        self.assertFalse("age" in alice)

    def test_property_non_existence_with_odd_name(self):
        foo, = self.graph_db.create({""" !"#$%&'()*+,-./?""": "foo"})
        self.assertFalse("age" in foo)

    def test_property_iteration(self):
        properties = {"name": "Alice", "age": 33, """ !"#$%&'()*+,-./?""": "foo"}
        alice, = self.graph_db.create(properties)
        count = 0
        for key in alice:
            self.assertEqual(properties[key], alice[key])
            count += 1
        self.assertEqual(len(properties), count)


if __name__ == '__main__':
    unittest.main()

