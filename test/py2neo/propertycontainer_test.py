#/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
PY3K = sys.version_info[0] >= 3

__author__    = "Nigel Small <py2neo@nigelsmall.org>"
__copyright__ = "Copyright 2011 Nigel Small"
__license__   = "Apache License, Version 2.0"

from py2neo import neo4j

import unittest


def default_graph_db():
    return neo4j.GraphDatabaseService("http://localhost:7474/db/data/")


class PropertyContainerTestCase(unittest.TestCase):

    def setUp(self):
        self.graph_db = default_graph_db()

    def test_property_count(self):
        alice, = self.graph_db.create({"name": "Alice", "age": 33})
        self.assertEqual(2, len(alice))

    def test_get_property(self):
        alice, = self.graph_db.create({"name": "Alice"})
        self.assertEqual("Alice", alice["name"])

    def test_set_property(self):
        alice, = self.graph_db.create({})
        alice["name"] = "Alice"
        self.assertEqual("Alice", alice["name"])

    def test_del_property(self):
        alice, = self.graph_db.create({"name": "Alice"})
        del alice["name"]
        self.assertRaises(KeyError, alice.__getitem__, "name")

    def test_property_existence(self):
        alice, = self.graph_db.create({"name": "Alice"})
        self.assertTrue("name" in alice)

    def test_property_non_existence(self):
        alice, = self.graph_db.create({"name": "Alice"})
        self.assertFalse("age" in alice)

    def test_property_iteration(self):
        properties = {"name": "Alice", "age": 33}
        alice, = self.graph_db.create(properties)
        count = 0
        for key in alice:
            self.assertEqual(properties[key], alice[key])
            count += 1
        self.assertEqual(len(properties), count)


if __name__ == '__main__':
    unittest.main()

