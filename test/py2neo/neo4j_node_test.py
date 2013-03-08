#/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright 2011-2013, Nigel Small
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

from py2neo import neo4j

import logging
import unittest

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    level=logging.DEBUG,
)

UNICODE_TEST_STR = ""
for ch in [0x3053,0x308c,0x306f,0x30c6,0x30b9,0x30c8,0x3067,0x3059]:
    UNICODE_TEST_STR += chr(ch) if PY3K else unichr(ch)

def default_graph_db():
    return neo4j.GraphDatabaseService()

def recycle(*entities):
    for entity in entities:
        try:
            entity.delete()
        except Exception:
            pass


class SingleNodeTestCase(unittest.TestCase):

    data = {
        "true": True,
        "false": False,
        "int": 42,
        "float": 3.141592653589,
        "long": 9223372036854775807 if PY3K else long("9223372036854775807"),
        "str": "This is a test",
        "unicode": UNICODE_TEST_STR,
        "boolean_list": [True, False, True, True, False],
        "int_list": [1, 1, 2, 3, 5, 8, 13, 21, 35],
        "str_list": ["red", "orange", "yellow", "green", "blue", "indigo", "violet"]
    }

    def setUp(self):
        self.graph_db = default_graph_db()
        self.node, = self.graph_db.create(self.data)

    def test_is_created(self):
        self.assertIsNotNone(self.node)

    def test_has_correct_properties(self):
        for key, value in self.data.items():
            self.assertEqual(self.node[key], value)

    @unittest.expectedFailure
    def test_cannot_assign_none(self):
        self.node["none"] = None

    @unittest.expectedFailure
    def test_cannot_assign_oversized_long(self):
        self.node["long"] = 9223372036854775808 if PY3K else long("9223372036854775808")

    @unittest.expectedFailure
    def test_cannot_assign_complex(self):
        self.node["complex"] = complex(17, 30)

    @unittest.expectedFailure
    def test_cannot_assign_mixed_list(self):
        self.node["mixed_list"] = [42, "life", "universe", "everything"]

    @unittest.expectedFailure
    def test_cannot_assign_dict(self):
        self.node["dict"] = {"foo": 3, "bar": 4, "baz": 5}

    def tearDown(self):
        self.node.delete()


class NodeTestCase(unittest.TestCase):

    def setUp(self):
        self.gdb = default_graph_db()
        self.fred, self.wilma, self.fred_and_wilma = self.gdb.create(
            {"name": "Fred"}, {"name": "Wilma"}, (0, "REALLY LOVES", 1)
        )

    def test_get_all_relationships(self):
        rels = self.fred.get_relationships()
        self.assertEqual(1, len(rels))
        self.assertTrue(isinstance(rels[0], neo4j.Relationship))
        self.assertEqual("REALLY LOVES", rels[0].type)
        self.assertEqual(self.fred, rels[0].start_node)
        self.assertEqual(self.wilma, rels[0].end_node)

    def test_get_all_outgoing_relationships(self):
        rels = self.fred.get_relationships(neo4j.Direction.OUTGOING)
        self.assertEqual(1, len(rels))
        self.assertTrue(isinstance(rels[0], neo4j.Relationship))
        self.assertEqual("REALLY LOVES", rels[0].type)
        self.assertEqual(self.fred, rels[0].start_node)
        self.assertEqual(self.wilma, rels[0].end_node)

    def test_get_all_incoming_relationships(self):
        rels = self.wilma.get_relationships(neo4j.Direction.INCOMING)
        self.assertEqual(1, len(rels))
        self.assertTrue(isinstance(rels[0], neo4j.Relationship))
        self.assertEqual("REALLY LOVES", rels[0].type)
        self.assertEqual(self.fred, rels[0].start_node)
        self.assertEqual(self.wilma, rels[0].end_node)

    def test_get_all_relationships_of_type(self):
        rels = self.fred.get_relationships(neo4j.Direction.EITHER, "REALLY LOVES")
        self.assertEqual(1, len(rels))
        self.assertTrue(isinstance(rels[0], neo4j.Relationship))
        self.assertEqual("REALLY LOVES", rels[0].type)
        self.assertEqual(self.fred, rels[0].start_node)
        self.assertEqual(self.wilma, rels[0].end_node)

    def test_get_single_relationship(self):
        rel = self.fred.get_single_relationship(neo4j.Direction.EITHER, "REALLY LOVES")
        self.assertTrue(isinstance(rel, neo4j.Relationship))
        self.assertEqual("REALLY LOVES", rel.type)
        self.assertEqual(self.fred, rel.start_node)
        self.assertEqual(self.wilma, rel.end_node)

    def test_get_single_outgoing_relationship(self):
        rel = self.fred.get_single_relationship(neo4j.Direction.OUTGOING, "REALLY LOVES")
        self.assertTrue(isinstance(rel, neo4j.Relationship))
        self.assertEqual("REALLY LOVES", rel.type)
        self.assertEqual(self.fred, rel.start_node)
        self.assertEqual(self.wilma, rel.end_node)

    def test_get_single_incoming_relationship(self):
        rel = self.wilma.get_single_relationship(neo4j.Direction.INCOMING, "REALLY LOVES")
        self.assertTrue(isinstance(rel, neo4j.Relationship))
        self.assertEqual("REALLY LOVES", rel.type)
        self.assertEqual(self.fred, rel.start_node)
        self.assertEqual(self.wilma, rel.end_node)

    def test_explicit_is_related_to(self):
        self.assertTrue(self.fred.is_related_to(self.wilma, neo4j.Direction.EITHER, "REALLY LOVES"))

    def test_explicit_is_related_to_outgoing(self):
        self.assertTrue(self.fred.is_related_to(self.wilma, neo4j.Direction.OUTGOING, "REALLY LOVES"))

    def test_explicit_is_related_to_incoming(self):
        self.assertFalse(self.fred.is_related_to(self.wilma, neo4j.Direction.INCOMING, "REALLY LOVES"))

    def test_implicit_is_related_to(self):
        self.assertTrue(self.fred.is_related_to(self.wilma))

    def test_is_not_related_to(self):
        homer, = self.gdb.create({"name": "Homer"})
        self.assertFalse(self.fred.is_related_to(homer))

    def test_get_relationships_with(self):
        rels = self.fred.get_relationships_with(self.wilma, neo4j.Direction.EITHER, "REALLY LOVES")
        self.assertEqual(1, len(rels))
        self.assertEqual(self.fred, rels[0].start_node)
        self.assertEqual(self.wilma, rels[0].end_node)

    def tearDown(self):
        self.gdb.delete(self.fred_and_wilma, self.fred, self.wilma)


if __name__ == '__main__':
    unittest.main()
