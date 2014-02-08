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
PY3K = sys.version_info[0] >= 3

from py2neo import neo4j, node

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


class AbstractNodeTestCase(unittest.TestCase):

    def test_can_create_abstract_node(self):
        alice = node(name="Alice", age=34)
        assert isinstance(alice, neo4j.Node)
        assert alice.is_abstract
        assert alice["name"] == "Alice"
        assert alice["age"] == 34

    def test_can_equate_abstract_nodes(self):
        alice_1 = node(name="Alice", age=34)
        alice_2 = node(name="Alice", age=34)
        assert alice_1 == alice_2


class ConcreteNodeTestCase(unittest.TestCase):

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
        self.graph_db = neo4j.GraphDatabaseService()
        self.graph_db.clear()

    def test_can_create_concrete_node(self):
        alice, = self.graph_db.create({"name": "Alice", "age": 34})
        assert isinstance(alice, neo4j.Node)
        assert not alice.is_abstract
        assert alice["name"] == "Alice"
        assert alice["age"] == 34

    def test_all_property_types(self):
        data = {
            "nun": None,
            "yes": True,
            "no": False,
            "int": 42,
            "float": 3.141592653589,
            "long": 9223372036854775807 if PY3K else long("9223372036854775807"),
            "str": "This is a test",
            "unicode": UNICODE_TEST_STR,
            "boolean_list": [True, False, True, True, False],
            "int_list": [1, 1, 2, 3, 5, 8, 13, 21, 35],
            "str_list": ["red", "orange", "yellow", "green", "blue", "indigo", "violet"]
        }
        foo, = self.graph_db.create(data)
        for key, value in data.items():
            self.assertEqual(foo[key], value)

    def test_cannot_assign_oversized_long(self):
        foo, = self.graph_db.create({})
        try:
            if PY3K:
                foo["long"] = 9223372036854775808
            else:
                foo["long"] = long("9223372036854775808")
        except:
            assert True
        else:
            assert False

    def test_cannot_assign_mixed_list(self):
        foo, = self.graph_db.create({})
        try:
            foo["mixed_list"] = [42, "life", "universe", "everything"]
        except:
            assert True
        else:
            assert False

    def test_cannot_assign_dict(self):
        foo, = self.graph_db.create({})
        try:
            foo["dict"] = {"foo": 3, "bar": 4, "baz": 5}
        except:
            assert True
        else:
            assert False


class NodeTestCase(unittest.TestCase):

    def setUp(self):
        self.gdb = default_graph_db()
        self.fred, self.wilma, self.fred_and_wilma = self.gdb.create(
            {"name": "Fred"}, {"name": "Wilma"}, (0, "REALLY LOVES", 1)
        )

    def test_get_all_relationships(self):
        rels = list(self.fred.match())
        self.assertEqual(1, len(rels))
        self.assertTrue(isinstance(rels[0], neo4j.Relationship))
        self.assertEqual("REALLY LOVES", rels[0].type)
        self.assertEqual(self.fred, rels[0].start_node)
        self.assertEqual(self.wilma, rels[0].end_node)

    def test_get_all_outgoing_relationships(self):
        rels = list(self.fred.match_outgoing())
        self.assertEqual(1, len(rels))
        self.assertTrue(isinstance(rels[0], neo4j.Relationship))
        self.assertEqual("REALLY LOVES", rels[0].type)
        self.assertEqual(self.fred, rels[0].start_node)
        self.assertEqual(self.wilma, rels[0].end_node)

    def test_get_all_incoming_relationships(self):
        #rels = self.wilma.get_relationships(neo4j.Direction.INCOMING)
        rels = list(self.wilma.match_incoming())
        self.assertEqual(1, len(rels))
        self.assertTrue(isinstance(rels[0], neo4j.Relationship))
        self.assertEqual("REALLY LOVES", rels[0].type)
        self.assertEqual(self.fred, rels[0].start_node)
        self.assertEqual(self.wilma, rels[0].end_node)

    def test_get_all_relationships_of_type(self):
        #rels = self.fred.get_relationships(neo4j.Direction.EITHER, "REALLY LOVES")
        rels = list(self.fred.match("REALLY LOVES"))
        self.assertEqual(1, len(rels))
        self.assertTrue(isinstance(rels[0], neo4j.Relationship))
        self.assertEqual("REALLY LOVES", rels[0].type)
        self.assertEqual(self.fred, rels[0].start_node)
        self.assertEqual(self.wilma, rels[0].end_node)

    def test_get_single_relationship(self):
        #rel = self.fred.get_single_relationship(neo4j.Direction.EITHER, "REALLY LOVES")
        rels = list(self.fred.match("REALLY LOVES", limit=1))
        self.assertTrue(isinstance(rels[0], neo4j.Relationship))
        self.assertEqual("REALLY LOVES", rels[0].type)
        self.assertEqual(self.fred, rels[0].start_node)
        self.assertEqual(self.wilma, rels[0].end_node)

    def test_get_single_outgoing_relationship(self):
        #rel = self.fred.get_single_relationship(neo4j.Direction.OUTGOING, "REALLY LOVES")
        rels = list(self.fred.match_outgoing("REALLY LOVES", limit=1))
        self.assertTrue(isinstance(rels[0], neo4j.Relationship))
        self.assertEqual("REALLY LOVES", rels[0].type)
        self.assertEqual(self.fred, rels[0].start_node)
        self.assertEqual(self.wilma, rels[0].end_node)

    def test_get_single_incoming_relationship(self):
        #rel = self.wilma.get_single_relationship(neo4j.Direction.INCOMING, "REALLY LOVES")
        rels = list(self.wilma.match_incoming("REALLY LOVES",
                                              start_node=self.fred, limit=1))
        self.assertTrue(isinstance(rels[0], neo4j.Relationship))
        self.assertEqual("REALLY LOVES", rels[0].type)
        self.assertEqual(self.fred, rels[0].start_node)
        self.assertEqual(self.wilma, rels[0].end_node)

    def test_explicit_is_related_to(self):
        #self.assertTrue(self.fred.is_related_to(self.wilma, neo4j.Direction.EITHER, "REALLY LOVES"))
        matched = list(self.fred.match("REALLY LOVES", other_node=self.wilma,
                                       limit=1))
        assert matched

    def test_explicit_is_related_to_outgoing(self):
        #self.assertTrue(self.fred.is_related_to(self.wilma, neo4j.Direction.OUTGOING, "REALLY LOVES"))
        matched = list(self.fred.match_outgoing("REALLY LOVES",
                                                end_node=self.wilma, limit=1))
        assert matched

    def test_explicit_is_related_to_incoming(self):
        #self.assertFalse(self.fred.is_related_to(self.wilma, neo4j.Direction.INCOMING, "REALLY LOVES"))
        matched = list(self.fred.match_incoming("REALLY LOVES",
                                                 start_node=self.wilma, limit=1))
        assert not matched

    def test_implicit_is_related_to(self):
        #self.assertTrue(self.fred.is_related_to(self.wilma))
        matches = list(self.fred.match(other_node=self.wilma))
        assert matches

    def test_is_not_related_to(self):
        homer, = self.gdb.create({"name": "Homer"})
        #self.assertFalse(self.fred.is_related_to(homer))
        matches = list(self.fred.match(other_node=homer))
        assert not matches

    def test_get_relationships_with(self):
        #rels = self.fred.get_relationships_with(self.wilma, neo4j.Direction.EITHER, "REALLY LOVES")
        rels = list(self.fred.match("REALLY LOVES", other_node=self.wilma))
        self.assertEqual(1, len(rels))
        self.assertEqual(self.fred, rels[0].start_node)
        self.assertEqual(self.wilma, rels[0].end_node)

    def tearDown(self):
        self.gdb.delete(self.fred_and_wilma, self.fred, self.wilma)


if __name__ == '__main__':
    unittest.main()
