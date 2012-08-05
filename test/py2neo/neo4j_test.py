#/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
PY3K = sys.version_info[0] >= 3

__author__    = "Nigel Small <py2neo@nigelsmall.org>"
__copyright__ = "Copyright 2011 Nigel Small"
__license__   = "Apache License, Version 2.0"

from py2neo import neo4j, rest

import unittest

UNICODE_TEST_STR = ""
for ch in [0x3053,0x308c,0x306f,0x30c6,0x30b9,0x30c8,0x3067,0x3059]:
    UNICODE_TEST_STR += chr(ch) if PY3K else unichr(ch)


def default_graph_db():
    return neo4j.GraphDatabaseService("http://localhost:7474/db/data/")


class FailureTest(unittest.TestCase):

    def test_no_response(self):
        self.assertRaises(rest.SocketError,
            neo4j.GraphDatabaseService,
            "http://localhsot:4747/data/db"
        )


class BadDatabaseURITest(unittest.TestCase):

    def test_wrong_host(self):
        self.assertRaises(rest.SocketError,
            neo4j.GraphDatabaseService, "http://localtoast:7474/db/data/")

    def test_wrong_port(self):
        self.assertRaises(rest.SocketError,
            neo4j.GraphDatabaseService, "http://localhost:7575/db/data/")

    def test_wrong_path(self):
        self.assertRaises(rest.ResourceNotFound,
            neo4j.GraphDatabaseService, "http://localhost:7474/foo/bar/")

    def test_no_trailing_slash(self):
        graph_db = neo4j.GraphDatabaseService("http://localhost:7474/db/data")
        self.assertEqual("http://localhost:7474/db/data/", graph_db._uri)


class GraphDatabaseServiceTest(unittest.TestCase):

    def setUp(self):
        self.gdb = default_graph_db()
        print("Neo4j Version: {0}".format(repr(self.gdb._neo4j_version)))
        print("Node count: {0}".format(self.gdb.get_node_count()))
        print("Relationship count: {0}".format(self.gdb.get_relationship_count()))

    def test_get_reference_node(self):
        ref_node = self.gdb.get_reference_node()
        self.assertIsNotNone(ref_node)

    def test_create_node(self):
        self.gdb.create_node()

    def test_get_node_by_id(self):
        a1 = self.gdb.create_node({"foo": "bar"})
        a2 = self.gdb.get_node(a1.id)
        self.assertEqual(a1, a2)

    def test_create_node_with_property_dict(self):
        node = self.gdb.create_node({"foo": "bar"})
        self.assertEqual("bar", node["foo"])

    def test_create_node_with_mixed_property_types(self):
        node = self.gdb.create_node(
            {"number": 13, "foo": "bar", "true": False, "fish": "chips"}
        )
        self.assertEqual(4, len(node.get_properties()))
        self.assertEqual("chips", node["fish"])
        self.assertEqual("bar", node["foo"])
        self.assertEqual(13, node["number"])
        self.assertEqual(False, node["true"])

    def test_create_multiple_nodes(self):
        nodes = self.gdb.create(
                {},
                {"foo": "bar"},
                {"number": 42, "foo": "baz", "true": True},
                {"fish": ["cod", "haddock", "plaice"], "number": 109}
        )
        self.assertEqual(4, len(nodes))
        self.assertEqual(0, len(nodes[0].get_properties()))
        self.assertEqual(1, len(nodes[1].get_properties()))
        self.assertEqual("bar", nodes[1]["foo"])
        self.assertEqual(3, len(nodes[2].get_properties()))
        self.assertEqual(42, nodes[2]["number"])
        self.assertEqual("baz", nodes[2]["foo"])
        self.assertEqual(True, nodes[2]["true"])
        self.assertEqual(2, len(nodes[3].get_properties()))
        self.assertEqual("cod", nodes[3]["fish"][0])
        self.assertEqual("haddock", nodes[3]["fish"][1])
        self.assertEqual("plaice", nodes[3]["fish"][2])
        self.assertEqual(109, nodes[3]["number"])

    def test_batch_get_properties(self):
        nodes = self.gdb.create(
                {},
                {"foo": "bar"},
                {"number": 42, "foo": "baz", "true": True},
                {"fish": ["cod", "haddock", "plaice"], "number": 109}
        )
        props = self.gdb.get_properties(*nodes)
        self.assertEqual(4, len(props))
        self.assertEqual(0, len(props[0]))
        self.assertEqual(1, len(props[1]))
        self.assertEqual("bar", props[1]["foo"])
        self.assertEqual(3, len(props[2]))
        self.assertEqual(42, props[2]["number"])
        self.assertEqual("baz", props[2]["foo"])
        self.assertEqual(True, props[2]["true"])
        self.assertEqual(2, len(props[3]))
        self.assertEqual("cod", props[3]["fish"][0])
        self.assertEqual("haddock", props[3]["fish"][1])
        self.assertEqual("plaice", props[3]["fish"][2])
        self.assertEqual(109, props[3]["number"])

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
        self.gdb = default_graph_db()
        self.node = self.gdb.create_node(self.data)

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
        rels = self.fred.get_relationships(neo4j.Direction.BOTH, "REALLY LOVES")
        self.assertEqual(1, len(rels))
        self.assertTrue(isinstance(rels[0], neo4j.Relationship))
        self.assertEqual("REALLY LOVES", rels[0].type)
        self.assertEqual(self.fred, rels[0].start_node)
        self.assertEqual(self.wilma, rels[0].end_node)

    def test_get_single_relationship(self):
        rel = self.fred.get_single_relationship(neo4j.Direction.BOTH, "REALLY LOVES")
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
        self.assertTrue(self.fred.is_related_to(self.wilma, neo4j.Direction.BOTH, "REALLY LOVES"))

    def test_explicit_is_related_to_outgoing(self):
        self.assertTrue(self.fred.is_related_to(self.wilma, neo4j.Direction.OUTGOING, "REALLY LOVES"))

    def test_explicit_is_related_to_incoming(self):
        self.assertFalse(self.fred.is_related_to(self.wilma, neo4j.Direction.INCOMING, "REALLY LOVES"))

    def test_implicit_is_related_to(self):
        self.assertTrue(self.fred.is_related_to(self.wilma))

    def test_get_relationships_with(self):
        rels = self.fred.get_relationships_with(self.wilma, neo4j.Direction.BOTH, "REALLY LOVES")
        self.assertEqual(1, len(rels))
        self.assertEqual(self.fred, rels[0].start_node)
        self.assertEqual(self.wilma, rels[0].end_node)

    def tearDown(self):
        self.gdb.delete(self.fred_and_wilma, self.fred, self.wilma)


class NewCreateTestCase(unittest.TestCase):

    def setUp(self):
        self.graph_db = default_graph_db()

    def test_can_create_single_node(self):
        results = self.graph_db.create(
            {"name": "Alice"}
        )
        self.assertIsNotNone(results)
        self.assertIsInstance(results, list)
        self.assertEqual(1, len(results))
        self.assertIsInstance(results[0], neo4j.Node)
        self.assertTrue("name" in results[0])
        self.assertEqual("Alice", results[0]["name"])

    def test_can_create_simple_graph(self):
        results = self.graph_db.create(
            {"name": "Alice"},
            {"name": "Bob"},
            (0, "KNOWS", 1)
        )
        self.assertIsNotNone(results)
        self.assertIsInstance(results, list)
        self.assertEqual(3, len(results))
        self.assertIsInstance(results[0], neo4j.Node)
        self.assertTrue("name" in results[0])
        self.assertEqual("Alice", results[0]["name"])
        self.assertIsInstance(results[1], neo4j.Node)
        self.assertTrue("name" in results[1])
        self.assertEqual("Bob", results[1]["name"])
        self.assertIsInstance(results[2], neo4j.Relationship)
        self.assertEqual("KNOWS", results[2].type)
        self.assertEqual(results[0], results[2].start_node)
        self.assertEqual(results[1], results[2].end_node)

    def test_can_create_simple_graph_with_rel_data(self):
        results = self.graph_db.create(
            {"name": "Alice"},
            {"name": "Bob"},
            (0, "KNOWS", 1, {"since": 1996})
        )
        self.assertIsNotNone(results)
        self.assertIsInstance(results, list)
        self.assertEqual(3, len(results))
        self.assertIsInstance(results[0], neo4j.Node)
        self.assertTrue("name" in results[0])
        self.assertEqual("Alice", results[0]["name"])
        self.assertIsInstance(results[1], neo4j.Node)
        self.assertTrue("name" in results[1])
        self.assertEqual("Bob", results[1]["name"])
        self.assertIsInstance(results[2], neo4j.Relationship)
        self.assertEqual("KNOWS", results[2].type)
        self.assertEqual(results[0], results[2].start_node)
        self.assertEqual(results[1], results[2].end_node)
        self.assertTrue("since" in results[2])
        self.assertEqual(1996, results[2]["since"])

    def test_can_create_graph_against_existing_node(self):
        ref_node = self.graph_db.get_reference_node()
        results = self.graph_db.create(
            {"name": "Alice"},
            (ref_node, "PERSON", 0)
        )
        self.assertIsNotNone(results)
        self.assertIsInstance(results, list)
        self.assertEqual(2, len(results))
        self.assertIsInstance(results[0], neo4j.Node)
        self.assertTrue("name" in results[0])
        self.assertEqual("Alice", results[0]["name"])
        self.assertIsInstance(results[1], neo4j.Relationship)
        self.assertEqual("PERSON", results[1].type)
        self.assertEqual(ref_node, results[1].start_node)
        self.assertEqual(results[0], results[1].end_node)

    def test_fails_on_bad_reference(self):
        self.assertRaises(ValueError, self.graph_db.create,
            {"name": "Alice"},
            (0, "KNOWS", 1)
        )

    def test_can_create_big_graph(self):
        size = 2000
        nodes = [
            {"number": i}
            for i in range(size)
        ]
        results = self.graph_db.create(*nodes)
        self.assertIsNotNone(results)
        self.assertIsInstance(results, list)
        self.assertEqual(size, len(results))
        for i in range(size):
            self.assertIsInstance(results[i], neo4j.Node)

class MultipleNodeTestCase(unittest.TestCase):

    flintstones = [
        {"name": "Fred"},
        {"name": "Wilma"},
        {"name": "Barney"},
        {"name": "Betty"}
    ]

    def setUp(self):
        self.gdb = default_graph_db()
        self.ref_node = self.gdb.get_reference_node()
        self.nodes = self.gdb.create(*self.flintstones)

    def test_is_created(self):
        self.assertIsNotNone(self.nodes)
        self.assertEqual(len(self.nodes), len(self.flintstones))

    def test_has_correct_properties(self):
        self.assertEqual([
            node.get_properties()
            for node in self.nodes
        ], self.flintstones)

    def test_create_relationships(self):
        rels = self.gdb.create(*[
            (self.ref_node, "FLINTSTONE", node)
            for node in self.nodes
        ])
        self.gdb.delete(*rels)
        self.assertEqual(len(self.nodes), len(rels))

    def tearDown(self):
        self.gdb.delete(*self.nodes)


#class IteratorTestCase(unittest.TestCase):
#
#    def setUp(self):
#        self.graph_db = default_graph_db()
#
#    def test_rel_iterator(self):
#        ref_node = self.graph_db.get_reference_node()
#        for rel in self.graph_db.relationships(start_node=ref_node,
#            type=None):
#            print rel

if __name__ == '__main__':
    unittest.main()

