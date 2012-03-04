#/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
PY3K = sys.version_info[0] >= 3

__author__    = "Nigel Small <py2neo@nigelsmall.org>"
__copyright__ = "Copyright 2011 Nigel Small"
__license__   = "Apache License, Version 2.0"

from py2neo import neo4j
import unittest

gdb_uri = "http://localhost:7474/db/data/"

UNICODE_TEST_STR = ""
for ch in [0x3053,0x308c,0x306f,0x30c6,0x30b9,0x30c8,0x3067,0x3059]:
	UNICODE_TEST_STR += chr(ch) if PY3K else unichr(ch)


def get_gdb():
	try:
		return neo4j.GraphDatabaseService(gdb_uri)
	except IOError:
		sys.exit("Unable to attach to GraphDatabaseService at {0}".format(gdb_uri))


class GraphDatabaseServiceTest(unittest.TestCase):

	def setUp(self):
		self.gdb = get_gdb()
		print("Neo4j Version: {0}".format(repr(self.gdb._neo4j_version)))

	def test_get_reference_node(self):
		ref_node = self.gdb.get_reference_node()
		self.assertIsNotNone(ref_node)

	def test_create_node(self):
		self.gdb.create_node()

	def test_create_node_with_property_dict(self):
		node = self.gdb.create_node({"foo": "bar"})
		self.assertEqual("bar", node["foo"])

	def test_create_node_with_property_args(self):
		node = self.gdb.create_node(foo="bar")
		self.assertEqual("bar", node["foo"])

	def test_create_node_with_mixed_property_types(self):
		node = self.gdb.create_node(
			{"true": False},
			{"number": 13, "foo": "bar", "true": True},
			fish="chips", number=109
		)
		self.assertEqual(4, len(node.get_properties()))
		self.assertEqual("chips", node["fish"])
		self.assertEqual("bar", node["foo"])
		self.assertEqual(109, node["number"])
		self.assertEqual(True, node["true"])

	def test_create_multiple_nodes(self):
		nodes = self.gdb.create_nodes(
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
		nodes = self.gdb.create_nodes(
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
		self.gdb = get_gdb()
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


class MultipleNodeTestCase(unittest.TestCase):

	flintstones = [
		{"name": "Fred"},
		{"name": "Wilma"},
		{"name": "Barney"},
		{"name": "Betty"}
	]

	def setUp(self):
		self.gdb = get_gdb()
		self.ref_node = self.gdb.get_reference_node()
		self.nodes = self.gdb.create_nodes(*self.flintstones)

	def test_is_created(self):
		self.assertIsNotNone(self.nodes)
		self.assertEqual(len(self.nodes), len(self.flintstones))

	def test_has_correct_properties(self):
		self.assertEqual([
			node.get_properties()
			for node in self.nodes
		], self.flintstones)

	def test_create_relationships(self):
		rels = self.gdb.create_relationships(*[
			{
				"start_node": self.ref_node,
				"end_node": node,
				"type": "FLINTSTONE"
			}
			for node in self.nodes
		])
		self.gdb.delete(*rels)
		self.assertEqual(len(self.nodes), len(rels))
		
	def test_simple_traverse(self):
		td = None
		rel0 = self.nodes[0].create_relationship_to(self.nodes[1], "FLINTSTONE", {})
		rel1 = self.nodes[1].create_relationship_to(self.nodes[2], "FLINTSTONE", {})
		#Created Relationship Fred -> Wilma -> Barney
		td = self.nodes[0].traverse(order = "depth_first",
							relationships = ("FLINTSTONE",),
							prune = ("javascript", "position.endNode().getProperty('name') == 'Barney';"),
							max_depth=2)
		
		self.assertEqual(2, len(td.nodes))
		self.gdb.delete(rel0)
		self.gdb.delete(rel1)

	def tearDown(self):
		self.gdb.delete(*self.nodes)



class IndexTestCase(unittest.TestCase):

	def setUp(self):
		self.gdb = get_gdb()

	def test_get_node_index(self):
		index1 = self.gdb.get_node_index("index1")
		self.assertIsNotNone(index1)

	def test_add_node_to_index(self):
		index1 = self.gdb.get_node_index("index1")
		ref_node = self.gdb.get_reference_node()
		index1.add(ref_node, "foo", "bar")
		s = index1.get("foo", "bar")
		print("Found index entries: {0}".format(s))
		found = -1
		for i in range(len(s)):
			if s[i]._uri == ref_node._uri:
				found = i
		self.assertTrue(found >= 0)
		index1.remove(s[found])

	def test_add_node_to_index_with_spaces(self):
		index1 = self.gdb.get_node_index("index1")
		ref_node = self.gdb.get_reference_node()
		index1.add(ref_node, "foo bar", "bar foo")
		s = index1.get("foo bar", "bar foo")
		print("Found index entries: {0}".format(s))
		self.assertEqual(s[0]._uri, ref_node._uri)
		index1.remove(s[0])

	def test_add_node_to_index_with_odd_chars(self):
		index1 = self.gdb.get_node_index("index1")
		ref_node = self.gdb.get_reference_node()
		index1.add(ref_node, "@!%#", "!\"£$%^&*()")
		s = index1.get("@!%#", "!\"£$%^&*()")
		print("Found index entries: {0}".format(s))
		self.assertEqual(s[0]._uri, ref_node._uri)
		index1.remove(s[0])

	def test_node_index_query(self):
		index1 = self.gdb.get_node_index("index1")
		node1 = self.gdb.create_node()
		node2 = self.gdb.create_node()
		node3 = self.gdb.create_node()
		index1.add(node1, "colour", "red")
		index1.add(node2, "colour", "green")
		index1.add(node3, "colour", "blue")
		s = index1.query("colour:*r*")
		print("Found index entries: {0}".format(s))
		self.assertTrue(node1 in s)
		self.assertTrue(node2 in s)
		self.assertFalse(node3 in s)


if __name__ == '__main__':
	unittest.main()

