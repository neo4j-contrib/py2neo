#/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
sys.path.append("../src") 

from py2neo import neo4j
import unittest

class GraphDatabaseServiceTest(unittest.TestCase):

	def test_get_reference_node(self):
		ref_node = gdb.get_reference_node()
		self.assertIsNotNone(ref_node)

class SingleNodeTestCase(unittest.TestCase):

	data = {
		"true": True,
		"false": False,
		"int": 42,
		"float": 3.141592653589,
		"long": 9223372036854775807L,
		"bytes": bytes([65, 66, 67]),
		"str": "This is a test",
		"unicode": u"これはテストです",
		"boolean_list": [True, False, True, True, False],
		"int_list": [1, 1, 2, 3, 5, 8, 13, 21, 35],
		"str_list": ["red", "orange", "yellow", "green", "blue", "indigo", "violet"]
	}

	def setUp(self):
		self.node = gdb.create_node(self.data)

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
		self.node["long"] = 9223372036854775808L

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
		self.nodes = gdb.create_nodes(*self.flintstones)

	def test_is_created(self):
		self.assertIsNotNone(self.nodes)
		self.assertEqual(len(self.nodes), len(self.flintstones))

	def test_has_correct_properties(self):
		self.assertEqual([
			node.get_properties()
			for node in self.nodes
		], self.flintstones)

	def tearDown(self):
		gdb.delete(*self.nodes)

if __name__ == '__main__':
	gdb_uri = "http://localhost:7474/db/data/"
	try:
		gdb = neo4j.GraphDatabaseService(gdb_uri)
		print "Attached to GraphDatabaseService at %s" % (gdb._uri)
	except IOError:
		sys.exit("Unable to attach to GraphDatabaseService at %s" % (gdb_uri))
	unittest.main()

