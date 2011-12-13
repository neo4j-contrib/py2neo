#/usr/bin/env python
# -*- coding: utf-8 -*-

__author__    = "Nigel Small <py2neo@nigelsmall.org>"
__copyright__ = "Copyright 2011 Nigel Small"
__license__   = "Apache License, Version 2.0"

import unittest

from py2neo import geoff, neo4j

try:
	from cStringIO import StringIO
except ImportError:
	from StringIO import StringIO


class GEOFFTestCase(unittest.TestCase):

	def setUp(self):
		super(GEOFFTestCase, self).setUp()
		self.graph_db = neo4j.GraphDatabaseService("http://localhost:7474/db/data/")

	def test_blank_lines(self):
		file = StringIO('\n')
		batch = geoff.Loader(self.graph_db).compile(file)
		self.assertEqual('[]', repr(batch))

	def test_lines_of_whitespace(self):
		file = StringIO('\t    ')
		batch = geoff.Loader(self.graph_db).compile(file)
		self.assertEqual('[]', repr(batch))

	def test_comments(self):
		file = StringIO('# this is a comment')
		batch = geoff.Loader(self.graph_db).compile(file)
		self.assertEqual('[]', repr(batch))

	def test_hook_descriptors(self):
		file = StringIO('{foo}')
		batch = geoff.Loader(self.graph_db).compile(file, foo=self.graph_db.get_reference_node())
		self.assertEqual('[{"method":"PUT","to":"/node/0/properties","body":null,"id":0}]', repr(batch))

	def test_hook_descriptors_with_data(self):
		file = StringIO('{foo} {"pi":3.1415}')
		batch = geoff.Loader(self.graph_db).compile(file, foo=self.graph_db.get_reference_node())
		self.assertEqual('[{"method":"PUT","to":"/node/0/properties","body":{"pi":3.1415},"id":0}]', repr(batch))

	def test_node_descriptors(self):
		file = StringIO('(foo)')
		batch = geoff.Loader(self.graph_db).compile(file)
		self.assertEqual('[{"method":"POST","to":"/node","body":null,"id":0}]', repr(batch))

	def test_node_descriptors_with_data(self):
		file = StringIO('(foo) {"pi":3.1415}')
		batch = geoff.Loader(self.graph_db).compile(file)
		self.assertEqual('[{"method":"POST","to":"/node","body":{"pi":3.1415},"id":0}]', repr(batch))

	# TODO: Tests for relationship descriptors

	def test_hook_index_inclusions(self):
		file = StringIO('{bob}<=|index1| {"foo":"bar"}')
		batch = geoff.Loader(self.graph_db).compile(file, bob=self.graph_db.get_reference_node())
		# Currently only works for v1.5+
		self.assertEqual(
			'[{"method":"POST","to":"/index/node/index1","body":{"uri":"/node/0","key":"foo","value":"bar"},"id":0}]',
			repr(batch)
		)

	def test_node_index_inclusions(self):
		file = StringIO("""\
(bob)
(bob)<=|index1| {"foo":"bar"}
""")
		batch = geoff.Loader(self.graph_db).compile(file)
		# Currently only works for v1.5+
		self.assertEqual(
			'[' +
			'{"method":"POST","to":"/node","body":null,"id":0},' +
			'{"method":"POST","to":"/index/node/index1","body":{"uri":"{0}","key":"foo","value":"bar"},"id":1}' +
			']',
			repr(batch)
		)

	def test_relationship_index_inclusions(self):
		file = StringIO("""\
(alice)
(bob)
(alice)-[rel:KNOWS]->(bob)
[rel]<=|index1| {"foo":"bar"}
""")
		batch = geoff.Loader(self.graph_db).compile(file)
		# Currently only works for v1.5+
		self.assertEqual(
			'[' +
				'{"method":"POST","to":"/node","body":null,"id":0},' +
				'{"method":"POST","to":"/node","body":null,"id":1},' +
				'{"method":"POST","to":"{0}/relationships","body":{"type":"KNOWS","to":"{1}","data":null},"id":2},' +
				'{"method":"POST","to":"/index/relationship/index1","body":{"uri":"{2}","key":"foo","value":"bar"},"id":3}' +
			']',
			repr(batch)
		)
