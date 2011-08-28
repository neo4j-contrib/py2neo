#!/usr/bin/env python

# Copyright 2011 Nigel Small
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
# 	http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
GEOFF file handling (see U{http://py2neo.org/geoff/}).
"""


import json
import neo4j
import re


__version__   = "0.95"
__author__    = "Nigel Small <py2neo@nigelsmall.org>"
__copyright__ = "Copyright 2011 Nigel Small"
__license__   = "Apache License, Version 2.0"


class Dumper(object):

	def __init__(self, file, eol=None):
		self.write = file.write
		self.eol = eol or "\r\n"

	def dump(self, paths):
		nodes = {}
		rels = {}
		for path in paths:
			nodes.update(dict([
				(node._id, node)
				for node in path.nodes
			]))
			rels.update(dict([
				(rel._id, rel)
				for rel in path.relationships
			]))
		self.write(self.eol.join([
			"%s\t%s" % (
				unicode(node),
				json.dumps(node.get_properties(), separators=(',',':'))
			)
			for node in nodes.values()
		]))
		self.write(self.eol)
		self.write(self.eol.join([
			"%s%s%s\t%s" % (
				unicode(rel.get_start_node()),
				unicode(rel),
				unicode(rel.get_end_node()),
				json.dumps(rel.get_properties(), separators=(',',':'))
			)
			for rel in rels.values()
		]))


class Loader(object):

	DESCRIPTOR_PATTERN = re.compile(r"^(\((\w+)\)(-\[:(\w+)\]->\((\w+)\))?)(\s+(.*))?")
	INDEX_ENTRY_PATTERN = re.compile(r"^(\{(\w+)\}->\((\w+)\))(\s+(.*))?")

	def __init__(self, file, gdb):
		self.file = file
		self.gdb = gdb

	def load(self):
		first_node_id = None
		nodes = {}
		rels = {}
		index_entries = {}
		line_no = 0
		for line in self.file:
			line = line.strip()
			line_no += 1
			if line == "" or line.startswith("#"):
				pass
			else:
				m = self.DESCRIPTOR_PATTERN.match(line)
				if m and m.group(3):
					(start_node, type, end_node) = (
						unicode(m.group(2)),
						unicode(m.group(4)),
						unicode(m.group(5))
					)
					if start_node in nodes and end_node in nodes:
						rels[(start_node, type, end_node)] = json.loads(m.group(7) or 'null')
					else:
						raise ValueError("Invalid node reference on line %d: %s" % (line_no, repr(m.group(1))))
				elif m:
					node_id = unicode(m.group(2))
					if node_id not in nodes:
						nodes[node_id] = json.loads(m.group(7) or 'null')
						first_node_id = first_node_id or node_id
					else:
						raise ValueError("Duplicate node on line %d: %s" % (line_no, repr(line)))
				else:
					m = self.INDEX_ENTRY_PATTERN.match(line)
					if m:
						(index, node) = (
							unicode(m.group(2)),
							unicode(m.group(3))
						)
						data = json.loads(m.group(5) or 'null')
						if index not in index_entries:
							index_entries[index] = {}
						if node not in index_entries[index]:
							index_entries[index][node] = {}
						if data:
							index_entries[index][node].update(data)
					else:
						raise ValueError("Cannot parse line %d: %s" % (line_no, repr(line)))
		if first_node_id is None:
			return None
		else:
			for key in nodes.keys():
				nodes[key] = self.gdb.create_node(nodes[key])
			for key in rels.keys():
				rel = rels[key]
				rels[key] = nodes[key[0]].create_relationship_to(nodes[key[2]], key[1], rels[key])
			if len(index_entries) > 0:
				for index_key in index_entries.keys():
					index = self.gdb.get_node_index(index_key)
					for node_key in index_entries[index_key].keys():
						node = nodes[node_key]
						for (key, value) in index_entries[index_key][node_key].items():
							index.add(node, key, value)
			return nodes[first_node_id]


try:
	from cStringIO import StringIO
except ImportError:
	from StringIO import StringIO

def dump(paths, file):
	Dumper(file).dump(paths)

def dumps(paths):
	file = StringIO()
	Dumper(file).dump(paths)
	return file.getvalue()

def load(file, gdb):
	return Loader(file, gdb).load()

def loads(str, gdb):
	file = StringIO(str)
	return Loader(file, gdb).load()


