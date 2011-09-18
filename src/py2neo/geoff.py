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

	DESCRIPTOR_PATTERN               = re.compile(r"^(\((\w+)\)(-\[(\w*):(\w+)\]->\((\w+)\))?)(\s+(.*))?")
	NODE_INDEX_ENTRY_PATTERN         = re.compile(r"^(\{(\w+)\}->\((\w+)\))(\s+(.*))?")
	RELATIONSHIP_INDEX_ENTRY_PATTERN = re.compile(r"^(\{(\w+)\}->\[(\w+)\])(\s+(.*))?")

	def __init__(self, file, gdb):
		self.file = file
		self.gdb = gdb

	def load(self):
		# Stage 1: parse file and load into memory
		first_node_id = None
		nodes = {}
		rels = []
		named_rels = {}
		node_index_entries = {}
		rel_index_entries = {}
		line_no = 0
		for line in self.file:
			# increment line no and trim whitespace from current line
			line_no, line = line_no + 1, line.strip()
			# skip blank lines and comments
			if line == "" or line.startswith("#"):
				continue
			# try to identify line as node or relationship descriptor
			m = self.DESCRIPTOR_PATTERN.match(line)
			# firstly, try as a relationship descriptor
			if m and m.group(3):
				(start_node, name, type, end_node) = (
					unicode(m.group(2)),
					unicode(m.group(4)),
					unicode(m.group(5)),
					unicode(m.group(6))
				)
				if start_node not in nodes or end_node not in nodes:
					raise ValueError("Invalid node reference on line %d: %s" % (line_no, repr(m.group(1))))
				rel = {
					'start_node': start_node,
					'end_node': end_node,
					'type': type,
					'data': json.loads(m.group(8) or 'null')
				}
				if len(name or "") > 0:
					named_rels[name] = len(rels)
				rels.append(rel)
				continue
			# secondly, try as a node descriptor
			if m:
				node_id = unicode(m.group(2))
				if node_id in nodes:
					raise ValueError("Duplicate node on line %d: %s" % (line_no, repr(line)))
				nodes[node_id] = json.loads(m.group(8) or 'null')
				first_node_id = first_node_id or node_id
				continue
			# neither of those, so try as a node index entry descriptor
			m = self.NODE_INDEX_ENTRY_PATTERN.match(line)
			if m:
				(index, node) = (
					unicode(m.group(2)),
					unicode(m.group(3))
				)
				data = json.loads(m.group(5) or 'null')
				if index not in node_index_entries:
					node_index_entries[index] = {}
				if node not in node_index_entries[index]:
					node_index_entries[index][node] = {}
				if data:
					node_index_entries[index][node].update(data)
				continue
			# or as a relationship index entry descriptor
			m = self.RELATIONSHIP_INDEX_ENTRY_PATTERN.match(line)
			if m:
				(index, rel) = (
					unicode(m.group(2)),
					unicode(m.group(3))
				)
				data = json.loads(m.group(5) or 'null')
				if index not in rel_index_entries:
					rel_index_entries[index] = {}
				if rel not in rel_index_entries[index]:
					rel_index_entries[index][rel] = {}
				if data:
					rel_index_entries[index][rel].update(data)
				continue
			# no idea then... this line is invalid
			raise ValueError("Cannot parse line %d: %s" % (line_no, repr(line)))
		# Stage 2: write data from memory to graph
		if first_node_id is None:
			return None
		# unzip nodes into keys and data
		z = zip(*nodes.items())
		# create nodes using batch operation
		nodes = dict(zip(*[
			z[0],
			self.gdb.create_nodes(*z[1])
		]))
		# create relationships
		rels = self.gdb.create_relationships(*[
			{
				'start_node': nodes[rel['start_node']],
				'end_node': nodes[rel['end_node']],
				'type': rel['type'],
				'data': rel['data']
			}
			for rel in rels
		])
		# create node index entries
		if len(node_index_entries) > 0:
			for index_key in node_index_entries.keys():
				index = self.gdb.get_node_index(index_key)
				index.start_batch()
				for node_key in node_index_entries[index_key].keys():
					node = nodes[node_key]
					for (key, value) in node_index_entries[index_key][node_key].items():
						index.add(node, key, value)
				index.submit_batch()
		# create relationship index entries
		if len(rel_index_entries) > 0:
			for index_key in rel_index_entries.keys():
				index = self.gdb.get_relationship_index(index_key)
				index.start_batch()
				for rel_key in rel_index_entries[index_key].keys():
					rel = rels[named_rels[rel_key]]
					for (key, value) in rel_index_entries[index_key][rel_key].items():
						index.add(rel, key, value)
				index.submit_batch()
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


