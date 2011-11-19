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


import argparse
try:
	import json
except:
	import simplejson as json
import neo4j
import re
import sys
import warnings

from urllib import quote


__author__    = "Nigel Small <py2neo@nigelsmall.org>"
__copyright__ = "Copyright 2011 Nigel Small"
__license__   = "Apache License, Version 2.0"


NODE_DESCRIPTOR                      = re.compile(r"^\((\w+)\)$")
NODE_INDEX_ENTRY                     = re.compile(r"^\|(\w+)\|->\((\w+)\)$")
HOOK_DESCRIPTOR                      = re.compile(r"^\{(\w+)\}$")
HOOK_INDEX_ENTRY                     = re.compile(r"^\|(\w+)\|->\{(\w+)\}$")
NODE_TO_NODE_RELATIONSHIP_DESCRIPTOR = re.compile(r"^\((\w+)\)-\[(\w*):(\w+)\]->\((\w+)\)$")
HOOK_TO_NODE_RELATIONSHIP_DESCRIPTOR = re.compile(r"^\{(\w+)\}-\[(\w*):(\w+)\]->\((\w+)\)$")
NODE_TO_HOOK_RELATIONSHIP_DESCRIPTOR = re.compile(r"^\((\w+)\)-\[(\w*):(\w+)\]->\{(\w+)\}$")
HOOK_TO_HOOK_RELATIONSHIP_DESCRIPTOR = re.compile(r"^\{(\w+)\}-\[(\w*):(\w+)\]->\{(\w+)\}$")
RELATIONSHIP_INDEX_ENTRY             = re.compile(r"^\|(\w+)\|->\[(\w+)\]$")

OLD_NODE_INDEX_ENTRY         = re.compile(r"^(\{(\w+)\}->\((\w+)\))$")
OLD_RELATIONSHIP_INDEX_ENTRY = re.compile(r"^(\{(\w+)\}->\[(\w+)\])$")


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


class Batch(object):

	def __init__(self, graphdb, **hooks):
		self._graphdb = graphdb
		self._hooks = hooks
		self._named_nodes = {}
		self._named_relationships = {}
		self._batch = []
		self.first_node_id = None

	def _assert_node_name_does_not_exist(self, node_name):
		if node_name in self._named_nodes:
			raise ValueError("Duplicate node name \"%s\"" % (node_name))

	def _assert_node_name_exists(self, node_name):
		if node_name not in self._named_nodes:
			raise ValueError("Unknown node name \"%s\"" % (node_name))

	def _assert_hook_name_exists(self, hook_name):
		if hook_name not in self._hooks:
			raise ValueError("Unknown hook name \"%s\"" % (hook_name))

	def _assert_rel_name_does_not_exist(self, rel_name):
		if rel_name is not None and len(rel_name) > 0 and rel_name in self._named_nodes:
			raise ValueError("Duplicate relationship name \"%s\"" % (rel_name))

	def _assert_hook_is_a_node(self, hook_name):
		if not isinstance(self._hooks[hook_name], neo4j.Node):
			raise ValueError("Hook \"%s\" is not a node" % (hook_name))

	def _assert_rel_name_exists(self, rel_name):
		if rel_name not in self._named_relationships:
			raise ValueError("Unknown relationship name \"%s\"" % (rel_name))

	def create_node(self, node_name, data):
		self._assert_node_name_does_not_exist(node_name)
		job_id = len(self._batch)
		self._batch.append('{"method":"POST","to":"/node","body":%s,"id":%d}' % (
			json.dumps(data, separators=(',',':')),
			job_id
		))
		if node_name is not None and len(node_name) > 0:
			self._named_nodes[node_name] = job_id
		if self.first_node_id is None:
			self.first_node_id = job_id

	def add_node_index_entry(self, index_name, node_name, key, value):
		self._assert_node_name_exists(node_name)
		job_id = len(self._batch)
		if self._graphdb._neo4j_version >= (1, 5):
			self._batch.append('{"method":"POST","to":"/index/node/%s","body":{"uri":"{%d}","key":%s,"value":%s},"id":%d}' % (
				index_name,
				self._named_nodes[node_name],
				json.dumps(key, separators=(',',':')),
				json.dumps(value, separators=(',',':')),
				job_id
			))
		else:
			self._batch.append('{"method":"POST","to":"/index/node/%s/%s/%s","body":"{%d}","id":%d}' % (
				index_name,
				quote(key, "") if isinstance(key, basestring) else key,
				quote(value, "") if isinstance(value, basestring) else value,
				self._named_nodes[node_name],
				job_id
			))

	def update_hook_properties(self, hook_name, data):
		self._assert_hook_name_exists(hook_name)
		job_id = len(self._batch)
		self._batch.append('{"method":"PUT","to":%s,"body":%s,"id":%d}' % (
			json.dumps(self._hooks[hook_name]._relative_uri + "/properties", separators=(',',':')),
			json.dumps(data, separators=(',',':')),
			job_id
		))

	def add_hook_index_entry(self, index_name, hook_name, key, value):
		self._assert_hook_name_exists(hook_name)
		hook = self._hooks[hook_name]
		if isinstance(hook, neo4j.Node):
			index_type = "node"
		elif isinstance(hook, neo4j.Relationship):
			index_type = "relationship"
		else:
			raise ValueError("Cannot index a hook of type %s" % type(hook))
		job_id = len(self._batch)
		if self._graphdb._neo4j_version >= (1, 5):
			self._batch.append('{"method":"POST","to":"/index/%s/%s","body":{"uri":%s,"key":%s,"value":%s},"id":%d}' % (
				index_type,
				index_name,
				json.dumps(hook._relative_uri, separators=(',',':')),
				json.dumps(key, separators=(',',':')),
				json.dumps(value, separators=(',',':')),
				job_id
			))
		else:
			self._batch.append('{"method":"POST","to":"/index/%s/%s/%s/%s","body":%s,"id":%d}' % (
				index_type,
				index_name,
				quote(key, "") if isinstance(key, basestring) else key,
				quote(value, "") if isinstance(value, basestring) else value,
				json.dumps(hook._relative_uri, separators=(',',':')),
				job_id
			))

	def create_node_to_node_relationship(self, start_node_name, rel_name, rel_type, end_node_name, data):
		self._assert_rel_name_does_not_exist(rel_name)
		self._assert_node_name_exists(start_node_name)
		self._assert_node_name_exists(end_node_name)
		job_id = len(self._batch)
		self._batch.append('{"method":"POST","to":"{%d}/relationships","body":{"type":"%s","to":"{%d}","data":%s},"id":%d}' % (
			self._named_nodes[start_node_name],
			rel_type,
			self._named_nodes[end_node_name],
			json.dumps(data, separators=(',',':')),
			job_id
		))
		if rel_name is not None and len(rel_name) > 0:
			self._named_relationships[rel_name] = job_id

	def create_hook_to_node_relationship(self, start_hook_name, rel_name, rel_type, end_node_name, data):
		self._assert_rel_name_does_not_exist(rel_name)
		self._assert_hook_name_exists(start_hook_name)
		self._assert_hook_is_a_node(start_hook_name)
		self._assert_node_name_exists(end_node_name)
		job_id = len(self._batch)
		self._batch.append('{"method":"POST","to":%s,"body":{"type":"%s","to":"{%d}","data":%s},"id":%d}' % (
			json.dumps(self._hooks[start_hook_name]._relative_uri + "/relationships", separators=(',',':')),
			rel_type,
			self._named_nodes[end_node_name],
			json.dumps(data, separators=(',',':')),
			job_id
		))
		if rel_name is not None and len(rel_name) > 0:
			self._named_relationships[rel_name] = job_id

	def create_node_to_hook_relationship(self, start_node_name, rel_name, rel_type, end_hook_name, data):
		self._assert_rel_name_does_not_exist(rel_name)
		self._assert_node_name_exists(start_node_name)
		self._assert_hook_name_exists(end_hook_name)
		self._assert_hook_is_a_node(end_hook_name)
		job_id = len(self._batch)
		self._batch.append('{"method":"POST","to":"{%d}/relationships","body":{"type":"%s","to":%s,"data":%s},"id":%d}' % (
			self._named_nodes[start_node_name],
			rel_type,
			json.dumps(self._hooks[end_hook_name]._relative_uri, separators=(',',':')),
			json.dumps(data, separators=(',',':')),
			job_id
		))
		if rel_name is not None and len(rel_name) > 0:
			self._named_relationships[rel_name] = job_id

	def create_hook_to_hook_relationship(self, start_hook_name, rel_name, rel_type, end_hook_name, data):
		self._assert_rel_name_does_not_exist(rel_name)
		self._assert_hook_name_exists(start_hook_name)
		self._assert_hook_is_a_node(start_hook_name)
		self._assert_hook_name_exists(end_hook_name)
		self._assert_hook_is_a_node(end_hook_name)
		job_id = len(self._batch)
		self._batch.append('{"method":"POST","to":%s,"body":{"type":"%s","to":%s,"data":%s},"id":%d}' % (
			json.dumps(self._hooks[start_hook_name]._relative_uri + "/relationships", separators=(',',':')),
			rel_type,
			json.dumps(self._hooks[end_hook_name]._relative_uri, separators=(',',':')),
			json.dumps(data, separators=(',',':')),
			job_id
		))
		if rel_name is not None and len(rel_name) > 0:
			self._named_relationships[rel_name] = job_id

	def add_relationship_index_entry(self, index_name, relationship_name, key, value):
		self._assert_rel_name_exists(relationship_name)
		job_id = len(self._batch)
		if self._graphdb._neo4j_version >= (1, 5):
			self._batch.append('{"method":"POST","to":"/index/relationship/%s","body":{"uri":"{%d}","key":%s,"value":%s},"id":%d}' % (
				index_name,
				self._named_relationships[relationship_name],
				json.dumps(key, separators=(',',':')),
				json.dumps(value, separators=(',',':')),
				job_id
			))
		else:
			self._batch.append('{"method":"POST","to":"/index/relationship/%s/%s/%s","body":"{%d}","id":%d}' % (
				index_name,
				quote(key, "") if isinstance(key, basestring) else key,
				quote(value, "") if isinstance(value, basestring) else value,
				self._named_relationships[relationship_name],
				job_id
			))

	def submit(self):
		return self._graphdb._request(
			'POST',
			self._graphdb._batch_uri,
			"[" + ",".join(self._batch) + "]"
		)
		self._batch = []


class Loader(object):

	def __init__(self, file, graphdb, **hooks):
		self.file = file
		self.graphdb = graphdb
		self.hooks = hooks

	def load(self):

		def parse(descriptor, data=None):
			# try as a node descriptor
			m = NODE_DESCRIPTOR.match(descriptor)
			if m:
				return (
					NODE_DESCRIPTOR,
					{
						"node_name": unicode(m.group(1))
					},
					data
				)
			# try as a node index entry
			m = NODE_INDEX_ENTRY.match(descriptor)
			if m:
				return (
					NODE_INDEX_ENTRY,
					{
						"index_name": unicode(m.group(1)),
						"node_name": unicode(m.group(2))
					},
					data
				)
			# try as a hook descriptor
			m = HOOK_DESCRIPTOR.match(descriptor)
			if m:
				return (
					HOOK_DESCRIPTOR,
					{
						"hook_name": unicode(m.group(1))
					},
					data
				)
			# try as a hook index entry
			m = HOOK_INDEX_ENTRY.match(descriptor)
			if m:
				return (
					HOOK_INDEX_ENTRY,
					{
						"index_name": unicode(m.group(1)),
						"hook_name": unicode(m.group(2))
					},
					data
				)
			# try as a node-to-node relationship descriptor
			m = NODE_TO_NODE_RELATIONSHIP_DESCRIPTOR.match(descriptor)
			if m:
				return (
					NODE_TO_NODE_RELATIONSHIP_DESCRIPTOR,
					{
						"start_node_name": unicode(m.group(1)),
						"rel_name": unicode(m.group(2)),
						"rel_type": unicode(m.group(3)),
						"end_node_name": unicode(m.group(4))
					},
					data
				)
			# try as a hook-to-node relationship descriptor
			m = HOOK_TO_NODE_RELATIONSHIP_DESCRIPTOR.match(descriptor)
			if m:
				return (
					HOOK_TO_NODE_RELATIONSHIP_DESCRIPTOR,
					{
						"start_hook_name": unicode(m.group(1)),
						"rel_name": unicode(m.group(2)),
						"rel_type": unicode(m.group(3)),
						"end_node_name": unicode(m.group(4))
					},
					data
				)
			# try as a node-to-hook relationship descriptor
			m = NODE_TO_HOOK_RELATIONSHIP_DESCRIPTOR.match(descriptor)
			if m:
				return (
					NODE_TO_HOOK_RELATIONSHIP_DESCRIPTOR,
					{
						"start_node_name": unicode(m.group(1)),
						"rel_name": unicode(m.group(2)),
						"rel_type": unicode(m.group(3)),
						"end_hook_name": unicode(m.group(4))
					},
					data
				)
			# try as a hook-to-hook relationship descriptor
			m = HOOK_TO_HOOK_RELATIONSHIP_DESCRIPTOR.match(descriptor)
			if m:
				return (
					HOOK_TO_HOOK_RELATIONSHIP_DESCRIPTOR,
					{
						"start_hook_name": unicode(m.group(1)),
						"rel_name": unicode(m.group(2)),
						"rel_type": unicode(m.group(3)),
						"end_hook_name": unicode(m.group(4))
					},
					data
				)
			# try as a relationship index entry
			m = RELATIONSHIP_INDEX_ENTRY.match(descriptor)
			if m:
				return (
					RELATIONSHIP_INDEX_ENTRY,
					{
						"index_name": unicode(m.group(1)),
						"rel_name": unicode(m.group(2))
					},
					data
				)
			""" --- START OF DEPRECATED SYNTAXES --- """
			""" --- REMOVE PRIOR TO V1.0 RELEASE --- """
			# deprecated node index entry
			m = OLD_NODE_INDEX_ENTRY.match(descriptor)
			if m:
				index_name = unicode(m.group(2))
				warnings.warn("The index entry syntax {%s} is deprecated - please use |%s| instead" % (
					index_name,
					index_name
				), DeprecationWarning)
				return (
					NODE_INDEX_ENTRY,
					{
						"index_name": unicode(m.group(2)),
						"node_name": unicode(m.group(3))
					},
					data
				)
			# deprecated relationship index entry
			m = OLD_RELATIONSHIP_INDEX_ENTRY.match(descriptor)
			if m:
				index_name = unicode(m.group(2))
				warnings.warn("The index entry syntax {%s} is deprecated - please use |%s| instead" % (
					index_name,
					index_name
				), DeprecationWarning)
				return (
					RELATIONSHIP_INDEX_ENTRY,
					{
						"index_name": unicode(m.group(2)),
						"rel_name": unicode(m.group(3))
					},
					data
				)
			""" --- END OF DEPRECATED SYNTAXES --- """
			# no idea then... this line is invalid
			raise ValueError("Cannot parse line %d: %s" % (line_no, repr(line)))

		def add(descriptor, params, data):
			if descriptor is NODE_DESCRIPTOR:
				batch.create_node(params['node_name'], data)
			elif descriptor is NODE_INDEX_ENTRY:
				if data:
					for key, value in data.items():
						batch.add_node_index_entry(params['index_name'], params['node_name'], key, value)
			elif descriptor is HOOK_DESCRIPTOR:
				batch.update_hook_properties(params['hook_name'], data)
			elif descriptor is HOOK_INDEX_ENTRY:
				if data:
					for key, value in data.items():
						batch.add_hook_index_entry(params['index_name'], params['hook_name'], key, value)
			elif descriptor is NODE_TO_NODE_RELATIONSHIP_DESCRIPTOR:
				batch.create_node_to_node_relationship(
					params['start_node_name'],
					params['rel_name'],
					params['rel_type'],
					params['end_node_name'],
					data
				)
			elif descriptor is HOOK_TO_NODE_RELATIONSHIP_DESCRIPTOR:
				batch.create_hook_to_node_relationship(
					params['start_hook_name'],
					params['rel_name'],
					params['rel_type'],
					params['end_node_name'],
					data
				)
			elif descriptor is NODE_TO_HOOK_RELATIONSHIP_DESCRIPTOR:
				batch.create_node_to_hook_relationship(
					params['start_node_name'],
					params['rel_name'],
					params['rel_type'],
					params['end_hook_name'],
					data
				)
			elif descriptor is HOOK_TO_HOOK_RELATIONSHIP_DESCRIPTOR:
				batch.create_hook_to_hook_relationship(
					params['start_hook_name'],
					params['rel_name'],
					params['rel_type'],
					params['end_hook_name'],
					data
				)
			elif descriptor is RELATIONSHIP_INDEX_ENTRY:
				if data:
					for key, value in data.items():
						batch.add_relationship_index_entry(params['index_name'], params['rel_name'], key, value)

		batch = Batch(self.graphdb, **self.hooks)
		line_no = 0
		for line in self.file:
			# increment line no and trim whitespace from current line
			line_no, line = line_no + 1, line.strip()
			# skip blank lines and comments
			if line == "" or line.startswith("#"):
				continue
			# TODO: match composite descriptors
			bits = line.split(None, 1)
			if len(bits) == 1:
				descriptor, params, data = parse(bits[0])
				#add(bits[0])
			else:
				descriptor, params, data = parse(bits[0], json.loads(bits[1]))
				#add(bits[0], json.loads(bits[1]))
			add(descriptor, params, data)
		results = batch.submit()
		return neo4j.Node(results[batch.first_node_id]['location'])


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

def load(file, graphdb, **hooks):
	return Loader(file, graphdb, **hooks).load()

def loads(str, graphdb, **hooks):
	file = StringIO(str)
	return Loader(file, graphdb, **hooks).load()

if __name__ == "__main__":
	parser = argparse.ArgumentParser(description="Import graph data from a GEOFF file into a Neo4j database.")
	parser.add_argument("-u", metavar="DATABASE_URI", default=None, help="the URI of the destination Neo4j database server")
	parser.add_argument("-f", metavar="SOURCE_FILE", help="the GEOFF file to load")
	parser.add_argument("uri", nargs="*", help="a relative URI of a node or relationship")
	args = parser.parse_args()
	try:
		if args.f:
			source_file = open(args.f, "r")
		else:
			source_file = sys.stdin
	except:
		sys.stderr.write("Failed to open GEOFF file.\r\n")
		sys.exit(1)
	try:
		if args.u:
			dest_graphdb = neo4j.GraphDatabaseService(args.u)
		else:
			dest_graphdb = neo4j.GraphDatabaseService("http://localhost:7474/db/data/")
	except:
		sys.stderr.write("Failed to open destination database.\r\n")
		sys.exit(1)
	hooks = []
	for uri in args.uri:
		if uri.startswith("/node/"):
			hooks.append(neo4j.Node(dest_graphdb._base_uri + uri))
		elif uri.startswith("/relationship/"):
			hooks.append(neo4j.Relationship(dest_graphdb._base_uri + uri))
		else:
			sys.stderr.write("Bad relative entity URI: %s\r\n" % (uri))
			sys.exit(1)
	print "New graph data available from node %s" % (
		load(source_file, dest_graphdb, **dict([
			(str(i), hooks[i])
			for i in range(len(hooks))
		]))
	)

