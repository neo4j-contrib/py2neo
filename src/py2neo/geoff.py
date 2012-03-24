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
Geoff file handling (see `<http://geoff.nigelsmall.net/>`_).
"""

import sys
PY3K = sys.version_info[0] >= 3

__author__    = "Nigel Small <py2neo@nigelsmall.org>"
__copyright__ = "Copyright 2011 Nigel Small"
__license__   = "Apache License, Version 2.0"


try:
	import json
except ImportError:
	import simplejson as json
try:
	from . import neo4j
except ValueError:
	import neo4j
import re
import string

try:
	from io import StringIO
except ImportError:
	from cStringIO import StringIO


class Subgraph(object):

	def __init__(self, *rules):
		self.rules = []
		for rule in rules:
			try:
				self.load(rule)
			except AttributeError:
				self.add(rule)

	def __repr__(self):
		return "Subgraph(" + ", ".join(["'" + str(rule) + "'" for rule in self.rules]) + ")"

	def __str__(self):
		return "\n".join([
			"{0} {1}".format(rule[0], json.dumps(rule[1]))
			for rule in self.rules
		])

	def __json__(self):
		return json.dumps([
			"{0} {1}".format(rule[0], json.dumps(rule[1]))
			for rule in self.rules
		])

	def add(self, *rules):
		for rule in rules:
			if rule and not rule.startswith("#"):
				try:
					rule = re.split("\s+", rule, 1)
					if len(rule) > 1:
						rule[1] = json.loads(rule[1])
				except TypeError:
					pass
				self.rules.append((
					str(rule[0]),
					dict(rule[1]) if len(rule) > 1 else {}
				))

	def load(self, *files):
		for file in files:
			f = file.read()
			try:
				f = json.loads(f)
			except ValueError:
				f = f.splitlines()
			for rule in f:
				self.add(rule)

	def loads(self, str):
		try:
			file = StringIO(str)
		except TypeError:
			file = StringIO(unicode(str))
		self.load(file)


class Dumper(object):

	def __init__(self, file, eol=None):
		self.write = file.write
		self.eol = eol or "\r\n"

	def dump(self, paths):
		nodes = {}
		relationships = {}
		for path in paths:
			nodes.update(dict([
				(node._id, node)
				for node in path.nodes
			]))
			relationships.update(dict([
				(rel._id, rel)
				for rel in path.relationships
			]))
		self.write(self.eol.join([
			"{0}\t{1}".format(
				unicode(node),
				json.dumps(node.get_properties(), separators=(',',':'))
			)
			for node in nodes.values()
		]))
		self.write(self.eol)
		self.write(self.eol.join([
			"{0}{1}{2}\t{3}".format(
				unicode(rel.get_start_node()),
				unicode(rel),
				unicode(rel.get_end_node()),
				json.dumps(rel.get_properties(), separators=(',',':'))
			)
			for rel in relationships.values()
		]))


class Loader(object):

	def __init__(self, graph_db):
		self.graph_db = graph_db
		try:
			self._insert_uri = self.graph_db._extension_uri('GeoffPlugin', 'insert')
			self._delete_uri = self.graph_db._extension_uri('GeoffPlugin', 'delete')
			self._merge_uri  = self.graph_db._extension_uri('GeoffPlugin', 'merge')
		except NotImplementedError:
			self._insert_uri = None
			self._delete_uri = None
			self._merge_uri = None

	def insert(self, subgraph, **params):
		if self._insert_uri:
			response = self.graph_db._post(
				self._insert_uri,
				{'subgraph': [str(rule) for rule in subgraph.rules], 'params': dict(params)}
			)
			return response['params']
		else:
			raise NotImplementedError

	def merge(self, subgraph, **params):
		if self._merge_uri:
			response = self.graph_db._post(
				self._merge_uri,
				{'subgraph': [str(rule) for rule in subgraph.rules], 'params': dict(params)}
			)
			return response['params']
		else:
			raise NotImplementedError

	def delete(self, subgraph, **params):
		if self._delete_uri:
			response = self.graph_db._post(
				self._delete_uri,
				{'subgraph': [str(rule) for rule in subgraph.rules], 'params': dict(params)}
			)
			return response['params']
		else:
			raise NotImplementedError


def dump(paths, file):
	Dumper(file).dump(paths)

def dumps(paths):
	file = StringIO()
	Dumper(file).dump(paths)
	return file.getvalue()

def insert(file, graph_db, **params):
	return Loader(graph_db).insert(Subgraph(file), **params)

def inserts(str, graph_db, **params):
	file = StringIO(str)
	return Loader(graph_db).insert(Subgraph(file), **params)

def merge(file, graph_db, **params):
	return Loader(graph_db).merge(Subgraph(file), **params)

def merges(str, graph_db, **params):
	file = StringIO(str)
	return Loader(graph_db).merge(Subgraph(file), **params)

def delete(file, graph_db, **params):
	return Loader(graph_db).delete(Subgraph(file), **params)

def deletes(str, graph_db, **params):
	file = StringIO(str)
	return Loader(graph_db).delete(Subgraph(file), **params)


if __name__ == "__main__":

	def print_usage(cmd):
		sys.stdout.write("""\
Usage: {0} insert|merge|delete DATABASE [SUBGRAPH] [PARAM]...

{0} is a command line tool for loading subgraphs into a Neo4j graph database
using either insert, merge or delete methods.

DATABASE - Neo4j graph database URI (e.g. http://localhost:7474/db/data/)
SUBGRAPH - File containing subgraph data (delimited text or JSON)
PARAM    - Input parameter such as A=/node/123 or AB=/relationship/45
""".format(cmd))
		sys.exit()

	cmd = None
	method = None
	database = None
	subgraph = None
	params = {}
	for arg in sys.argv:
		if "=" in arg:
			key, eq, value = arg.partition("=")
			params[key] = value
		elif cmd is None:
			cmd = arg.rpartition("/")[2]
		elif method is None:
			method = arg
		elif database is None:
			database = neo4j.GraphDatabaseService(arg)
		elif subgraph is None:
			subgraph = open(arg, "r")
		else:
			print_usage(cmd)
	database = database or neo4j.GraphDatabaseService()
	subgraph = subgraph or sys.stdin
	if method == "insert":
		params = insert(subgraph, database, **params)
	elif method == "merge":
		params = merge(subgraph, database, **params)
	elif method == "delete":
		params = delete(subgraph, database, **params)
	else:
		print_usage(cmd)
	names = sorted(params.keys())
	width = max([len(name) for name in names]) + 1
	for name in names:
		print(string.ljust(name, width) + params[name])
