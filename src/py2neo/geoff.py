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
Geoff file handling (see `<http://py2neo.org/geoff/>`_).
"""

__author__    = "Nigel Small <py2neo@nigelsmall.org>"
__copyright__ = "Copyright 2011 Nigel Small"
__license__   = "Apache License, Version 2.0"


import argparse
try:
	import json
except ImportError:
	import simplejson as json
try:
	from . import neo4j
except ValueError:
	import neo4j
import sys


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

	def load(self, file, **params):
		if self.graph_db._geoff_uri is None:
			raise NotImplementedError
		else:
			response = graph_db._post(
				graph_db._geoff_uri,
				{'rules': file.read(), 'params': dict(params)}
			)
			return response['params']

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

def load(file, graph_db, **params):
	return Loader(graph_db).load(file, **params)

def loads(str, graph_db, **params):
	file = StringIO(str)
	return Loader(graph_db).load(file, **params)


if __name__ == "__main__":
	parser = argparse.ArgumentParser(description="""\
Import graph data from a GEOFF file into a Neo4j database. A source file may be
specified with the -f option and a destination database with the -u option. The
remainder of the arguments will be passed as hooks into the load routine. Each
hook may be a node of relationship and can optionally be named. Unnamed hooks
will be automatically named by their relative zero-based position. For example,
"foo=/node/123" designates the node with ID 123, named as "foo", whereas
"/relationship/456" designates the relationship with ID 456 and will be named
"0" if it is in the first position, "1" for the second, and so on.
EXAMPLE: geoff.py -f foo.geoff bar=/node/123 baz=/relationship/456
""")
	parser.add_argument("-u", metavar="DATABASE_URI", default=None, help="the URI of the destination Neo4j database server")
	parser.add_argument("-f", metavar="SOURCE_FILE", help="the GEOFF file to load")
	parser.add_argument("uri", metavar="name=uri", nargs="*", help="named relative entity URI (e.g. foo=/node/123)")
	args = parser.parse_args()
	# Attempt to open source file
	try:
		if args.f:
			source_file = open(args.f, "r")
		else:
			source_file = sys.stdin
	except Exception:
		sys.stderr.write("Failed to open GEOFF file\n")
		sys.exit(1)
	# Attempt to open destination database
	try:
		graph_db = neo4j.GraphDatabaseService(args.u or "http://localhost:7474/db/data/")
	except Exception:
		sys.stderr.write("Failed to open destination database\n")
		sys.exit(1)
	# Parse load parameters
	params = {}
	for i in range(len(args.uri)):
		key, eq, value = args.uri[i].rpartition("=")
		key = key or str(i)
		if graph_db._geoff_uri is None:
			if value.startswith("/node/"):
				params[key] = neo4j.Node(graph_db._base_uri + value)
			elif value.startswith("/relationship/"):
				params[key] = neo4j.Relationship(graph_db._base_uri + value)
			else:
				sys.stderr.write("Bad relative entity URI: {0}\n".format(value))
				sys.exit(1)
		else:
			params[key] = value
	# Perform the load
	try:
		if graph_db._geoff_uri is None:
			load(source_file, graph_db, **params)
		else:
			params = load(source_file, graph_db, **params)
			column_width_1 = max([len(key) for key in params.keys()]) + 1
			for key, value in load(source_file, graph_db, **params).items():
				print(key.ljust(column_width_1) + value)
	except Exception as e:
		sys.stderr.write(str(e) + "\n")
		sys.exit(1)
