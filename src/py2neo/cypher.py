#!/usr/bin/env python

# Copyright 2011 Nigel Small
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

"""
Cypher utility module
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
import string
import sys


def _stringify(value, quoted=False, with_properties=False):
	if isinstance(value, dict):
		if 'type' in value:
			# relationship
			out = "({0})-[{1}:{2}]->({3})".format(
				value['start'].rpartition("/")[2],
				int(value['self'].rpartition("/")[2]),
				value['type'],
				value['end'].rpartition("/")[2]
			)
		else:
			# node
			out = "({0})".format(
				int(value['self'].rpartition("/")[2])
			)
		if quoted:
			out = '"' + out + '"'
		if with_properties:
			out += " " + json.dumps(value['data'], separators=(',',':'))
	else:
		# property
		if quoted:
			out = json.dumps(value)
		else:
			out = str(value)
	return out

def execute(query, graph_db):
	"""
	Execute a Cypher query against a database and return a tuple of data
	and columns.
	"""
	if graph_db._cypher_uri is None:
		raise NotImplementedError("Cypher functionality not available")
	else:
		response = graph_db._post(graph_db._cypher_uri, {'query': query})
		return response['data'], response['columns']

def execute_and_output_as_delimited(query, graph_db, field_delimiter="\t", out=None):
	out = out or sys.stdout
	data, columns = execute(query, graph_db)
	out.write(string.join([
		json.dumps(column)
		for column in columns
	], field_delimiter))
	out.write("\n")
	for row in data:
		out.write(string.join([
			_stringify(value, quoted=True)
			for value in row
		], field_delimiter))
		out.write("\n")

def execute_and_output_as_json(query, graph_db, out=None):
	out = out or sys.stdout
	data, columns = execute(query, graph_db)
	columns = [json.dumps(column) for column in columns]
	row_count = 0
	out.write("[\n")
	for row in data:
		row_count += 1
		if row_count > 1:
			out.write(",\n")
		out.write("\t{" + string.join([
			columns[i] + ": " + _stringify(row[i], quoted=True)
			for i in range(len(row))
		], ", ") + "}")
	out.write("\n]\n")

def execute_and_output_as_geoff(query, graph_db, out=None):
	out = out or sys.stdout
	nodes = {}
	relationships = {}
	def update_descriptors(value):
		if isinstance(value, dict):
			if 'type' in value:
				# relationship
				relationships["({0})-[{1}:{2}]->({3})".format(
					value['start'].rpartition("/")[2],
					int(value['self'].rpartition("/")[2]),
					value['type'],
					value['end'].rpartition("/")[2]
				)] = value['data']
			else:
				# node
				nodes["({0})".format(
					int(value['self'].rpartition("/")[2])
				)] = value['data']
		else:
			# property - not supported in GEOFF format, so ignore
			pass
	data, columns = execute(query, graph_db)
	for row in data:
		for i in range(len(row)):
			update_descriptors(row[i])
	for key, value in nodes.items():
		out.write("{0}\t{1}\n".format(
			key,
			json.dumps(value)
		))
	for key, value in relationships.items():
		out.write("{0}\t{1}\n".format(
			key,
			json.dumps(value)
		))

def execute_and_output_as_text(query, graph_db, out=None):
	out = out or sys.stdout
	data, columns = execute(query, graph_db)
	column_widths = [len(column) for column in columns]
	for row in data:
		column_widths = [
			max(column_widths[i], None if row[i] is None else len(_stringify(row[i], with_properties=True)))
			for i in range(len(row))
		]
	out.write("+-" + string.join([
		"".ljust(column_widths[i], "-")
		for i in range(len(columns))
	], "---") + "-+\n")
	out.write("| " + string.join([
		columns[i].ljust(column_widths[i])
		for i in range(len(columns))
	], " | ") + " |\n")
	out.write("+-" + string.join([
		"".ljust(column_widths[i], "-")
		for i in range(len(columns))
	], "---") + "-+\n")
	for row in data:
		out.write("| " + string.join([
			_stringify(row[i], with_properties=True).ljust(column_widths[i])
			for i in range(len(row))
		], " | ") + " |\n")
	out.write("+-" + string.join([
		"".ljust(column_widths[i], "-")
		for i in range(len(columns))
	], "---") + "-+\n")

if __name__ == "__main__":
	parser = argparse.ArgumentParser(description="Execute Cypher queries against a Neo4j database server and output the results.")
	parser.add_argument("-u", metavar="DATABASE_URI", default=None, help="the URI of the source Neo4j database server")
	parser.add_argument("-d", action="store_true", default=False, help="output all values in delimited format")
	parser.add_argument("-g", action="store_true", default=False, help="output nodes and relationships in GEOFF format")
	parser.add_argument("-j", action="store_true", default=False, help="output all values as a single JSON array")
	parser.add_argument("-t", action="store_true", default=True, help="output all results in a plain text table (default)")
	parser.add_argument("query", help="the Cypher query to execute")
	args = parser.parse_args()
	try:
		graph_db = neo4j.GraphDatabaseService(args.u or "http://localhost:7474/db/data/")
		if args.g:
			execute_and_output_as_geoff(args.query, graph_db)
		elif args.j:
			execute_and_output_as_json(args.query, graph_db)
		elif args.t:
			execute_and_output_as_text(args.query, graph_db)
		else:
			execute_and_output_as_delimited(args.query, graph_db)
	except SystemError as err:
		content = err.args[0]['content']
		if 'exception' in content and 'stacktrace' in content:
			sys.stderr.write("{0}\n".format(content['exception']))
			stacktrace = content['stacktrace']
			for frame in stacktrace:
				sys.stderr.write("\tat {0}\n".format(frame))
		else:
			sys.stderr.write("{0}\n".format(content))

