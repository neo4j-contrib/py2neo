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


import argparse
try:
	import json
except ImportError:
	import simplejson as json
import neo4j
import string
import sys


__author__    = "Nigel Small <py2neo@nigelsmall.org>"
__copyright__ = "Copyright 2011 Nigel Small"
__license__   = "Apache License, Version 2.0"


def execute(query, database_uri=None):
	database_uri = database_uri or "http://localhost:7474/db/data/"
	graphdb = neo4j.GraphDatabaseService(database_uri)
	response = graphdb.execute_cypher_query(query)
	data, columns = response['data'], response['columns']
	return data, columns

def output(value):
	if isinstance(value, dict):
		if 'type' in value:
			# relationship
			out = "\"(%s)-[%d:%s]->(%s)\"" % (
				value['start'].rpartition("/")[2],
				int(value['self'].rpartition("/")[2]),
				value['type'],
				value['end'].rpartition("/")[2]
			)
		else:
			# node
			out = "\"(%d)\"" % (
				int(value['self'].rpartition("/")[2])
			)
	else:
		# property
		out = json.dumps(value)
	return out

def output_text(value):
	if isinstance(value, dict):
		if 'type' in value:
			# relationship
			out = "(%s)-[%d:%s]->(%s) %s" % (
				value['start'].rpartition("/")[2],
				int(value['self'].rpartition("/")[2]),
				value['type'],
				value['end'].rpartition("/")[2],
				json.dumps(value['data'], separators=(',',':'))
			)
		else:
			# node
			out = "(%d) %s" % (
				int(value['self'].rpartition("/")[2]),
				json.dumps(value['data'], separators=(',',':'))
			)
	else:
		# property
		out = str(value)
	return out

def execute_and_output_as_delimited(query, database_uri=None, field_delimiter="\t"):
	data, columns = execute(query, database_uri)
	print string.join([
		json.dumps(column)
		for column in columns
	], field_delimiter)
	for row in data:
		print string.join([
			output(value)
			for value in row
		], field_delimiter)

def execute_and_output_as_json(query, database_uri=None):
	data, columns = execute(query, database_uri)
	columns = [json.dumps(column) for column in columns]
	row_count = 0
	sys.stdout.write("[\n")
	for row in data:
		row_count += 1
		if row_count > 1:
			sys.stdout.write(",\n")
		sys.stdout.write("\t{" + string.join([
			columns[i] + ": " + output(row[i])
			for i in range(len(row))
		], ", ") + "}")
	sys.stdout.write("\n]\n")

def execute_and_output_as_geoff(query, database_uri=None):
	nodes = {}
	rels = {}
	def update_descriptors(value, column):
		if isinstance(value, dict):
			if 'type' in value:
				# relationship
				rels["(%s)-[%d:%s]->(%s)" % (
					value['start'].rpartition("/")[2],
					int(value['self'].rpartition("/")[2]),
					value['type'],
					value['end'].rpartition("/")[2]
				)] = value['data']
			else:
				# node
				nodes["(%d)" % (
					int(value['self'].rpartition("/")[2])
				)] = value['data']
		else:
			# property - not supported in GEOFF format
			pass
	data, columns = execute(query, database_uri)
	for row in data:
		for i in range(len(row)):
			update_descriptors(row[i], columns[i])
	for key, value in nodes.items():
		print "%s\t%s" % (
			key,
			json.dumps(value)
		)
	for key, value in rels.items():
		print "%s\t%s" % (
			key,
			json.dumps(value)
		)

def execute_and_output_as_text(query, database_uri=None, field_delimiter="\t"):
	data, columns = execute(query, database_uri)
	column_widths = [len(column) for column in columns]
	for row in data:
		column_widths = [
			max(column_widths[i], None if row[i] is None else len(output_text(row[i])))
			for i in range(len(row))
		]
	print "+-" + string.join([
		"".ljust(column_widths[i], "-")
		for i in range(len(columns))
	], "---") + "-+"
	print "| " + string.join([
		columns[i].ljust(column_widths[i])
		for i in range(len(columns))
	], " | ") + " |"
	print "+-" + string.join([
		"".ljust(column_widths[i], "-")
		for i in range(len(columns))
	], "---") + "-+"
	for row in data:
		print "| " + string.join([
			output_text(row[i]).ljust(column_widths[i])
			for i in range(len(row))
		], " | ") + " |"
	print "+-" + string.join([
		"".ljust(column_widths[i], "-")
		for i in range(len(columns))
	], "---") + "-+"

if __name__ == "__main__":
	parser = argparse.ArgumentParser(description="Execute Cypher queries against a Neo4j database server and output the results.")
	parser.add_argument("-u", metavar="DATABASE_URI", default=None, help="the URI of the source Neo4j database server")
	parser.add_argument("-d", action="store_true", default=True, help="output all values in delimited format (default)")
	parser.add_argument("-j", action="store_true", default=False, help="output all values as a single JSON array")
	parser.add_argument("-g", action="store_true", default=False, help="output nodes and relationships in GEOFF format")
	parser.add_argument("-t", action="store_true", default=False, help="output all results in a plain text table")
	parser.add_argument("query", help="the Cypher query to execute")
	args = parser.parse_args()
	try:
		if args.g:
			execute_and_output_as_geoff(args.query, args.u)
		elif args.j:
			execute_and_output_as_json(args.query, args.u)
		elif args.t:
			execute_and_output_as_text(args.query, args.u)
		else:
			execute_and_output_as_delimited(args.query, args.u)
	except SystemError as err:
		content = err.args[0]['content']
		if 'exception' in content and 'stacktrace' in content:
			sys.stderr.write("%s\n" % (content['exception']))
			stacktrace = content['stacktrace']
			for frame in stacktrace:
				sys.stderr.write("\tat %s\n" % (frame))
		else:
			sys.stderr.write("%s\n" % (content))

