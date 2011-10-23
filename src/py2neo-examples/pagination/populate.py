#!/usr/bin/env python

import sys
import inflect

from py2neo import neo4j

if len(sys.argv) < 2:
	sys.exit("Usage: %s <number_of_records>" % (sys.argv[0]))

# Hook into the database and create the inflection engine
gdb = neo4j.GraphDatabaseService("http://localhost:7474/db/data/")
root = gdb.get_subreference_node("NUMBERS")
eng = inflect.engine()

# In case we've run this before, purge any existing number records
nodes = root.get_related_nodes(neo4j.Direction.OUTGOING, "NUMBER")
rels = root.get_relationships(neo4j.Direction.OUTGOING, "NUMBER")
gdb.delete(*rels)
gdb.delete(*nodes)

# Create the nodes
count = int(sys.argv[1])
nodes = gdb.create_nodes(*[
	{
		"number": i,
		"name": eng.number_to_words(i)
	}
	for i in range(count)
])

# Connect the nodes to the subreference node
gdb.create_relationships(*[
	{
		"start_node": root,
		"end_node": node,
		"type": "NUMBER"
	}
	for node in nodes
])

