#!/usr/bin/env python

import sys

from py2neo import neo4j

if len(sys.argv) < 2:
	sys.exit("Usage: %s <page_number>" % (sys.argv[0]))

# Hook into the database
gdb = neo4j.GraphDatabaseService("http://localhost:7474/db/data/")
root = gdb.get_subreference_node("NUMBERS")

# Set up pagination variables
page_number = int(sys.argv[1])
page_size = 10

# Build the cypher query
query = """\
start x=%s
match (x)-[:NUMBER]->(n)
return n
order by n.name
skip %d
limit %d
""" % (
	str(root),
	page_size * (page_number - 1),
	page_size
)

# Grab the results, iterate and print
results = gdb.execute_cypher_query(query)
for result in results["data"]:
	print result[0]["data"]["name"]

