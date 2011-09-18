#!/usr/bin/env python

"""
Script to load sample GEOFF file into database
"""

from py2neo import neo4j

# Attach to the graph db instance
gdb = neo4j.GraphDatabaseService("http://localhost:7474/db/data")

# Obtain a link to the reference node of the db
ref_node = gdb.get_reference_node()

# Load the royal family into the database
handle = geoff.load(file("royals.geoff"), gdb)
print "New data available from node %s" % (handle)

