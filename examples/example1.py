#!/usr/bin/env python

"""
Simple first example showing connection and traversal
"""

# Allow the import path to access neo4j modules
import sys
sys.path.append("../src")

from py2neo import neo4j

# Attach to the graph db instance
gdb = neo4j.GraphDatabaseService("http://localhost:7474/db/data")

# Obtain a link to the reference node of the db
ref_node = gdb.get_reference_node()

# Obtain a traverser instance relative to reference node
traverser = ref_node.traverse(order="depth_first", max_depth=2)

# Output all the paths from this traversal
for path in traverser.paths:
	print path

