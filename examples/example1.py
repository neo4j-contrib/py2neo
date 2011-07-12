#!/usr/bin/env python

import sys
sys.path.append("../src") 

from py2neo import neo4j

gdb = neo4j.GraphDatabaseService("http://localhost:7474/db/data")
ref_node = gdb.get_reference_node()
traverser = ref_node.get_traverser()
traverser.set_max_depth(3)
for path in traverser.traverse_depth_first():
	print path

