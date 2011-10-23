#!/usr/bin/env python

"""
Example showing persistent objects
"""

from py2neo import neo4j

gdb = neo4j.GraphDatabaseService("http://localhost:7474/db/data")

# Define a few subclasses of PersistentObject

class Continent(neo4j.PersistentObject):

	def __init__(self, node, name):
		neo4j.PersistentObject.__init__(self, node)
		self.name = name

class Country(neo4j.PersistentObject):

	def __init__(self, node, name, population):
		neo4j.PersistentObject.__init__(self, node)
		self.name = name
		self.population = population
		self.currency = None
		self.continent = None

class Currency(neo4j.PersistentObject):

	def __init__(self, node, name):
		neo4j.PersistentObject.__init__(self, node)
		self.name = name
		self.currency = None

# Populate some objects, each attached to new nodes

europe = Continent(gdb.create_node(), "Europe")

uk = Country(gdb.create_node(), "United Kingdom", 62698362)
fr = Country(gdb.create_node(), "France", 65312249)
de = Country(gdb.create_node(), "Germany", 81471834)

gbp = Currency(gdb.create_node(), "Sterling")
eur = Currency(gdb.create_node(), "Euro")

# Update property values

uk.currency = gbp
fr.currency = eur
de.currency = eur

uk.continent = europe
fr.continent = europe
de.continent = europe

# Show the node detail

print "Europe can be found at node %s" % (europe.__node__)

