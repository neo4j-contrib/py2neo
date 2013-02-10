py2neo
======

Py2neo is a simple and pragmatic Python library that provides access to the
popular graph database Neo4j via its RESTful web service interface. With no
external dependencies, installation is straightforward and getting started
with coding is easy. The library is actively maintained on GitHub, regularly
updated in the Python Package Index and is built uniquely for Neo4j in close
association with its team and community.

In addition, the library provides support for the Graph Export Object File
Format (Geoff). For further information on Geoff, visit
[[http://nigelsmall.com/geoff]].

Requirements
------------

You will need to be running version 1.8.1 or above of Neo4j (available from
http://neo4j.org/) in order to use the full feature set of py2neo. Reduced
capabilities will be available for server versions 1.6 and 1.7.

Your Python version should be at least 2.6 although 2.7 is recommended. Py2neo
is also fully compatible with Python 3. Partial support exists for Jython - if
you are using this then please help to improve support by providing feedback.

Package Contents
----------------

.. toctree::
   :maxdepth: 4

   neo4j
   cypher
   ogm
   geoff
   gremlin
   calendar
   admin
   rest

Quick Start
-----------

If you want to jump in and start coding, the following short programme
illustrates a simple usage of the py2neo library::

    #!/usr/bin/env python
     
    """
    Simple example showing node and relationship creation plus
    execution of Cypher queries
    """
     
    from __future__ import print_function
     
    # Import Neo4j modules
    from py2neo import neo4j, cypher
     
    # Attach to the graph db instance
    graph_db = neo4j.GraphDatabaseService("http://localhost:7474/db/data/")
     
    # Create two nodes
    node_a, node_b = graph_db.create(
        {"name": "Alice"},
        {"name": "Bob"}
    )
     
    # Join the nodes with a relationship
    rel_ab = node_a.create_relationship_to(node_b, "KNOWS")
     
    # Build a Cypher query
    query = "START a=node({A}) MATCH a-[:KNOWS]->b RETURN a,b"
     
    # Define a row handler...
    def print_row(row):
        a, b = row
        print(a["name"] + " knows " + b["name"])
     
    # ...and execute the query
    cypher.execute(graph_db, query, {"A": node_a.id}, row_handler=print_row)

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

