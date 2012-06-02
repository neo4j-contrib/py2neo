py2neo
======

Py2neo provides a clean and simple interface from Python to Neo4j via its
REST API.

Requirements
------------

Py2neo has been built against the following software:

* Python 2.6+ <http://python.org/>
* Tornado 2.2.1 <http://www.tornadoweb.org/>
* Neo4j 1.6+ <http://neo4j.org/>

Earlier versions of these may work but are not guaranteed to do so.

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

Package Contents
----------------

.. toctree::
   :maxdepth: 4

   neo4j
   cypher
   geoff
   gremlin
   rest


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

