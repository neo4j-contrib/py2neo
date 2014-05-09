========
Cookbook
========

Creating a Simple Graph
=======================

Listed below are a couple of short examples on how to create a simple graph.

Using the Regular REST API
--------------------------

::

    from py2neo import neo4j

    graph_db = neo4j.Graph()
    a, b, ab = graph_db.create(node(name="Alice"), node(name="Bob"), rel(0, "KNOWS", 1))

Using Cypher
------------

::

    from py2neo import neo4j

    graph_db = neo4j.Graph()
    query = neo4j.CypherQuery(db, "CREATE (a {name:{name_a}})-[ab:KNOWS]->(b {name:{name_b}})"
                                  "RETURN a, b, ab")
    a, b, ab = query.execute(name_a="Alice", name_b="Bob").data[0]

Batch Insertion using Index
===========================

::

    # some example source data
    records = [(101, "Alice"), (102, "Bob"), (103, "Carol")]

    graph_db = neo4j.Graph("http://localhost:7474/db/data/")
    batch = neo4j.WriteBatch(graph_db)  # batch is linked to graph database

    for emp_no, name in records:
        # get_or_create_indexes_node is one of many batch methods available
        batch.get_or_create_indexed_node("Employees", "emp_no", emp_no, {
            "emp_no": emp_no, "name": name
        })

    nodes = batch.submit()  # will return `Node` objects for the nodes created

Default URI
===========

.. warning:: The value of the `DEFAULT_URI` has changed in version 1.6. This now
    points to the default service root (http://localhost:7474) *not* the default
    graph database service (http://localhost:7474/db/data/).

A default Neo4j instance will listen on port 7474. Therefore, for such a
default installation, the ``DEFAULT_URI`` can be used:

.. autoattribute:: py2neo.neo4j.DEFAULT_URI

This default will be used if the URI is omitted from construction of a new
``ServiceRoot`` instance::

    service_root = neo4j.ServiceRoot()

If the URI is omitted from a ``Graph`` constructor, the default
will be discovered via the default service root. Therefore this only uses the
`DEFAULT_URI` indirectly.

Authentication
==============

.. autofunction:: py2neo.neo4j.authenticate

As of py2neo 1.6.1, authentication can also be supplied through any resource
URI, e.g.::

    from py2neo import neo4j
    graph_db = neo4j.Graph("http://arthur:excalibur@localhost:7474/db/data/")
    
.. warning:: Implicit authentication such as this applies not just to the
    resource specified but instead applies globally to any resources with the
    same host and port used thereafter.

URI Rewriting
=============

.. autofunction:: py2neo.neo4j.rewrite

Logging
=======

To enable logging, simply import the ``logging`` module and provide a
configuration, e.g.::

    import logging
    logging.basicConfig(level=logging.DEBUG)

