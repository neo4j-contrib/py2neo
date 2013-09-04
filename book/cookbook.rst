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

    graph_db = neo4j.GraphDatabaseService()
    a, b, ab = graph_db.create(node(name="Alice"), node(name="Bob"), rel(0, "KNOWS", 1))

Using Cypher
------------

::

    from py2neo import neo4j

    graph_db = neo4j.GraphDatabaseService()
    query = neo4j.CypherQuery(db, "CREATE (a {name:{name_a}})-[ab:KNOWS]->(b {name:{name_b}})"
                                  "RETURN a, b, ab")
    a, b, ab = query.execute(name_a="Alice", name_b="Bob").data[0]

Batch Insertion using Index
===========================

::

    # some example source data
    records = [(101, "Alice"), (102, "Bob"), (103, "Carol")]

    graph_db = neo4j.GraphDatabaseService("http://localhost:7474/db/data/")
    batch = neo4j.WriteBatch(graph_db)  # batch is linked to graph database

    for emp_no, name in records:
        # get_or_create_indexes_node is one of many batch methods available
        batch.get_or_create_indexed_node("Employees", "emp_no", emp_no, {
            "emp_no": emp_no, "name": name
        })

    nodes = batch.submit()  # will return `Node` objects for the nodes created

Default URI
-----------

A default Neo4j instance will listen on port 7474. Therefore, for such a
default installation, the ``DEFAULT_URI`` can be used:

.. autoattribute:: py2neo.neo4j.DEFAULT_URI

This default will be used if the URI is omitted from construction of a new
``GraphDatabaseService`` instance::

    graph_db = neo4j.GraphDatabaseService()

Authentication
--------------

.. autofunction:: py2neo.neo4j.authenticate

URI Rewriting
-------------

.. autofunction:: py2neo.neo4j.rewrite
