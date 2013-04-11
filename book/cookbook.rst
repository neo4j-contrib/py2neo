Cookbook
========

If you want to jump in and start coding, the following short programme
illustrates a simple usage of the py2neo library::

    from py2neo import neo4j, cypher

    # attach to a local graph database service
    graph_db = neo4j.GraphDatabaseService("http://localhost:7474/db/data/")

    # create two nodes and a relationship between them
    #   (Alice)-[:KNOWS]->(Bob)
    alice, bob, ab = graph_db.create(
        node(name="Alice"), node(name="Bob"), rel(0, "KNOWS", 1)
    )

    # build a Cypher query and related parameters
    query = (
        "START a = node({A}) "
        "MATCH (a)-[:KNOWS]->(b) "
        "RETURN a, b"
    )
    params = {"A": node_a.id}

    # define a row handler
    def print_row(row):
        a, b = row
        print(a["name"] + " knows " + b["name"])

    # execute the query
    cypher.execute(graph_db, query, params, row_handler=print_row)

Batch Insertion using Index
---------------------------

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
