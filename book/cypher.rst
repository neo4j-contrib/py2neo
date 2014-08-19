======
Cypher
======

`Cypher <http://www.neo4j.org/learn/cypher>`_ is to Neo4j as SQL is to
relational databases. It is a declarative language that provides facilities
to query and manipulate data within a graph database using a syntax which is
both quick to learn and easy to read.

In py2neo, all Cypher functionality is accessible through the ``Graph.cypher``
attribute. To execute a single statement, simply use the ``execute`` method::

    >>> from py2neo import Graph
    >>> graph = Graph()
    >>> graph.cypher.execute("MERGE (a:Person {name:'Alice'}) RETURN a")
     a
    -----------------------------
     (n1:Person {name:"Alice"})
    (1 row)

Each statement executed in this way is carried out within a new database
transaction.


Parameters
==========

For most non-trivial statements, it is advisable to use parameters. This allows
the Neo4j Cypher engine to cache and reuse statements that vary only in their
parameters. Cypher automatically handles escaping and quoting for parameter values
and can therefore help to prevent injection attacks.

Note that parameter substitution applies chiefly to property values and that node
labels and relationship types may not be parameterised. (TODO: link to neo site)

To supply parameters, simply pass a second dictionary argument to the ``execute``
method::

    >>> graph.cypher.execute("MERGE (a:Person {name:{N}}) RETURN a", {"N": "Alice"})
     a
    -----------------------------
     (n1:Person {name:"Alice"})
    (1 row)


Transactions
============

Neo4j 2.0 introduced a facility to allow multiple statements to be carried out within
a single transaction. From py2neo's perspective, statements are collected within
the client application, then executed en masse on the server. This process may be
repeated several times for the same transaction until a final ``commit`` or
``rollback`` call is made whereupon the transaction is considered "finished".

To create a new transaction, use the ``begin`` method::

    from py2neo import Graph
    graph = Graph()

    # create template statements
    merge_person = "MERGE (a:Person {name:{N}}) RETURN a"
    join_people = ("MATCH (a:Person), (b:Person) "
                   "WHERE a.name = {N1} AND b.name = {N2} "
                   "CREATE UNIQUE (a)-[ab:KNOWS]->(b) "
                   "RETURN ab")

    tx = graph.cypher.begin()

    # send three statements to for execution but leave the transaction open
    tx.append(merge_person, {"N": "Alice"})
    tx.append(merge_person, {"N": "Bob"})
    tx.append(join_people, {"N1": "Alice", "N2": "Bob"})
    tx.execute()

    # send another three statements and commit the transaction
    tx.append(merge_person, {"N": "Carol"})
    tx.append(merge_person, {"N": "Dave"})
    tx.append(join_people, {"N1": "Carol", "N2": "Dave"})
    tx.commit()


API
===

.. autoclass:: py2neo.cypher.Transaction
    :members:

.. autoclass:: py2neo.cypher.Record
    :members:

.. autoexception:: py2neo.cypher.TransactionError
    :members:

.. autoexception:: py2neo.cypher.TransactionFinished
    :members:

.. autoclass:: py2neo.neo4j.CypherResults
    :members:

.. autoclass:: py2neo.neo4j.IterableCypherResults
    :members:
