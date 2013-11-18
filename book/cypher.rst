Cypher
======

`Cypher <http://www.neo4j.org/learn/cypher>`_ is to Neo4j as SQL is to
relational databases. It is a declarative language that provides facilities
to query and manipulate data within a graph database using a syntax which is
both quick to learn and easy to read.

.. note::
   In py2neo 1.6.0, execution functions in the `cypher` module were deprecated
   and replaced by the :py:class:`CypherQuery <py2neo.neo4j.CypherQuery>`
   class from the `neo4j` module. From version 1.6.1 onwards, the entire
   `cypher` module has been repurposed and now contains new support for
   `Cypher transactions <http://docs.neo4j.org/chunked/milestone/rest-api-transactional.html>`_,
   as introduced in Neo4j 2.0.


Cypher Transactions
-------------------

Cypher transactions were introduced in Neo4j 2.0 and allow multiple statements
to be executed within a single server transaction.

::

    from py2neo import cypher

    session = cypher.Session("http://localhost:7474")
    tx = session.create_transaction()

    # send three statements to for execution but leave the transaction open
    tx.append("MERGE (a:Person {name:'Alice'})")
    tx.append("MERGE (b:Person {name:'Bob'})")
    tx.append("CREATE UNIQUE (a)-[:KNOWS]->(b)")
    tx.execute()

    # send another three statements and commit the transaction
    tx.append("MERGE (c:Person {name:'Carol'})")
    tx.append("MERGE (d:Person {name:'Dave'})")
    tx.append("CREATE UNIQUE (c)-[:KNOWS]->(d)")
    tx.commit()


.. autoclass:: py2neo.cypher.Session
    :members:

.. autoclass:: py2neo.cypher.Transaction
    :members:

.. autoclass:: py2neo.cypher.Record
    :members:

.. autoexception:: py2neo.cypher.TransactionError
    :members:

.. autoexception:: py2neo.cypher.TransactionFinished
    :members:


Classic Cypher Execution
------------------------

.. autoclass:: py2neo.neo4j.CypherQuery
    :members:

.. autoclass:: py2neo.neo4j.CypherResults
    :members:

.. autoclass:: py2neo.neo4j.IterableCypherResults
    :members:
