**************************************
``py2neo.database`` -- Graph Databases
**************************************

.. module:: py2neo.database

The ``py2neo.database`` package contains classes and functions required to interact with a Neo4j server.
For convenience, many of these classes are also exposed through the top-level package, ``py2neo``.

The most useful of the classes provided here is the :class:`.Graph` class which represents a Neo4j graph database instance and provides access to a large portion of the most commonly used py2neo API.

To run a query against a local database is straightforward::

    >>> from py2neo import Graph
    >>> graph = Graph(password="password")
    >>> graph.run("UNWIND range(1, 3) AS n RETURN n, n * n as n_sq").to_table()
       n | n_sq
    -----|------
       1 |    1
       2 |    4
       3 |    9


The :class:`.GraphService`
==========================

.. autoclass:: GraphService
   :members:


The :class:`.Graph`
===================

.. autoclass:: Graph(uri, name=None, **settings)
   :members:

.. autoclass:: Schema
   :members:

The :class:`.SystemGraph`
-------------------------

.. autoclass:: SystemGraph(uri, **settings)
   :members:


Transactions
============

.. autoclass:: GraphTransaction(autocommit=False)
   :members:


Cypher Results
==============

.. autoclass:: Cursor
   :members:

.. autoclass:: CypherStats
   :members:


Errors & Warnings
=================

.. autoclass:: GraphError
   :members:

.. autoclass:: ClientError
   :members:

.. autoclass:: DatabaseError
   :members:

.. autoclass:: TransientError
   :members:

.. autoclass:: GraphTransactionError
   :members:
