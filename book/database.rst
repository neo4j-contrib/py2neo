**************************************
``py2neo.database`` -- Graph Databases
**************************************

.. module:: py2neo.database

The ``py2neo.database`` package contains classes and functions required to interact with a Neo4j server.
The most important of these is the :class:`.Graph` class which represents a Neo4j graph database instance and provides access to much of the available functionality.

To run a query against a local database is straightforward::

    >>> from py2neo import Graph
    >>> graph = Graph(password="password")
    >>> graph.run("UNWIND range(1, 10) AS n RETURN n, n * n as n_sq").dump()
    n   n_sq
   ----------
     1     1
     2     4
     3     9
     4    16
     5    25
     6    36
     7    49
     8    64
     9    81
    10   100

.. note::
    The previous version of py2neo allowed Cypher execution through :meth:`Graph.cypher.execute`.
    This facility is now instead accessible via :meth:`.Graph.run` and returns a lazily-evaluated :class:`.Cursor` rather than an eagerly-evaluated :class:`RecordList`.

The Graph
=========

.. autoclass:: Graph(*uris, **settings)
   :members:

.. autoclass:: Schema
   :members:


Transactions
============

.. autoclass:: Transaction(autocommit=False)
   :members:


Cursors
=======

.. autoclass:: Cursor
   :members:


The DBMS
========

.. autoclass:: DBMS
   :members:


Security
========

.. automodule:: py2neo.database.auth
   :members:


Errors & Warnings
=================

.. automodule:: py2neo.database.status
   :members:
