**************************************
``py2neo.database`` -- Graph Databases
**************************************

.. module:: py2neo.database

The ``py2neo.database`` module contains classes and functions required to interact with a Neo4j server.
The most important of these is the :class:`.Graph` class which represents a Neo4j graph database instance and provides access to much of the available functionality.

To run a query against a local database is straightforward::

    >>> from py2neo import Graph
    >>> graph = Graph(host="localhost", http_port=7474, bolt_port=7687, user="neo4j", password="password")


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


The Database Management System
==============================

.. autoclass:: DBMS
   :members:
