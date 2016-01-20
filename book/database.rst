****************
Database Servers
****************

.. module:: py2neo.database

The Graph
=========

Each Neo4j server installation can be represented by an instance of the :class:`.Graph` class. To
get connected, the full REST URI of the server should be supplied to the Graph constructor. Note
that this includes the ``/db/data/`` path and a trailing slash.

    >>> from py2neo import Graph
    >>> graph = Graph("http://myserver:7474/db/data/")

If the Neo4j server is Bolt-enabled, this will be automatically detected and Bolt used in
preference to HTTP.

Authentication information can also be supplied to a graph URI::

    >>> secure_graph = Graph("http://arthur:excalibur@camelot:1138/db/data/")

Once created, a Graph instance provides direct or indirect access to most of the functionality
available within py2neo.

.. autoclass:: Graph
   :members:

.. class:: Schema

.. class:: Transaction

.. class:: Cursor

.. class:: DBMS
