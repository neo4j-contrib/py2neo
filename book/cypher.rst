Cypher
======

`Cypher <http://www.neo4j.org/learn/cypher>`_ is to Neo4j as SQL is to
relational databases. It is a declarative language that provides facilities
to query and manipulate data within a graph database using a syntax which is
both quick to learn and easy to read.

.. note::
   As of py2neo 1.6, the `cypher` module has been deprecated. This module will
   likely be reintroduced at a later time in order to provide support for
   client transactions but in the meantime the new facilities provided by the
   `neo4j` module are recommended for individual and batch query use.


.. autoclass:: py2neo.neo4j.CypherQuery
    :members:

.. autoclass:: py2neo.neo4j.CypherResults
    :members:

.. autoclass:: py2neo.neo4j.IterableCypherResults
    :members:
