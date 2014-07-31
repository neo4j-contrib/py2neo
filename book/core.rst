============
Core Objects
============

A handful of core classes lay at the heart of py2neo and are essential for almost every
application. Instances of these classes will often be constructed explicitly.


The Graph Object
================

The **Graph** class (named **GraphDatabaseService** in earlier versions of py2neo) provides a link
to a local or remote Neo4j database service.

.. autoclass:: py2neo.Graph
   :members:


Property Containers
===================

.. autoclass:: py2neo.PropertyContainer
   :members:
.. autoclass:: py2neo.Node
   :members:
.. autoclass:: py2neo.Rel
   :members:
.. autoclass:: py2neo.Rev
   :members:
.. autoclass:: py2neo.LabelSet
   :members:
.. autoclass:: py2neo.PropertySet
   :members:


Paths & Relationships
=====================

.. autoclass:: py2neo.Path
   :members:
.. autoclass:: py2neo.Relationship
   :members:


Errors
======

TODO
