========================
Core Functions & Classes
========================

The ``py2neo`` top-level package contains the functions and classes that are used directly and
ubiquitously across the whole library.


Service Wrappers
================

.. autoclass:: py2neo.Graph

.. autoclass:: py2neo.Schema

.. autoclass:: py2neo.ServiceRoot


Entities
========

.. autoclass:: py2neo.Node

.. autoclass:: py2neo.Relationship

.. autoclass:: py2neo.Rel

.. autoclass:: py2neo.Rev

.. autoclass:: py2neo.PropertyContainer

.. autoclass:: py2neo.PropertySet

.. autoclass:: py2neo.LabelSet


Structures
==========

.. autoclass:: py2neo.Path

.. autoclass:: py2neo.Subgraph


Helpers
=======

.. autoclass:: py2neo.NodePointer

.. autofunction:: py2neo.authenticate

.. autofunction:: py2neo.rewrite

.. autofunction:: py2neo.watch


Exceptions
==========

.. autoexception:: py2neo.BindError

.. autoexception:: py2neo.Finished

.. autoexception:: py2neo.GraphError

.. autoexception:: py2neo.JoinError
