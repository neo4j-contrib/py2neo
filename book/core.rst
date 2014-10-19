========================
Core Functions & Classes
========================

The ``py2neo`` top-level package contains the functions and classes that are used directly and
ubiquitously across the whole library.


Service Wrappers
================

.. autoclass:: py2neo.Graph
   :members:

.. autoclass:: py2neo.SchemaResource
   :members:

.. autoclass:: py2neo.ServiceRoot
   :members:


Entities
========

.. autoclass:: py2neo.Node
   :members:

.. autoclass:: py2neo.Relationship
   :members:

.. autoclass:: py2neo.Rel
   :members:

.. autoclass:: py2neo.Rev
   :members:

.. autoclass:: py2neo.PropertyContainer
   :members:

.. autoclass:: py2neo.PropertySet
   :members:

.. autoclass:: py2neo.LabelSet
   :members:


Structures
==========

.. autoclass:: py2neo.Path
   :members:

.. autoclass:: py2neo.Subgraph
   :members:


Helpers
=======

.. autoclass:: py2neo.NodePointer
   :members:

.. autofunction:: py2neo.authenticate

.. autofunction:: py2neo.rewrite

.. autofunction:: py2neo.watch


Exceptions
==========

.. autoexception:: py2neo.BindError
   :members:

.. autoexception:: py2neo.Finished
   :members:

.. autoexception:: py2neo.GraphError
   :members:

.. autoexception:: py2neo.JoinError
   :members:
