===================
API: The Essentials
===================

The ``py2neo`` top-level package contains the functions and classes that are used directly and
ubiquitously across the whole library.


The Graph
=========

.. autoclass:: py2neo.Graph
   :members:
   :inherited-members:


Authentication
==============

.. autofunction:: py2neo.set_auth_token


Nodes
=====

.. autoclass:: py2neo.Node
   :members:
   :inherited-members:


Relationships
=============

.. autoclass:: py2neo.Relationship
   :members:
   :inherited-members:

.. autoclass:: py2neo.Rel
   :members:
   :inherited-members:

.. autoclass:: py2neo.Rev
   :members:
   :inherited-members:


Paths
=====

.. autoclass:: py2neo.Path
   :members:
   :inherited-members:


Labels & Properties
===================

.. autoclass:: py2neo.LabelSet
   :members:

.. autoclass:: py2neo.PropertySet
   :members:


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
