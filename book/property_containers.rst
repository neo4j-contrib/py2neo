===================
Property Containers
===================

Py2neo provides two (and a bit) classes that allow storage of *key:value*
pairs: :class:`.Node` and :class:`.Rel` (as well as child class :class:`.Rev`).
These classes extend a base class named :class:`.PropertyContainer` that
implements a *properties* attibute of type :class:`.PropertySet`. The
:class:`.Node` class also provides a similar *labels*  attibute of type
:class:`.LabelSet`.

.. autoclass:: py2neo.PropertyContainer
   :members:


Nodes
=====

.. autoclass:: py2neo.Node
   :members:


Rels & Revs
===========

.. autoclass:: py2neo.Rel
   :members:
.. autoclass:: py2neo.Rev
   :members:


PropertySet & LabelSet
======================

.. autoclass:: py2neo.PropertySet
   :members:
.. autoclass:: py2neo.LabelSet
   :members:

