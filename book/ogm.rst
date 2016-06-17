**************************************
``py2neo.ogm`` -- Object-Graph Mapping
**************************************

.. module:: py2neo.ogm

The ``py2neo.ogm`` package contains a set of facilities for binding Python objects to an underlying set of graph data.
Class definitions extend :class:`.GraphObject` and include :class:`.Property` and :class:`.Label` definitions as well as details of :class:`.Related` objects.

A simple example, based on the movie graph data set::

   class Movie(GraphObject):
       __primarykey__ = "title"

       title = Property()
       tag_line = Property(key="tagline")
       released = Property()

       actors = RelatedFrom("Person", "ACTED_IN")
       directors = RelatedFrom("Person", "DIRECTED")
       producers = RelatedFrom("Person", "PRODUCED")


   class Person(GraphObject):
       __primarykey__ = "name"

       name = Property()
       born = Property()

       acted_in = RelatedTo(Movie)
       directed = RelatedTo(Movie)
       produced = RelatedTo(Movie)


Class Definition
================

.. autoclass:: py2neo.ogm.GraphObject
   :members:

.. autoclass:: py2neo.ogm.Property
   :members:

.. autoclass:: py2neo.ogm.Label
   :members:

.. autoclass:: py2neo.ogm.RelatedTo
   :members:

.. autoclass:: py2neo.ogm.RelatedFrom
   :members:


Related Objects
===============

.. autoclass:: py2neo.ogm.RelatedObjects
   :members:


Graph Object Selection
======================

.. autoclass:: py2neo.ogm.GraphObjectSelection
   :members:
