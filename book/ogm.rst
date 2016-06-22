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


Graph Objects
=============

At the heart of the py2neo OGM framework is the :class:`.GraphObject`.
This is a base class for all classes that are to be mapped onto the graph database.
Each `GraphObject` represents a node plus a set of pointers to :class:`.RelatedObjects` as well as the relationship details that connect them.

A `GraphObject` instance may be constructed just like any other Python object but can also be `selected <#py2neo.ogm.GraphObject.select>`_ from the database.
Each instance may contain attributes that represent labels, nodes or related objects.

.. autoclass:: py2neo.ogm.GraphObject
   :members:

TODO: push, pull, create, etc

Primary Labels, Keys and Values
-------------------------------

A `GraphObject` instance may define `__primarylabel__` and `__primarykey__` attributes.
These are strings representing the main label associated with the class and the name of a uniquely identifying property for that class.
If undefined, `__primarylabel__` defaults to the name of the class and `__primarykey__` to ``"__id__"``, which is a special reference to the internal node ID.

For example::

    class Film(GraphObject):
        __primarylabel__ = "Movie"  # alternative primary label, instead of "Film"
        __primarykey__ = "title"    # film title is considered unique

TODO: __primaryvalue__

Properties
==========

.. autoclass:: py2neo.ogm.Property
   :members:

.. NOTE:: There is currently no support for constraining property type.

Labels
======

.. autoclass:: py2neo.ogm.Label
   :members:



Related Objects
===============

Related objects are `GraphObject` instances connected to a central `GraphObject` in a particular way.
For example, if a ``(:Person)-[:LIKES]->(:Person)`` relationship is used within the graph database to model a friendship, this might be modelled within the OGM layer as::

    class Person(GraphObject):
        __primarykey__ = "name"

        name = Property()

        likes = RelatedTo("Person")

This defines a `likes` attribute using :class:`.RelatedTo`, which describes an outgoing relationship.
:class:`.RelatedFrom` can be used for an incoming relationship and :class:`.Related` for a relationship where direction is unimportant.

When a `Person` is created, its set of friends can be queried and modified through its `person.likes` attribute which is itself a :class:`.RelatedObjects` instance.
Therefore to print a list of all friends::

    for friend in person.likes:
        print(friend.name)

.. NOTE:: It is not possible to constrain the set to contain only one item.

.. autoclass:: py2neo.ogm.Related
   :members:

.. autoclass:: py2neo.ogm.RelatedTo
   :members:

.. autoclass:: py2neo.ogm.RelatedFrom
   :members:

.. autoclass:: py2neo.ogm.RelatedObjects
   :members:


Selection
=========

.. autoclass:: py2neo.ogm.GraphObjectSelection
   :members:
