**************************************
``py2neo.ogm`` -- Object-Graph Mapping
**************************************

.. module:: py2neo.ogm

The ``py2neo.ogm`` package contains a set of facilities for binding Python objects to an underlying set of graph data.
Class definitions extend :class:`.Model` and include :class:`.Property` and :class:`.Label` definitions as well as details of :class:`.Related` objects.

A simple example, based on the movie graph data set::

   class Movie(Model):
       __primarykey__ = "title"

       title = Property()
       tag_line = Property("tagline")
       released = Property()

       actors = RelatedFrom("Person", "ACTED_IN")
       directors = RelatedFrom("Person", "DIRECTED")
       producers = RelatedFrom("Person", "PRODUCED")


   class Person(Model):
       __primarykey__ = "name"

       name = Property()
       born = Property()

       acted_in = RelatedTo(Movie)
       directed = RelatedTo(Movie)
       produced = RelatedTo(Movie)


Repositories
============

.. autoclass:: py2neo.ogm.Repository
   :members:


Graph Objects
=============

At the heart of the py2neo OGM framework is the :class:`.Model`.
This is a base class for all classes that are to be mapped onto the graph database.
Each :class:`.Model` wraps a node as well as a set of pointers to :class:`.RelatedObjects` and the relationship details that connect them.

A :class:`.Model` instance may be constructed just like any other Python object but can also be `matched <#py2neo.ogm.Model.match>`_ from the database.
Each instance may contain attributes that represent labels, nodes or related objects.

.. class:: py2neo.ogm.Model

   .. attribute:: __primarylabel__

      The primary node label used for Cypher `MATCH` and `MERGE`
      operations. By default the name of the class is used.

   .. attribute:: __primarykey__

      The primary property key used for Cypher `MATCH` and `MERGE`
      operations. If undefined, the special value of ``"__id__"`` is used
      to hinge uniqueness on the internal node ID instead of a property.
      Note that this alters the behaviour of operations such as
      :meth:`.Graph.create` and :meth:`.Graph.merge` on :class:`.Model`
      instances.

   .. attribute:: __primaryvalue__

      The value of the property identified by :attr:`.__primarykey__`.
      If the key is ``"__id__"`` then this value returns the internal
      node ID.

   .. attribute:: __node__

      The :class:`.Node` wrapped by this :class:`.Model`.

   .. automethod:: wrap

   .. automethod:: match


Properties
==========

A :class:`.Property` defined on a :class:`.Model` provides an accessor to a property of the underlying node.


.. autoclass:: py2neo.ogm.Property
   :members:

.. code-block:: python

    >>> class Person(Model):
    ...     name = Property()
    ...
    >>> alice = Person()
    >>> alice.name = "Alice Smith"
    >>> alice.name
    "Alice Smith"


Labels
======

A :class:`.Label` defined on a :class:`.Model` provides an accessor to a label of the underlying central node.
It is exposed as a boolean value, the setting of which allows the label to be toggled on or off.

Labels are exposed in the API in an identical way to boolean properties.
The difference between these is in how the value translates into the graph database.
A label should generally be used if matches are regularly carried out on that value.
Secondary or supporting information could be stored in a boolean property.

.. autoclass:: py2neo.ogm.Label
   :members:

.. code-block:: python

    >>> class Food(Model):
    ...     hot = Label()
    ...
    >>> pizza = Food()
    >>> pizza.hot
    False
    >>> pizza.hot = True
    >>> pizza.hot
    True


Related Objects
===============

Related objects are `Model` instances connected to a central `Model` in a particular way.
For example, if a ``(:Person)-[:LIKES]->(:Person)`` relationship is used within the graph database to model a friendship, this might be modelled within the OGM layer as::

    class Person(Model):
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


Object Matching
===============

One or more :class:`.Model` instances can be selected from the database by using the ``match`` method of the relevant subclass.

To select a single instance using the primary label and primary key::

    >>> Person.match(graph, "Keanu Reeves").first()
    <Person name='Keanu Reeves'>

To select all instances that match certain criteria, you can simply iterate through the ``Match`` object.
Note the use of the underscore in the Cypher `WHERE` clause to refer to the underlying node::

    >>> list(Person.match(graph).where("_.name =~ 'K.*'"))
    [<Person name='Keanu Reeves'>,
     <Person name='Kevin Bacon'>,
     <Person name='Kiefer Sutherland'>,
     <Person name='Kevin Pollak'>,
     <Person name='Kelly McGillis'>,
     <Person name='Kelly Preston'>]


.. autoclass:: py2neo.ogm.GraphObjectMatch
   :members: __iter__, first
