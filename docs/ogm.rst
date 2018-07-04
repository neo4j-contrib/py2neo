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
       tag_line = Property("tagline")
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
Each :class:`.GraphObject` wraps a node as well as a set of pointers to :class:`.RelatedObjects` and the relationship details that connect them.

A :class:`.GraphObject` instance may be constructed just like any other Python object but can also be `matched <#py2neo.ogm.GraphObject.match>`_ from the database.
Each instance may contain attributes that represent labels, nodes or related objects.

.. class:: py2neo.ogm.GraphObject

   .. attribute:: __primarylabel__

      The primary node label used for Cypher `MATCH` and `MERGE`
      operations. By default the name of the class is used.

   .. attribute:: __primarykey__

      The primary property key used for Cypher `MATCH` and `MERGE`
      operations. If undefined, the special value of ``"__id__"`` is used
      to hinge uniqueness on the internal node ID instead of a property.
      Note that this alters the behaviour of operations such as
      :meth:`.Graph.create` and :meth:`.Graph.merge` on :class:`.GraphObject`
      instances.

   .. attribute:: __primaryvalue__

      The value of the property identified by :attr:`.__primarykey__`.
      If the key is ``"__id__"`` then this value returns the internal
      node ID.

   .. automethod:: match

   .. automethod:: wrap

   .. automethod:: unwrap


Properties
==========

A :class:`.Property` defined on a :class:`.GraphObject` provides an accessor to a property of the underlying node.


.. autoclass:: py2neo.ogm.Property
   :members:

.. code-block:: python

    >>> class Person(GraphObject):
    ...     name = Property()
    ...
    >>> alice = Person()
    >>> alice.name = "Alice Smith"
    >>> alice.name
    "Alice Smith"


Labels
======

A :class:`.Label` defined on a :class:`.GraphObject` provides an accessor to a label of the underlying central node.
It is exposed as a boolean value, the setting of which allows the label to be toggled on or off.

Labels are exposed in the API in an identical way to boolean properties.
The difference between these is in how the value translates into the graph database.
A label should generally be used if matches are regularly carried out on that value.
Secondary or supporting information could be stored in a boolean property.

.. autoclass:: py2neo.ogm.Label
   :members:

.. code-block:: python

    >>> class Food(GraphObject):
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


Object Matching
===============

One or more :class:`.GraphObject` instances can be selected from the database by using the ``match`` method of the relevant subclass.

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


Object Operations
=================

:class:`.GraphObject` instances can be pushed into and pulled from the database just like other py2neo objects.
Unlike with other kinds of object though, a `GraphObject` can be pushed without having first created or merged it.
For example::

    >>> alice = Person()
    >>> alice.name = "Alice Smith"
    >>> graph.push(alice)
    >>> alice.__ogm__.node
   (cc3030a:Person {name:"Alice Smith"})


.. method:: Graph.pull(graph_object)

   Update a :class:`.GraphObject` and its associated :class:`.RelatedObject` instances with
   changes from the graph.

.. method:: Graph.push(graph_object)

   Update the graph with changes from a :class:`.GraphObject` and its associated
   :class:`.RelatedObject` instances. If a corresponding remote node does not exist, one will be
   created. If one does exist, it will be updated. The set of outgoing relationships will be
   adjusted to match those described by the `RelatedObject` instances.

.. method:: Graph.create(graph_object)
            Graph.merge(graph_object)

   For a :class:`.GraphObject`, `create` and `merge` are an identical operation. This is because
   `GraphObject` instances have uniqueness defined by their primary label and primary key and so
   both operations can be considered a form of `merge`.

   If a corresponding remote node does not exist, one will be created. Unlike `push`, however,
   no update will occur if a node already exists.

.. method:: Graph.delete(graph_object)

   Delete the remote node and relationships that correspond to the given :class:`.GraphObject`.
