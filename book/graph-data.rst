=======================
Graph Data Fundamentals
=======================

Before connecting to a Neo4j server, it's useful to become familiar with the fundamental data types of the property graph model offered by py2neo.
While these types are completely compatible with Neo4j, they can also be used independently of it.

The two most important data types are :class:`.Node` (the primary container for data within a graph) and :class:`.Relationship` (a way to connect two nodes in a meaningful way)::

    >>> from py2neo import Node, Relationship
    >>> a = Node("Person", name="Alice")
    >>> b = Node("Person", name="Bob")
    >>> ab = Relationship(a, "KNOWS", b)

The example above shows how to create a couple of nodes, each with one label (Person) and one property (name), as well as a "KNOWS" relationship joining them.
Relationship types can alternatively be created by extending the :class:`.Relationship` class.
The default type of such relationships is derived from the class name::

    >>> c = Node("Person", name="Carol")
    >>> class WorksWith(Relationship): pass
    >>> ac = WorksWith(a, c)
    >>> ac.type()
    'WORKS_WITH'

Arbitrary collections of nodes and relationships may be contained in a :class:`.Subgraph` object.
The simplest way to form :class:`.Subgraph` instances is by combining nodes and relationships with standard set operations.
For example::

    >>> s = ab | ac
    >>> s
    {(_dq3ITO64:Person {name:"Carol"}),
     (_dq3ITNw8:Person {name:"Alice"}),
     (_dq3ITNzk:Person {name:"Bob"}),
     (_dq3ITNw8)-[_dq3ITO2S:KNOWS]->(_dq3ITNzk),
     (_dq3ITNw8)-[_dq3ITO7s:WORKS_WITH]->(_dq3ITO64)}
    >>> s.nodes()
    frozenset({(_dq3ITO64:Person {name:"Carol"}),
               (_dq3ITNw8:Person {name:"Alice"}),
               (_dq3ITNzk:Person {name:"Bob"})})
    >>> s.relationships()
    frozenset({(_dq3ITNw8)-[_dq3ITO2S:KNOWS]->(_dq3ITNzk),
               (_dq3ITNw8)-[_dq3ITO7s:WORKS_WITH]->(_dq3ITO64)})

A :class:`.Walkable` is a subgraph with added traversal information.
These can be formed by concatenating nodes and relationships::

    >>> t = ab + ac


Type Summary
============

- :class:`.Node` - unit of data storage within a graph
- :class:`.Relationship` - typed connected between a pair of nodes
- :class:`.Subgraph` - collection of nodes and relationships
- :class:`.Walkable` - subgraph with traversal information

=============  ============  ============  ============  ============
Type           Node          Relationship  Subgraph      Walkable
=============  ============  ============  ============  ============
``.order()``   1             1 or 2        0 or more
``.size()``    0             1             0 or more
``.length()``  0             1             `n/a`
=============  ============  ============  ============  ============


The available operations are:

====================  ===========  ===========
Operation             Notation     Result
====================  ===========  ===========
union                 ``s1 | s2``  A subgraph containing all nodes and relationships from s1 and s2 combined
intersection          ``s1 & s2``  A subgraph containing all nodes and relationships common to both s1 and s2
difference            ``s1 - s2``
symmetric difference  ``s1 ^ s2``
====================  ===========  ===========


Equality Rules
==============

TODO


API
===

.. autoclass:: py2neo.graph.Node
   :show-inheritance:
   :members:

.. autoclass:: py2neo.graph.Relationship
   :show-inheritance:
   :members:

.. autoclass:: py2neo.graph.Subgraph
   :show-inheritance:
   :members:

.. autoclass:: py2neo.graph.Walkable
   :show-inheritance:
   :members:

.. autofunction:: py2neo.walk

.. autoclass:: py2neo.data.PropertyContainer
   :show-inheritance:
   :members:

.. autoclass:: py2neo.data.PropertySet
   :show-inheritance:
   :members:
