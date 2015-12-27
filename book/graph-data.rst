=======================
Graph Data Fundamentals
=======================

Before connecting to a Neo4j server, it's useful to become familiar with the fundamental data types of the property graph model offered by py2neo.
While these types are completely compatible with Neo4j, they can also be used independently of it.

The two essential building blocks are :class:`.Node` and :class:`.Relationship`.
Along with the container types :class:`.Subgraph` and :class:`.Walkable`, these provide a way to construct and work with Neo4j-compatible graph data.
The four types can be summarised as follows:

- :class:`.Node` - fundamental unit of data storage within a graph
- :class:`.Relationship` - typed connection between a pair of nodes
- :class:`.Subgraph` - collection of nodes and relationships
- :class:`.Walkable` - subgraph with added traversal information

The example below shows how to create a couple of nodes as well as a relationship joining them.
Each node has a single property, `name`, and is labelled as a `Person`.
The relationship has the type `KNOWS` and joins from the first node `a` to the second node `b`.

::

    >>> from py2neo import Node, Relationship
    >>> a = Node("Person", name="Alice")
    >>> b = Node("Person", name="Bob")
    >>> ab = Relationship(a, "KNOWS", b)

Relationship types can alternatively be created by extending the :class:`.Relationship` class.
The default type of such relationships is derived from the class name::

    >>> c = Node("Person", name="Carol")
    >>> class WorksWith(Relationship): pass
    >>> ac = WorksWith(a, c)
    >>> ac.type()
    'WORKS_WITH'

Arbitrary collections of nodes and relationships may be contained in a :class:`.Subgraph` object.
The simplest way to construct these is by combining nodes and relationships with standard set operations.
For example::

    >>> s = ab | ac
    >>> s
    {(xyz01:Person {name:"Alice"}),
     (xyz02:Person {name:"Bob"}),
     (xyz03:Person {name:"Carol"}),
     (xyz01)-[:KNOWS]->(xyz02),
     (xyz01)-[:WORKS_WITH]->(xyz03)}
    >>> s.nodes()
    frozenset({(xyz01:Person {name:"Alice"}),
               (xyz02:Person {name:"Bob"}),
               (xyz03:Person {name:"Carol"})})
    >>> s.relationships()
    frozenset({(xyz01)-[:KNOWS]->(xyz02),
               (xyz01)-[:WORKS_WITH]->(xyz03)})

A :class:`.Walkable` is a subgraph with added traversal information.
These can be formed by concatenating nodes and relationships::

    >>> w = ab + Relationship(b, "LIKES", c) + ac
    >>> w
    (xyz01)-[:KNOWS]->(xyz02)-[:LIKES]->(xyz03)<-[:WORKS_WITH]-(xyz01)


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
