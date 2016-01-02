=======================
Graph Data Fundamentals
=======================

Before connecting to a Neo4j server, it's useful to become familiar with the fundamental data types of the property graph model offered by py2neo.
While the types described here are completely compatible with Neo4j, they can also be used independently of it.


Type Overview
=============

The two essential building blocks of the py2neo property graph model are :class:`.Node` and :class:`.Relationship`.
Along with the container types :class:`.Subgraph` and :class:`.Walkable`, these provide a way to construct and work with a wide variety of Neo4j-compatible graph data.
The four types can be summarised as follows:

- :class:`.Node` - fundamental unit of data storage within a graph
- :class:`.Relationship` - typed connection between a pair of nodes
- :class:`.Subgraph` - collection of nodes and relationships
- :class:`.Walkable` - subgraph with added traversal information

The example below shows how to create a couple of nodes and a relationship joining them.
Each node has a single property, `name`, and is labelled as a `Person`.
The relationship ``ab`` describes a connection from the first node ``a`` to the second node ``b`` of type `KNOWS`.

::

    >>> from py2neo import Node, Relationship
    >>> a = Node("Person", name="Alice")
    >>> b = Node("Person", name="Bob")
    >>> ab = Relationship(a, "KNOWS", b)

Relationship types can alternatively be determined by a class that extends the :class:`.Relationship` class.
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
The simplest way to construct a :class:`.Walkable` is by concatenating other graph objects::

    >>> w = ab + Relationship(b, "LIKES", c) + ac
    >>> w
    (xyz01)-[:KNOWS]->(xyz02)-[:LIKES]->(xyz03)<-[:WORKS_WITH]-(xyz01)


Graph Arithmetic
================

Graph objects can be combined in a number of ways using standard Python operators.
In this context, Node and Relationship objects are treated as simple :class:`.Subgraph` instances.
The full set of operations are detailed below.

Union
-----
**Syntax**: ``x | y``

The union of `x` and `y` is a :class:`.Subgraph` containing all nodes and relationships from `x` as well as all nodes and relationships from `y`.
Any entities common to both operands will only be included once.

For example::

    >>> a = Node()
    >>> b = Node()
    >>> c = Node()
    >>> ab = Relationship(a, "TO", b)
    >>> ac = Relationship(a, "TO", c)
    >>> s = ab | ac
    >>> s
    {(Z0N0a), (Z0PAe), (Z0PCS), (Z0PAe)-[:TO]->(Z0PCS), (Z0PAe)-[:TO]->(Z0N0a)}
    >>> s | Relationship(b, "TO", c)
    {(Z0N0a), (Z0PAe), (Z0PCS), (Z0N0a)-[:TO]->(Z0PCS), (Z0PAe)-[:TO]->(Z0PCS), (Z0PAe)-[:TO]->(Z0N0a)}


====================  ===========  ===========
Operation             Notation     Result
====================  ===========  ===========
union                 ``s1 | s2``  A :class:`.Subgraph` containing all nodes and relationships from `s1` and `s2` combined
intersection          ``s1 & s2``  A :class:`.Subgraph` containing all nodes and relationships common to both `s1` and `s2`
difference            ``s1 - s2``  A :class:`.Subgraph` containing all nodes and relationships from `s1` excluding those that are also in `s2` (nodes in `s2` attached to relationships in `s1` will remain)
symmetric difference  ``s1 ^ s2``  A :class:`.Subgraph` containing all nodes and relationships in either `s1` or `s2` but not both (nodes attached to relationships solely in `s1` or `s2` will remain)
concatenation         ``s1 + s2``  A :class:`.Walkable` containing a :func:`.walk` of `s1` followed by a :func:`.walk` of `s2`
====================  ===========  ===========


Equality Rules
==============

Node equality is based on identity.
This means that a node is only equal to itself and is not equal to another node with the same properties and labels.

Relationship equality is based on type and endpoints.
A relationship will therefore be considered equal to another relationship of the same type attached to the same nodes.
Properties are not considered for relationship equality.

