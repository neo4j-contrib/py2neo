****************
Graph Data Types
****************

**Py2neo** provides a set of core graph data types that are completely compatible with Neo4j but
that can also be used independently of it. These types include the fundamental entities
:class:`.Node` and :class:`.Relationship` as well as classes used to construct these entities and
entity collections. In addition, a :class:`.CypherWriter` class, along with some associated
functions, provide facilities to conveniently output these types in string form.

These types have been carefully designed to work together using standard operations, most notably
set operations. Details of these operations are covered in detail in the section on :ref:`graph
arithmetic <graph-arithmetic>`.


Nodes & Relationships
=====================

The two essential building blocks of the property graph model used by Neo4j are the :class:`.Node`
and the :class:`.Relationship`. A node is the fundamental unit of data storage within a graph and
can contain a set of properties and be adorned with labels for purposes of classification. A
relationship is a typed connection between a pair of nodes (or occasionally between a node and
itself) that can also contain properties.

The code below shows how to create a couple of nodes and a relationship joining them. Each node has
a single property, `name`, and is labelled as a `Person`. The relationship ``ab`` describes a
connection from the first node ``a`` to the second node ``b`` of type `KNOWS`.

::

    >>> from py2neo import Node, Relationship
    >>> a = Node("Person", name="Alice")
    >>> b = Node("Person", name="Bob")
    >>> ab = Relationship(a, "KNOWS", b)
    >>> ab
    (alice)-[:KNOWS]->(bob)


.. class:: Node(*labels, **properties)

    Construct a new node object with the labels and properties specified. This node will not
    initially be bound to a Neo4j database.

    .. method:: node.labels()


.. class:: Relationship(start_node, type, end_node, **properties)
           Relationship(start_node, end_node, **properties)
           Relationship(node, type, **properties)
           Relationship(node, **properties)

    Construct a relationship between a pair of nodes (or between a node and itself) of type *type*.
    If the type is not specified, it will default to ``TO``. This default can be overridden by
    extending the ``Relationship`` class::

        >>> c = Node("Person", name="Carol")
        >>> class WorksWith(Relationship): pass
        >>> ac = WorksWith(a, c)
        >>> ac.type()
        'WORKS_WITH'


Properties
----------

Both :class:`.Node` and :class:`.Relationship` extend the :class:`.PropertyDict` class which itself
extends Python's built-in dictionary. This means that nodes and relationships are both mapping
types that can contain property values, indexed by key.

In a similar way to Neo4j, properties values may not be ``None``. A missing property (i.e. no key
present) is the idiomatic way to model absence of value.

The *PropertyDict* class is described in more detail below.

.. class:: PropertyDict(iterable, **kwargs)

    The *PropertyDict* extends Python's built-in *dict* type. All operations and methods are
    identical to those of the base class with the exceptions of the ones described below.

    .. describe:: properties == other

        Return ``True`` if ``properties`` is equal to ``other`` after all ``None`` values have been
        removed from ``other``.

    .. describe:: properties != other

        Return ``True`` if ``properties`` is unequal to ``other`` after all ``None`` values have
        been removed from ``other``.

    .. describe:: properties[key]

        Return the value of *properties* with key *key* or ``None`` if the key is missing.

    .. describe:: properties[key] = value

        Set the value of *properties* with key *key* to *value* or remove the property if *value*
        is ``None``.

    .. describe:: properties.setdefault(key, default=None)

        If *key* is in *properties*, return its value. If not, insert *key* with a value of
        *default* and return *default* unless *default* is ``None``, in which case do nothing. The
        value of *default* defaults to ``None``.

    .. describe:: properties.update(iterable=None, **kwargs)

        Update *properties* with the key-value pairs from *iterable* combined with the keyword
        arguments from *kwargs*, overwriting existing properties. Any values of ``None`` will not
        be included and will remove any property with that key that already exists.


Equality Rules
--------------

Node equality is based on identity. This means that a node is only ever equal to itself and is
*never* equal to other nodes, even those with identical properties and labels.

Relationship equality is based on type and endpoints. A relationship will be considered equal to
any other another relationship of the same type that is attached to the same nodes. Properties are
not considered for relationship equality.


Subgraphs
=========

Arbitrary collections of nodes and relationships may be collected in a :class:`.Subgraph` object.
The simplest way to construct these is by combining nodes and relationships using standard set
operations. For example::

    >>> s = ab | ac
    >>> s
    {(alice:Person {name:"Alice"}),
     (bob:Person {name:"Bob"}),
     (carol:Person {name:"Carol"}),
     (alice)-[:KNOWS]->(bob),
     (alice)-[:WORKS_WITH]->(carol)}
    >>> s.nodes()
    frozenset({(alice:Person {name:"Alice"}),
               (bob:Person {name:"Bob"}),
               (carol:Person {name:"Carol"})})
    >>> s.relationships()
    frozenset({(alice)-[:KNOWS]->(bob),
               (alice)-[:WORKS_WITH]->(carol)})


.. class:: Subgraph(nodes, relationships)

    A *Subgraph* is an immutable set of nodes and relationships that can be provided as an argument
    to graph database functions. It is also used as a base class for :class:`.Node`,
    :class:`.Relationship` and :class:`.Walkable`, giving instances of those classes operational
    compatibility with each other.

    .. method:: subgraph.keys()

        Return all the property keys used by the nodes and relationships in this subgraph.

    .. method:: subgraph.labels()

        Return all the node labels in this subgraph.

    .. method:: subgraph.nodes()

        Return all the nodes in this subgraph.

    .. method:: subgraph.relationships()

        Return all the relationships in this subgraph.

    .. method:: subgraph.types()

        Return all the relationship types in this subgraph.

.. function:: order(subgraph)

    Return the number of nodes in this subgraph.

.. function:: size(subgraph)

    Return the number of relationships in this subgraph.


Walkable Types
==============

A :class:`.Walkable` is a subgraph with added traversal information.
The simplest way to construct a :class:`.Walkable` is by concatenating
other graph objects::

    >>> w = ab + Relationship(b, "LIKES", c) + ac
    >>> w
    (alice)-[:KNOWS]->(bob)-[:LIKES]->(carol)<-[:WORKS_WITH]-(alice)


.. class:: Walkable(iterable)

.. class:: Path(iterable)

.. function:: walk(*walkables)


Graph Arithmetic
================

Graph objects can be combined in a number of ways using standard
Python operators. In this context, :class:`.Node` and :class:`.Relationship`
instances are treated as :class:`.Subgraph` or :class:`.Walkable` instances.
The available operations are detailed below.

Union
-----
**Syntax**: ``x | y``

The union of `x` and `y` is a :class:`.Subgraph` containing all
nodes and relationships from `x` as well as all those from `y`.
Any entities common to both operands will only be included once.

For example::

    >>> a = Node()
    >>> b = Node()
    >>> c = Node()
    >>> ab = Relationship(a, "TO", b)
    >>> ac = Relationship(a, "TO", c)
    >>> s = ab | ac
    >>> s
    {(a21abf3), (a0daea6), (b6515bc), (b6515bc)-[:TO]->(a0daea6), (b6515bc)-[:TO]->(a21abf3)}
    >>> s | Relationship(b, "TO", c)
    {(a0daea6), (a21abf3), (b6515bc), (b6515bc)-[:TO]->(a0daea6), (b6515bc)-[:TO]->(a21abf3), (a21abf3)-[:TO]->(a0daea6)}

Intersection
------------
**Syntax**: ``x & y``

The intersection of `x` and `y` is a :class:`.Subgraph` containing all
nodes and relationships common to both `x` and `y`.

Difference
----------
**Syntax**: ``x - y``

The difference between `x` and `y` is a :class:`.Subgraph` containing all
nodes and relationships that exist in `x` but do not exist in `y` as well
as all nodes that are connected by the the relationships in `x` regardless
of whether or not they exist in `y`.

Symmetric Difference
--------------------
**Syntax**: ``x ^ y``

The symmetric difference between `x` and `y` is a :class:`.Subgraph` containing
all nodes and relationships that exist in `x` or `y` but not in both as well
as all nodes that are connected by those relationships regardless
of whether or not they are common to `x` and `y`.

Concatenation
-------------
**Syntax**: ``x + y``

The concatenation of `x` and `y` is a :class:`.Walkable` that represents a
walk of `x` followed by a walk of `y`. This is only possible if the end node
of `x` is the same as either the start node or the end node of `y`; in the
latter case, `y` will be walked in reverse.


Records
=======

*TODO*

.. class:: Record(keys, values)


String Representations
======================

*TODO*
