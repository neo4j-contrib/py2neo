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

As well as nodes and relationships, composite types :class:`.Subgraph` and :class:`.TraversableSubgraph` are also available.
These both allow arbitrary collections of nodes and relationships to be combined into a single object.
The latter also allows for traversal information to be stored, giving the structure a natural order.

===================  ===========
Type                 Description
===================  ===========
Node                 A fundamental unit of data storage within a property graph that may optionally be connected, via relationships, to other nodes.
Relationship
Subgraph
TraversableSubgraph
===================  ===========

The simplest way to form :class:`.Subgraph` instances is by combining nodes and relationships with set operations::

    >>> s = ab | ac
    >>> s
    <Subgraph order=3 size=2>
    >>> s.nodes()
    frozenset({<Node labels={'Person'} properties={'name': 'Alice'}>,
               <Node labels={'Person'} properties={'name': 'Bob'}>,
               <Node labels={'Person'} properties={'name': 'Carol'}>})
    >>> s.relationships()
    frozenset({<Relationship type='WORKS_WITH' properties={}>,
               <Relationship type='KNOWS' properties={}>})

The available operations are:

====================  ===========  ===========
Operation             Notation     Result
====================  ===========  ===========
union                 ``s1 | s2``  A subgraph containing all nodes and relationships from s1 and s2 combined
intersection          ``s1 & s2``  A subgraph containing all nodes and relationships common to both s1 and s2
difference            ``s1 - s2``
symmetric difference  ``s1 ^ s2``
====================  ===========  ===========

Likewise, :class:`.TraversableSubgraph` instances can be formed by concatenating nodes and relationships::

    >>> t = ab + ac


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

.. autoclass:: py2neo.graph.TraversableSubgraph
   :show-inheritance:
   :members:

.. autofunction:: py2neo.traverse

.. autoclass:: py2neo.data.PropertyContainer
   :show-inheritance:
   :members:

.. autoclass:: py2neo.data.PropertySet
   :show-inheritance:
   :members:
