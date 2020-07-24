***********************************
``py2neo.data`` -- Graph data types
***********************************

.. module:: py2neo.data

.. note:: For convenience, the members of ``py2neo.data`` can also be imported directly from ``py2neo``.

Py2neo provides a rich set of data types for working with both graph and record-based data.
The graph types are completely compatible with Neo4j but can also be used independently.

The two essential building blocks of the labelled property graph model used by Neo4j are the :class:`.Node` and the :class:`.Relationship`.
The node is the fundamental unit of data storage within a graph.
It can contain a set of key-value pairs (properties) and can optionally be adorned with one or more textual labels.
A relationship is a typed, directed connection between a pair of nodes (or alternatively a `loop <https://en.wikipedia.org/wiki/Loop_%28graph_theory%29>`_ on a single node).
Like nodes, relationships may also contain a set of properties.

The code below shows how to create a couple of nodes and a relationship joining them.
Each node has a single property, `name`, and is labelled as a `Person`.
The relationship ``ab`` describes a `KNOWS` connection from the first node ``a`` to the second node ``b``.

::

    >>> from py2neo.data import Node, Relationship
    >>> a = Node("Person", name="Alice")
    >>> b = Node("Person", name="Bob")
    >>> ab = Relationship(a, "KNOWS", b)
    >>> ab
    (Alice)-[:KNOWS]->(Bob)


:class:`.Node` objects
======================

.. autoclass:: Node(*labels, **properties)

    ..
        .. automethod:: cast
        .. automethod:: hydrate

    .. raw:: html

        <h4>Identity and equality</h4>

    The properties and methods described in the section below relate to now node equality works, as well as how nodes can be uniquely identified.
    Note that bound nodes exhibit slightly different behaviour to unbound nodes, with respect to identity.

    .. describe:: node == other

        Return :py:const:`True` if `node` and `other` are equal.
        Node equality is based solely on the ID of the remote node it represents; neither properties nor labels factor into equality.
        This means that if `bound`, a node object can only be considered equal to another node object that is bound to the same remote node.
        If a node is `unbound`, thereby having no corresponding node ID, it can only ever be equal to itself.

    .. describe:: node != other

        Return :py:const:`True` if the nodes are unequal.

    .. describe:: hash(node)

        Return a hash of `node` based on its object ID, if unbound, or the ID of the remote node it represents, if bound.

    .. attribute:: graph

        The remote graph to which this node is bound, if any.

    .. attribute:: identity

        The ID of the remote node to which this node is bound, if any.

    .. raw:: html

        <h4>Labels</h4>

    The property and methods below provide a way to view and manipulate
    the labels attached to a :class:`.Node`. Labels are a unique,
    unordered set of tags that can be used to classify and identify
    certain nodes.

        >>> a = Node("Person", name="Alice")
        >>> set(a.labels)
        {'Person'}
        >>> a.add_label("Employee")
        >>> set(a.labels)
        {'Employee', 'Person'}

    .. autoattribute:: labels

    .. automethod:: has_label(label)

    .. automethod:: add_label(label)

    .. automethod:: remove_label(label)

    .. automethod:: clear_labels()

    .. automethod:: update_labels(labels)

    .. raw:: html

        <h4>Properties</h4>

    Node properties can be queried and managed using the attributes
    below. Note that changes occur only on the local side of a bound
    node, and therefore the node must be :meth:`pushed <.Transaction.push>`
    in order to propagate these changes to the remote node.

    .. describe:: node[name]

        Return the value of the property called `name`, or :py:const:`None` if the name is missing.

    .. describe:: node[name] = value

        Set the value of the property called `name` to `value`, or remove the property if `value` is :py:const:`None`.

    .. describe:: del node[name]

        Remove the property called `name` from this node, raising a :exc:`KeyError` if such a property does not exist.

    .. describe:: len(node)

        Return the number of properties on this node.

    .. describe:: dict(node)

        Return a dictionary of all the properties in this node.

    .. method:: clear()

        Remove all properties from this node.

    .. method:: get(name, default=None)

        Return the value of the property called `name`, or `default` if the name is missing.

    .. method:: items()

        Return a list of all properties as 2-tuples of name-value pairs.

    .. method:: keys()

        Return a list of all property names.

    .. method:: setdefault(name, default=None)

        If this node has a property called `name`, return its value.
        If not, add a new property with a value of `default` and return `default`.

    .. method:: update(properties, **kwproperties)

        Update the properties on this node with a dictionary or name-value
        list of `properties`, plus additional `kwproperties`.

    .. method:: values()

        Return a list of all property values.


:class:`.Relationship` objects
==============================

.. class:: Relationship(start_node, type, end_node, **properties)
           Relationship(start_node, end_node, **properties)
           Relationship(node, type, **properties)
           Relationship(node, **properties)

    Construct a relationship between a pair of nodes (or between a node and itself) of type *type*.
    If the type is not specified, it will default to ``TO``.
    This default can be overridden by extending the ``Relationship`` class::

        >>> c = Node("Person", name="Carol")
        >>> class WorksWith(Relationship): pass
        >>> ac = WorksWith(a, c)
        >>> type(ac)
        'WORKS_WITH'

    .. raw:: html

        <h4>Identity and equality</h4>

    The properties and methods described in the section below relate to now relationship equality works, as well as how relationships can be uniquely identified.
    Note that bound relationships exhibit slightly different behaviour to unbound relationships, with respect to identity.

    .. describe:: relationship == other

        Return :py:const:`True` if `relationship` and `other` are equal.
        Relationship equality is based on equality of the start node, the end node and the relationship type (node equality is described above).
        This means that any two relationships of the same type between the same nodes are always considered equal.
        Note that this behaviour differs slightly from Neo4j itself which permits multiple relationships of the same type between the same nodes.

    .. describe:: relationship != other

        Return :py:const:`True` if the relationships are unequal.

    .. describe:: hash(relationship)

        Return a hash of `relationship` based on its start node, end node and type.

    .. attribute:: graph

        The remote graph to which this relationship is bound, if any.

    .. attribute:: identity

        The ID of the remote relationship to which this relationship is bound, if any.

    .. raw:: html

        <h4>Type and geometry</h4>

    These attributes relate to the type and endpoints of the relationship.
    Every node in Neo4j is directed, and this is reflected in the API here.
    Node objects therefore have a designated start and end node, which can be accessed through the :attr:`.start_node` and :attr:`.end_node` attributes respectively.

    .. describe:: type(relationship)

        Return the :class:`.Relationship` subclass for `relationship`.

    .. describe:: type(relationship).__name__

        Return the name of the :class:`.Relationship` subclass for `relationship` as a string.

    .. attribute:: nodes

        A 2-tuple of start node and end node.

    .. attribute:: start_node

        The start node for this relationship.

    .. attribute:: end_node

        The end node for this relationship.

    .. raw:: html

        <h4>Properties</h4>

    Relationship properties can be queried and managed using the attributes
    below. Note that changes occur only on the local side of a bound
    relationship, and therefore the relationship must be :meth:`pushed <.Transaction.push>`
    in order to propagate these changes to the remote relationship.

    .. describe:: relationship[name]

        Return the value of the property called `name`, or :py:const:`None` if the name is missing.

    .. describe:: relationship[name] = value

        Set the value of the property called `name` to `value`, or remove the property if `value` is :py:const:`None`.

    .. describe:: del relationship[name]

        Remove the property called `name` from this relationship, raising a :exc:`KeyError` if such a property does not exist.

    .. describe:: len(node)

        Return the number of properties on this relationship.

    .. describe:: dict(node)

        Return a dictionary of all the properties in this relationship.

    .. method:: clear()

        Remove all properties from this relationship.

    .. method:: get(name, default=None)

        Return the value of the property called `name`, or `default` if the name is missing.

    .. method:: items()

        Return a list of all properties as 2-tuples of name-value pairs.

    .. method:: keys()

        Return a list of all property names.

    .. method:: setdefault(name, default=None)

        If this relationship has a property called `name`, return its value.
        If not, add a new property with a value of `default` and return `default`.

    .. method:: update(properties, **kwproperties)

        Update the properties on this relationship with a dictionary or name-value
        list of `properties`, plus additional `kwproperties`.

    .. method:: values()

        Return a list of all property values.


:class:`.Path` objects
======================

.. class:: Path(*entities)

    A *Path* is a type of :class:`.Walkable` returned by some Cypher queries.


``Subgraph`` objects
====================

A :class:`.Subgraph` is an arbitrary collection of nodes and relationships.
By definition, a *Subgraph* must contain at least one node; `null subgraphs <http://mathworld.wolfram.com/NullGraph.html>`_ should be represented by :const:`None`.
To test for `emptiness <http://mathworld.wolfram.com/EmptyGraph.html>`_ the built-in :func:`bool` function can be used.

The simplest way to construct a subgraph is by combining nodes and relationships using standard set operations.
For example::

    >>> s = ab | ac
    >>> s
    {(alice:Person {name:"Alice"}),
     (bob:Person {name:"Bob"}),
     (carol:Person {name:"Carol"}),
     (Alice)-[:KNOWS]->(Bob),
     (Alice)-[:WORKS_WITH]->(Carol)}
    >>> s.nodes()
    frozenset({(alice:Person {name:"Alice"}),
               (bob:Person {name:"Bob"}),
               (carol:Person {name:"Carol"})})
    >>> s.relationships()
    frozenset({(Alice)-[:KNOWS]->(Bob),
               (Alice)-[:WORKS_WITH]->(Carol)})


.. class:: Subgraph(nodes, relationships)

    A *Subgraph* is an immutable set of nodes and relationships that can be provided as an argument to many graph database functions.
    It is also the base class for :class:`.Node`, :class:`.Relationship` and :class:`.Walkable`, allowing instances of those classes to be combined using set operations.

    .. describe:: subgraph | other | ...

        Union.
        Return a new subgraph containing all nodes and relationships from *subgraph* as well as all those from *other*.
        Any entities common to both will only be included once.

    .. describe:: subgraph & other & ...

        Intersection.
        Return a new subgraph containing all nodes and relationships common to both *subgraph* and *other*.

    .. describe:: subgraph - other - ...

        Difference.
        Return a new subgraph containing all nodes and relationships that exist in *subgraph* but do not exist in *other*,
        as well as all nodes that are connected by the relationships in *subgraph* regardless of whether or not they exist in *other*.

    .. describe:: subgraph ^ other ^ ...

        Symmetric difference.
        Return a new subgraph containing all nodes and relationships that exist in *subgraph* or *other*, but not in both,
        as well as all nodes that are connected by those relationships regardless of whether or not they are common to *subgraph* and *other*.

    .. method:: subgraph.keys

        Return the set of all property keys used by the nodes and relationships in this subgraph.

    .. attribute:: subgraph.labels

        Return the set of all node labels in this subgraph.

    .. attribute:: subgraph.nodes

        Return the set of all nodes in this subgraph.

    .. attribute:: subgraph.relationships

        Return the set of all relationships in this subgraph.

    .. method:: subgraph.types

        Return the set of all relationship types in this subgraph.


.. _subgraph_arithmetic:

Subgraph arithmetic
-------------------

TODO


``Walkable`` objects
====================

A :class:`.Walkable` is a :class:`.Subgraph` with added traversal information
The simplest way to construct a walkable is to concatenate other graph objects::

    >>> w = ab + Relationship(b, "LIKES", c) + ac
    >>> w
    (Alice)-[:KNOWS]->(Bob)-[:LIKES]->(Carol)<-[:WORKS_WITH]-(Alice)

The traversal of a walkable object is achieved by using the :func:`walk` function, which yields alternating nodes and relationships and always starts and ends with a node.
Any node or relationship may be traversed one or more times in any direction.

.. class:: Walkable(iterable)

    A *Walkable* is a :class:`.Subgraph` with added traversal information.

    .. describe:: walkable + other + ...

        Concatenation.
        Return a new :class:`.Walkable` that represents a :func:`walk` of `walkable` followed by a :func:`walk` of `other`.
        This is only possible if the end node of `walkable` is the same as either the start node or the end node of `other`;
        in the latter case, `other` will be walked in reverse.

        Nodes that overlap from one operand onto another are not duplicated in the returned :class:`.Walkable`.

    .. describe:: walk(walkable)

        Perform a :func:`walk` of *walkable*, yielding alternating nodes and relationships.

    .. describe:: start_node

        Return the first node encountered on a :func:`walk` of this object.

    .. describe:: end_node

        Return the last node encountered on a :func:`walk` of this object.

    .. describe:: nodes

        Return a tuple of all nodes traversed on a :func:`walk` of this :class:`.Walkable`, listed in the order in which they were first encountered.

    .. describe:: relationships

        Return a tuple of all relationships traversed on a :func:`walk` of this :class:`.Walkable`, listed in the order in which they were first encountered.

``walk`` function
-----------------

.. function:: walk(*walkables)

    Traverse over the arguments supplied, in order, yielding alternating nodes and relationships.

.. _walkable_chaining:

Chaining walkables
------------------

TODO
