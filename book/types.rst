************************************
``py2neo.types`` -- Graph Data Types
************************************

**Py2neo** provides a set of core graph data types that are completely compatible with Neo4j but
that can also be used independently of it. These types include the fundamental entities
:class:`.Node` and :class:`.Relationship` as well as classes that represent collections of these
entities.

These types have been carefully designed to work together using standard operations, most notably
set operations. Details of these operations are covered in the sections on :ref:`subgraphs` and
:ref:`walkable_types`.


Nodes & Relationships
=====================

The two essential building blocks of the property graph model used by Neo4j are the :class:`.Node`
and the :class:`.Relationship`. A node is the fundamental unit of data storage within a graph. It
can contain a set of key-value pairs (properties) and can optionally be adorned with one or more
textual labels. A relationship is a typed, directed connection between a pair of nodes (or
alternatively a `loop <https://en.wikipedia.org/wiki/Loop_%28graph_theory%29>`_ on a single node).
Like nodes, relationships may also contain a set of properties.

The code below shows how to create a couple of nodes and a relationship joining them. Each node has
a single property, `name`, and is labelled as a `Person`. The relationship ``ab`` describes a
`KNOWS` connection from the first node ``a`` to the second node ``b``.

::

    >>> from py2neo import Node, Relationship
    >>> a = Node("Person", name="Alice")
    >>> b = Node("Person", name="Bob")
    >>> ab = Relationship(a, "KNOWS", b)
    >>> ab
    (alice)-[:KNOWS]->(bob)


.. class:: Node(*labels, **properties)

    Construct a new node object with the labels and properties specified. In its initial state, a
    node is `unbound`. This means that it exists only on the client and does not reference a
    corresponding server node. A node is typically `bound` by
    :meth:`creating <.Transaction.create>` it in a Neo4j database.

    .. describe:: node == other

        Return ``True`` if *node* and *other* are equal. Node equality is based solely on the ID of
        the remote node it represents; neither properties nor labels factor into equality. This
        means that if `bound`, a node object can only be considered equal to another node object
        that is bound to the same remote node. If a node is `unbound`, thereby having no
        corresponding node ID, it can only ever be equal to itself.

    .. describe:: node != other

        Return ``True`` if the nodes are unequal.

    .. describe:: hash(node)

        Return a hash of *node* based on its object ID, if unbound, or the ID of the remote node
        it represents, if bound.

    .. describe:: node[key]

        Return the property value of *node* with key *key* or ``None`` if the key is missing.

    .. describe:: node[key] = value

        Set the property value of *node* with key *key* to *value* or remove the property if
        *value* is ``None``.

    .. describe:: del node[key]

        Remove the property with key *key* from *node*, raising a :exc:`KeyError` if such a
        property does not exist.

    .. describe:: len(node)

        Return the number of properties in *node*.

    .. describe:: dict(node)

        Return a dictionary of all the properties in *node*.

    .. describe:: walk(node)

        Yield *node* as the only item in a :func:`walk`.

    .. method:: labels()

        Return the full set of labels associated with the node.

    .. method:: has_label(label)

        Return ``True`` if the node has the label *label*.

    .. method:: add_label(label)

        Add the label *label* to the node.

    .. method:: remove_label(label)

        Remove the label *label* from the node if it exists.

    .. method:: clear_labels()

        Remove all labels from the node.

    .. method:: update_labels(labels)

        Add multiple labels to the node from the iterable *labels*.

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

    .. describe:: relationship == other

        Return ``True`` if *relationship* and *other* are equal. Relationship equality is based on
        equality of the start node, the end node and the relationship type (node equality is
        described above). This means that any two relationships of the same type between the same
        nodes are always considered equal. Note that this behaviour differs slightly from Neo4j
        itself which permits multiple relationships of the same type between the same nodes.

    .. describe:: relationship != other

        Return ``True`` if the relationships are unequal.

    .. describe:: hash(relationship)

        Return a hash of *relationship* based on its start node, end node and type.

    .. describe:: relationship[key]

        Return the property value of *relationship* with key *key* or ``None`` if the key is
        missing.

    .. describe:: relationship[key] = value

        Set the property value of *relationship* with key *key* to *value* or remove the property
        if *value* is ``None``.

    .. describe:: del relationship[key]

        Remove the property with key *key* from *relationship*, raising a :exc:`KeyError` if such a
        property does not exist.

    .. describe:: len(relationship)

        Return the number of properties in *relationship*.

    .. describe:: dict(relationship)

        Return a dictionary of all the properties in *relationship*.

    .. describe:: walk(relationship)

        Perform a :func:`walk` of this relationship, yielding its start node, the relationship
        itself and its end node in turn.

    .. method:: type()

        Return the type of this relationship.


Properties
----------

Both :class:`.Node` and :class:`.Relationship` extend the :class:`.PropertyDict` class which itself
extends Python's built-in dictionary. This means that nodes and relationships are both mapping
types that can contain property values, indexed by key.

As with Neo4j itself, property values may not be ``None``. A missing property (i.e. no key present)
is the idiomatic way to model absence of value.

The *PropertyDict* class is described in more detail below.

.. class:: PropertyDict(iterable, **kwargs)

    The *PropertyDict* extends Python's built-in *dict* type. All operations and methods are
    identical to those of the base class except for those described below.

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

    .. method:: setdefault(key, default=None)

        If *key* is in the PropertyDict, return its value. If not, insert *key* with a value of
        *default* and return *default* unless *default* is ``None``, in which case do nothing. The
        value of *default* defaults to ``None``.

    .. method:: update(iterable=None, **kwargs)

        Update the PropertyDict with the key-value pairs from *iterable* followed by the keyword
        arguments from *kwargs*. Individual properties already in the PropertyDict will be
        overwritten by those in *iterable* and subsequently by those in *kwargs* if the keys match.
        Any value of ``None`` will effectively delete the property with that key, should it exist.


.. _subgraphs:

Subgraphs
=========

A :class:`.Subgraph` is a collection of nodes and relationships. By definition, a *Subgraph* must
contain at least one node; `null subgraphs <http://mathworld.wolfram.com/NullGraph.html>`_ should
be represented by :const:`None`. To test for `emptiness <http://mathworld.wolfram.com/EmptyGraph.html>`_
the built-in :func:`bool` function can be used.

The simplest way to construct a subgraph is by combining nodes and relationships using standard set
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
    to many graph database functions. It is also used as a base class for :class:`.Node`,
    :class:`.Relationship` and :class:`.Walkable`, allowing instances of those classes to be
    combined using set operations.

    .. describe:: subgraph | other | ...

        Union. Return a new subgraph containing all nodes and relationships from *subgraph* as well
        as all those from *other*. Any entities common to both will only be included once.

    .. describe:: subgraph & other & ...

        Intersection. Return a new subgraph containing all nodes and relationships common to both
        *subgraph* and *other*.

    .. describe:: subgraph - other - ...

        Difference. Return a new subgraph containing all nodes and relationships that exist in
        *subgraph* but do not exist in *other*, as well as all nodes that are connected by the
        relationships in *subgraph* regardless of whether or not they exist in *other*.

    .. describe:: subgraph ^ other ^ ...

        Symmetric difference. Return a new subgraph containing all nodes and relationships that
        exist in *subgraph* or *other*, but not in both, as well as all nodes that are connected by
        those relationships regardless of whether or not they are common to *subgraph* and *other*.

    .. method:: subgraph.keys()

        Return the set of all property keys used by the nodes and relationships in this subgraph.

    .. method:: subgraph.labels()

        Return the set of all node labels in this subgraph.

    .. method:: subgraph.nodes()

        Return the set of all nodes in this subgraph.

    .. method:: subgraph.relationships()

        Return the set of all relationships in this subgraph.

    .. method:: subgraph.types()

        Return the set of all relationship types in this subgraph.

.. function:: order(subgraph)

    Return the number of nodes in `subgraph`. This argument can be an instance of
    :class:`.Subgraph` or of any derived class, such as :class:`.Path`.

.. function:: size(subgraph)

    Return the number of relationships in `subgraph`. This argument can be an instance of
    :class:`.Subgraph` or of any derived class, such as :class:`.Path`.


.. _walkable_types:

Walkable Types
==============

A :class:`.Walkable` is a :class:`.Subgraph` with added traversal information. The simplest way to
construct a walkable is by concatenating other graph objects::

    >>> w = ab + Relationship(b, "LIKES", c) + ac
    >>> w
    (alice)-[:KNOWS]->(bob)-[:LIKES]->(carol)<-[:WORKS_WITH]-(alice)

The traversal of a walkable object is achieved by using the :func:`walk` function, which yields
alternating nodes and relationships and always starts and ends with a node. Any node or relationship
may be traversed one or more times in any direction.

.. class:: Walkable(iterable)

    A *Walkable* is a :class:`.Subgraph` with added traversal information.

    .. describe:: walkable + other + ...

        Concatenation. Return a new :class:`.Walkable` that represents a :func:`walk` of `walkable`
        followed by a :func:`walk` of `other`. This is only possible if the end node of `walkable`
        is the same as either the start node or the end node of `other`; in the latter case, `other`
        will be walked in reverse.

        Nodes that overlap from one operand onto another are not duplicated in the returned
        :class:`.Walkable`.

    .. describe:: walk(walkable)

        Perform a :func:`walk` of *walkable*, yielding alternating nodes and relationships.

    .. method:: start_node()

        Return the first node encountered on a :func:`walk` of this object.

    .. method:: end_node()

        Return the last node encountered on a :func:`walk` of this object.

    .. method:: nodes()

        Return a tuple of all nodes traversed on a :func:`walk` of this :class:`.Walkable`, listed
        in the order in which they were first encountered.

    .. method:: relationships()

        Return a tuple of all relationships traversed on a :func:`walk` of this :class:`.Walkable`,
        listed in the order in which they were first encountered.

.. class:: Path(*entities)

    A *Path* is a type of :class:`.Walkable` returned by some Cypher queries.

.. function:: walk(*walkables)

    Traverse over the arguments supplied, in order, yielding alternating nodes and relationships.
