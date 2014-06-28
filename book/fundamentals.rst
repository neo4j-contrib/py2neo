Fundamentals
============

At the heart of py2neo are three core classes needed for every application:
:py:class:`Graph <py2neo.neo4j.Graph>`,
:py:class:`Node <py2neo.neo4j.Node>` and
:py:class:`Relationship <py2neo.neo4j.Relationship>`. A
``Graph`` maintains a link to a Neo4j database via a root URI.
For a default installation, this will be `http://localhost:7474/db/data/`
(don't forget the trailing slash!) and can be constructed as follows::

    from py2neo import neo4j
    graph = neo4j.Graph("http://localhost:7474/db/data/")

A ``Graph`` object provides access to a number of core methods,
such as those to create and delete nodes, match relationships and manage
indexes. One of the most useful methods is simply named
:py:func:`create <py2neo.neo4j.Graph.create>` and this can be
used to easily build nodes and relationships within the graph. Imagine we
wanted to model the following data::

    (Bruce {"name": "Bruce Willis"})
    (John {"name": "John McClane"})
    (Alan {"name": "Alan Rickman"})
    (Hans {"name": "Hans Gruber"})
    (Nakatomi {"name": "Nakatomi Plaza"})
    (Bruce)-[:PLAYS]->(John)
    (Alan)-[:PLAYS]->(Hans)
    (John)-[:VISITS]->(Nakatomi)
    (Hans)-[:STEALS_FROM]->(Nakatomi)
    (John)-[:KILLS]->(Hans)

This could be carried out in a single ``create`` statement::

    from py2neo import node, rel
    die_hard = graph.create(
        node(name="Bruce Willis"),
        node(name="John McClane"),
        node(name="Alan Rickman"),
        node(name="Hans Gruber"),
        node(name="Nakatomi Plaza"),
        rel(0, "PLAYS", 1),
        rel(2, "PLAYS", 3),
        rel(1, "VISITS", 4),
        rel(3, "STEALS_FROM", 4),
        rel(1, "KILLS", 3),
    )

Each of the arguments passed to the statement above describes either a ``Node``
or a ``Relationship``. In py2neo, both of these entities may be either
*abstract* or *concrete*. A concrete entity describes one which exists (or has
existed) within a graph database; an abstract entity does not have such a link.

The same underlying classes are used to describe both abstract and concrete
entities. The core difference is that concrete entities will always have a
value within their ``__uri__`` property whereas for abstract entities, this is
:py:const:`None`. Both nodes and relationships provide an ``is_abstract``
property to allow easy determination of this attribute.

Node & Relationship Abstracts
-----------------------------

Many methods within py2neo require nodes and relationships to be provided in
abstract form, as in the example above. There are several different notations
for this but wherever possible, the :py:func:`node <neo4j.node>` and
:py:func:`rel <neo4j.rel>` functions should be used.

.. autofunction:: py2neo.node
.. autofunction:: py2neo.rel


.. note::
    The other representations shown are supported although may give less
    readability and are not considered future-proof.

Referencing Nodes within the same Batch
---------------------------------------

Within a batch context, it is often desirable to refer to a node within the
same batch. In some circumstances, it is possible to use an integer reference
to such a node and this makes it possible to carry out certain atomic
operations, such as creating a relationship between two newly-created nodes::

    graph.create(node({"name": "Alice"}), node({"name": "Bob"}), rel(0, "KNOWS", 1))

Due to server limitations however, not all functions support this capability.
In particular, functions that rely on implicit Cypher queries, such as
:py:func:`WriteBatch.create_path` cannot
support this notation.

Node & Relationship IDs
-----------------------

Py2neo provides limited facilities for managing node and relationship IDs.
Generally speaking, these IDs should not carry any relevance within your
application. There have been a number of discussions about this within the
Neo4j mailing list and Stack Overflow as the ID is an internal artifact and
should not be used like a primary key. It's purpose is more akin to an
in-memory address.

If you require unique identifiers, consider using
`UUIDs <http://docs.python.org/2/library/uuid.html#uuid.uuid4>`_.

Transactions
------------

Py2neo does not yet provide explicit support for transactions. Instead,
consider using a Cypher query or a batch request.

Errors
------

HTTP requests may occasionally trigger an error response. The exceptions which
may be raised are below and correspond to the equivalently named HTTP
response statuses.

.. autoclass:: py2neo.exceptions.GraphError

.. autoclass:: py2neo.exceptions.ServerError

.. autoclass:: py2neo.exceptions.BatchError

.. autoclass:: py2neo.exceptions.CypherError

