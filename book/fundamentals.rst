Fundamentals
============

At the heart of py2neo are three core classes needed for every application:
:py:class:`GraphDatabaseService <py2neo.neo4j.GraphDatabaseService>`,
:py:class:`Node <py2neo.neo4j.Node>` and
:py:class:`Relationship <py2neo.neo4j.Relationship>`. A
``GraphDatabaseService`` maintains a link to a Neo4j database via a root URI.
For a default installation, this will be
:py:attr:`http://localhost:7474/db/data/<py2neo.neo4j.DEFAULT_URI>` (don't
forget the trailing slash!) and can be constructed as follows::

    from py2neo import neo4j
    graph_db = neo4j.GraphDatabaseService("http://localhost:7474/db/data/")

A ``GraphDatabaseService`` object provides access to a number of core methods,
such as those to create and delete nodes, match relationships and manage
indexes. One of the most useful methods is simply named
:py:func:`create <py2neo.neo4j.GraphDatabaseService.create>` and this can be
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
    die_hard = graph_db.create(
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
method to allow easy determination of this attribute.

Node & Relationship Literals
----------------------------

Many methods within py2neo require nodes and relationships to be provided as
literals (as in the example above). There are a number of ways to do this but
the recommendation going forward is for abstract entities is to use the
:py:func:`node <neo4j.node>` and :py:func:`rel <neo4j.rel>` functions
whenever possible.

.. autofunction:: py2neo.node
.. autofunction:: py2neo.rel

Other representations are supported although these are not considered
future-proof. The following definitions show equivalent variations for modeling
abstract nodes (with the preferred variation listed first)::

    node(name="Alice", age=34)
    node({"name": "Alice", "age": 34})
    {"name": "Alice", "age": 34}

Similarly, the definitions below apply to abstract relationships (also with the
preferred variation listed first)::

    rel(alice, "KNOWS", bob, since=1999)
    rel(alice, "KNOWS", bob, {"since": 1999})
    rel((alice, "KNOWS", bob, {"since": 1999}))
    (alice, "KNOWS", bob, {"since": 1999})

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

While the core Neo4j engine supports transactions, the REST API used by py2neo
does not yet do so explicitly. If you prefer to wrap your code into
transactions, consider using a Cypher query or a batch request.

Errors
------

HTTP requests may occasionally trigger an error response. The exceptions which
may be raised are below and correspond to the equivalently named HTTP
response statuses.

.. autoclass:: py2neo.rest.BadRequest
    :show-inheritance:

.. autoclass:: py2neo.rest.ResourceNotFound
    :show-inheritance:

.. autoclass:: py2neo.rest.ResourceConflict
    :show-inheritance:
