==========================
Introduction to Py2neo 4.0
==========================

In this introduction, you will see how to get connected to a `Neo4j <http://neo4j.com/>`_ graph
database and how to carry out basic operations. You will also be introduced to many of the core
concepts needed when working with py2neo.


Getting Connected
=================

The simplest way to try out a connection to the Neo4j server is via the console. Once you have
started a local Neo4j server, open a new Python console and enter the following::

    >>> from py2neo import Graph
    >>> graph = Graph()

A password can also be supplied::

    >>> graph = Graph(password='camelot~1150')


This imports the :class:`Graph <py2neo.Graph>` class from py2neo and creates a instance bound to
the default Neo4j server URI, ``http://localhost:7474/db/data/``.

To connect to a server at an alternative address, simply pass in the URI value as a string argument
to the Graph constructor::

    >>> remote_graph = Graph("http://remotehost.com:6789/db/data/")

For a database behind a secure proxy, a user name and password can also be supplied to the
constructor URI. These credentials will then be applied to any subsequent HTTP requests made to the
host and port combination specified (in this case, ``camelot~1150``)::

    >>> secure_graph = Graph("https://arthur:excalibur@camelot~1150/db/data/")

A :class:`Graph <py2neo.Graph>` object provides a basis for most of the interaction with the Neo4j
server that a typical client application will need to make. The database URI is therefore generally
the only URI that needs to be provided explicitly.


Nodes & Relationships
=====================

`Nodes <http://neo4j.com/docs/2.1.5/javadocs/org/neo4j/graphdb/Node.html>`_ and
`relationships <http://neo4j.com/docs/2.1.5/javadocs/org/neo4j/graphdb/Relationship.html>`_ are the
fundamental building blocks of a Neo4j graph and both have a corresponding class in py2neo.
Assuming we've already established a connection to the server (as above) let's build a simple graph
with two nodes and one relationship::

    >>> from py2neo import Node, Relationship

    # Initialise Nodes and Relationship although they don't exist in the graph yet
    >>> alice = Node("Person", name="Alice") 
    >>> bob = Node("Person", name="Bob")
    >>> alice_knows_bob = Relationship(alice, "KNOWS", bob)

    # Create in graph
    >>> graph.create(alice_knows_bob)  

When first created, :class:`Node <py2neo.Node>` and :class:`Relationship <py2neo.Relationship>`
objects exist only in the client; nothing has yet been written to the server. The
:func:`Graph.create <py2neo.Graph.create>` method shown above creates corresponding server-side
objects and automatically binds each local object to its remote counterpart. Within py2neo,
*binding* is the process of applying a URI to a client object thereby allowing future
client-server synchonisation operations to occur.



Pushing & Pulling
=================

Client-server communication over `REST <http://neo4j.com/docs/2.1.4/rest-api/>`_ can be chatty if
not used carefully. Whenever possible, py2neo attempts to minimise the amount of chatter between
the client and the server by batching and lazily retrieving data. Most read and write operations
are explicit, allowing the Python application developer a high degree of control over network
traffic.

Py2neo will not carry out updates to client or server objects until this is explicitly requested.

To illustrate synchronisation, let's give Alice and Bob an ``age`` property each. Longhand, this is
done as follows::

    >>> alice.properties["favourite_colour"] = 'Blue'
    >>> graph.push(alice)

Here, we add a new property to each of the local nodes and :func:`push <py2neo.Node.push>` the
changes.


Pulling updates from server to client is similar: either call the :func:`pull <py2neo.Node.pull>`
method on an individual entity or batch together several updates by using
:func:`Graph.pull <py2neo.Graph.pull>`.
