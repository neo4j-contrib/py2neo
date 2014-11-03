==========================
Introduction to Py2neo 2.0
==========================

In this introduction, you will see how to get connected to a Neo4j graph database and how to carry
out basic operations. You will also be introduced to the core concepts needed when working with
py2neo.


Getting Connected
=================

The simplest way to try out a connection to the Neo4j server is via the console. Once you have
started a local Neo4j server, open a new Python console and enter the following::

    >>> from py2neo import Graph
    >>> graph = Graph()

This imports the `Graph` class from py2neo and creates a new instance for the default server URI:
'http://localhost:7474/db/data/'.

To connect to a server at an alternative address, simply pass in the URI value as a string argument
to the Graph constructor::

    >>> remote_graph = Graph("http://remotehost.com:6789/db/data/")

For a database behind a secure proxy, a user name and password can also be supplied to the
constructor URI. These credentials will then be applied to any subsequent HTTP requests made to the
host and port combination specified::

    >>> secure_graph = Graph("https://arthur:excalibur@camelot:1150/db/data/")

The Graph object provides a basis for most of the interaction with a Neo4j server and for that
reason, the database URI is generally the only one that needs to be provided explicitly.


Nodes & Relationships
=====================

Nodes and relationships are the fundamental data containers in Neo4j and both have a corresponding
class in py2neo. Assuming we've already established a connection to the server (as above) let's
build a simple graph with two nodes and one relationship::

    >>> from py2neo import Node, Relationship
    >>> alice = Node("Person", name="Alice")
    >>> bob = Node("Person", name="Bob")
    >>> alice_knows_bob = Relationship(alice, "KNOWS", bob)
    >>> graph.create(alice_knows_bob)

When first created, `Node` and `Relationship` objects exist only in the client; nothing has been
written to the server. The `Graph.create` method shown above creates corresponding server objects
and automatically binds each local object to its remote counterpart. Within py2neo, binding is the
process of applying a URI to a client object, which allows future synchonisation operations to
occur.

.. note:: Entity binding can be managed directly by using the `bind` and `unbind` methods and
    observed through the `bound` boolean property.


Pushing & Pulling
=================

Client-server communication over `REST <http://neo4j.com/docs/2.1.4/rest-api/>`_ can be chatty if
not used sensibly. Whenever possible, py2neo attempts to minimise the amount of chatter between the
client and the server by batching and lazily retrieving data. Most read and write operations are
explicit, allowing the Python application developer a high degree of control over network traffic.

.. note:: Previous versions of py2neo have synchronised data between client and server automatically,
    such as when setting a single property value. Py2neo 2.0 will not carry out updates to client
    or server objects until this is explicitly requested.

To illustrate synchronisation, let's give Alice and Bob an *age* property each. Longhand, this is
done as follows::

    >>> alice.properties["age"] = 33
    >>> bob.properties["age"] = 44
    >>> alice.push()
    >>> bob.push()

Here, we add a new property to each of the two nodes and `push` each in turn, resulting in two
separate HTTP calls being made. These calls can be seen more clearly with the debugging function,
`watch`::

    >>> from py2neo import watch
    >>> watch("httpstream")
    >>> alice.push()
    > POST http://localhost:7474/db/data/batch [146]
    < 200 OK [119]
    >>> bob.push()
    > POST http://localhost:7474/db/data/batch [146]
    < 200 OK [119]

.. note:: The watch function comes with the embedded `httpstream <http://github.com/nigelsmall/httpstream>`_
    library and simply dumps log entries to standard output.

To squash these two separate `push` operations into one, we can use the `Graph.push` method
instead::

    >>> graph.push(alice, bob)
    > POST http://localhost:7474/db/data/batch [289]
    < 200 OK [237]

Not only does this method reduce the activity down to a single HTTP call but it wraps both updates
in a single atomic transaction.

Pulling updates from server to client is similar: either call the `pull` method on an individual
entity or batch together several updates by using `Graph.pull`.


Cypher
======

Single Statements
-----------------

Neo4j has a built-in data query and manipulation language called
`Cypher <http://neo4j.com/guides/basic-cypher/>`_. To execute Cypher from within py2neo, simply use
the `cypher` attribute of a `Graph` instance and call the `execute` method::

    >>> graph.cypher.execute("CREATE (c:Person {name:{N}}) RETURN c", {"N": Carol})
       | c
    ---+----------------------------
     1 | (n2:Person {name:"Carol"})


The object returned from an `execute` call is a `RecordList` which is displayed as a table of
results. A `RecordList` operates like a read-only list object in which each item is a `Record`
instance::

    >>> for record in graph.cypher.execute("CREATE (d:Person {name:'Dave'}) RETURN d"):
    ...     print(record)
    ...
     d
    ---------------------------
     (n3:Person {name:"Dave"})


Each `Record` exposes its values through both named attributes and numeric indexes. Therefore, if a
Cypher query returns a column called `name`, that column can be accessed through the record
attribute called `name`::

    >>> for record in graph.cypher.execute("MATCH (p:Person) RETURN p.name AS name"):
    ...     print(record.name)
    ...
    Alice
    Bob
    Carol
    Dave


Transactions
------------

Neo4j 2.0 extended the REST interface to allow multiple Cypher statements to be sent to the server
as part of a single transaction. To use this endpoint, firstly call the `begin` method on the
graph's `cypher` resource to create a transaction, then use the methods listed below on the
`CypherTransaction` object:

- `enqueue(statement, [parameters])` - add a statement to the queue of statements to be executed (does not get passed to the server immediately)
- `process()` - push all queued statements to the server for execution (returns results from all queued statements)
- `commit()` - commit the transaction (returns results from all queued statements)
- `rollback()` - roll the transaction back

For example::

    >>> tx = graph.cypher.begin()
    >>> statement = "MATCH (a {name:{A}}), (b {name:{B}}) CREATE (a)-[:KNOWS]->(b)"
    >>> for person_a, person_b in [("Alice", "Bob"), ("Bob", "Dave"), ("Alice", "Carol")]:
    ...     tx.enqueue(statement, {"A": person_a, "B": person_b})
    ...
    >>> tx.commit()


Command Line
------------

Py2neo also provides a convenient command line tool for executing Cypher statements::

    $ cypher -p N Alice "MATCH (p:Person {name:{N}}) RETURN p"
       | p
    ---+----------------------------
     1 | (n1:Person {name:"Alice"})


This tool uses the ``NEO4J_URI`` environment variable to determine the location of the underlying
graph database. Support is also provided for a variety of output formats.


Unique Nodes
============

*TODO*


Unique Paths
============

*TODO*
