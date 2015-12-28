===================
Working with Cypher
===================

Py2neo provides Cypher execution functionality via the HTTP transactional endpoint or, if unavailable, the legacy endpoint.
This is typically accessed via the :attr:`py2neo.Graph.cypher` attribute from where transactions may be created and simple execution may be carried out.
Parameterised statements are fully supported and on top of this, an extra layer of client-side presubstitution is available.
Such presubstitution is useful for parameterisation of relationship types, node labels and property keys.
`Angled quotation marks <https://en.wikipedia.org/wiki/Guillemet#Typing_.22.C2.AB.22_and_.22.C2.BB.22_on_computers>`_ are used for delimiting parameter keys and can be mixed with regular parameters::

    from py2neo import Graph
    graph = Graph()
    cypher = graph.cypher
    cypher.execute("CREATE (a {name:{a}})-[:«rel»]->(b:«labels» {name:{b}})",
                   a="Alice", rel="KNOWS", labels=["Human Being", "Employee"], b="Bob")

Each presubstitution parameter will be correctly escaped within backticks if necessary and collections passed as a single parameter will be joined by colons.
Therefore, the code above is equivalent to::

    from py2neo import Graph
    graph = Graph()
    cypher = graph.cypher
    cypher.execute("CREATE (a {name:{a}})-[:KNOWS]->(b:`Human Being`:Employee {name:{b}})",
                   a="Alice", b="Bob")

Integer parameters will be substituted directly, without escaping, and integer 2-tuples will be assumed to denote a range::

    from py2neo import Graph
    graph = Graph()
    cypher = graph.cypher
    cypher.execute("MATCH (a)-[:KNOWS*«r1»]->(b)-[:KNOWS*«r2»]->(c)", r1=1, r2=(3, 5))


Cypher Resource
===============

.. autoclass:: py2neo.cypher.CypherEngine
   :members:


Transactions
============

.. autoclass:: py2neo.cypher.Transaction
   :members:


Records
=======

.. autoclass:: py2neo.cypher.Record
   :members:

.. autoclass:: py2neo.cypher.Cursor
   :members:



Builders
========

.. autoclass:: py2neo.cypher.CypherWriter
   :members:

.. autofunction:: py2neo.cypher.cypher_escape

.. autofunction:: py2neo.cypher.cypher_repr


Exceptions
==========

.. autoexception:: py2neo.status.CypherError
   :members:


Client Errors
-------------

.. autoexception:: py2neo.status.ClientError
   :members:

.. autoexception:: py2neo.status.request.Invalid
   :members:

.. autoexception:: py2neo.status.request.InvalidFormat
   :members:

.. autoexception:: py2neo.status.schema.ConstraintAlreadyExists
   :members:

.. autoexception:: py2neo.status.schema.ConstraintVerificationFailure
   :members:

.. autoexception:: py2neo.status.schema.ConstraintViolation
   :members:

.. autoexception:: py2neo.status.schema.IllegalTokenName
   :members:

.. autoexception:: py2neo.status.schema.IndexAlreadyExists
   :members:

.. autoexception:: py2neo.status.schema.IndexBelongsToConstraint
   :members:

.. autoexception:: py2neo.status.schema.LabelLimitReached
   :members:

.. autoexception:: py2neo.status.schema.NoSuchConstraint
   :members:

.. autoexception:: py2neo.status.schema.NoSuchIndex
   :members:

.. autoexception:: py2neo.status.statement.ArithmeticError
    :members:

.. autoexception:: py2neo.status.statement.ConstraintViolation
    :members:

.. autoexception:: py2neo.status.statement.EntityNotFound
    :members:

.. autoexception:: py2neo.status.statement.InvalidArguments
    :members:

.. autoexception:: py2neo.status.statement.InvalidSemantics
    :members:

.. autoexception:: py2neo.status.statement.InvalidSyntax
    :members:

.. autoexception:: py2neo.status.statement.InvalidType
    :members:

.. autoexception:: py2neo.status.statement.NoSuchLabel
    :members:

.. autoexception:: py2neo.status.statement.NoSuchProperty
    :members:

.. autoexception:: py2neo.status.statement.ParameterMissing
    :members:

.. autoexception:: py2neo.status.transaction.ConcurrentRequest
    :members:

.. autoexception:: py2neo.status.transaction.EventHandlerThrewException
    :members:

.. autoexception:: py2neo.status.transaction.InvalidType
    :members:

.. autoexception:: py2neo.status.transaction.UnknownId
    :members:


Database Errors
---------------

.. autoexception:: py2neo.status.DatabaseError
   :members:

.. autoexception:: py2neo.status.schema.ConstraintCreationFailure
    :members:

.. autoexception:: py2neo.status.schema.ConstraintDropFailure
    :members:

.. autoexception:: py2neo.status.schema.IndexCreationFailure
    :members:

.. autoexception:: py2neo.status.schema.IndexDropFailure
    :members:

.. autoexception:: py2neo.status.schema.NoSuchLabel
    :members:

.. autoexception:: py2neo.status.schema.NoSuchPropertyKey
    :members:

.. autoexception:: py2neo.status.schema.NoSuchRelationshipType
    :members:

.. autoexception:: py2neo.status.schema.NoSuchSchemaRule
    :members:

.. autoexception:: py2neo.status.statement.ExecutionFailure
    :members:

.. autoexception:: py2neo.status.transaction.CouldNotBegin
    :members:

.. autoexception:: py2neo.status.transaction.CouldNotCommit
    :members:

.. autoexception:: py2neo.status.transaction.CouldNotRollback
    :members:

.. autoexception:: py2neo.status.transaction.ReleaseLocksFailed
    :members:


Transient Errors
----------------

.. autoexception:: py2neo.status.TransientError
   :members:

.. autoexception:: py2neo.status.network.UnknownFailure
   :members:

.. autoexception:: py2neo.status.statement.ExternalResourceFailure
    :members:

.. autoexception:: py2neo.status.transaction.AcquireLockTimeout
    :members:

.. autoexception:: py2neo.status.transaction.DeadlockDetected
    :members:


Command Line
============

Py2neo also provides a command line tool for executing Cypher queries, simply called `cypher`. To
use this, pass the Cypher statement as a string argument::

    $ cypher "CREATE (a:Person {name:'Alice'}) RETURN a"
       | a
    ---+------------------------------
     1 | (n1:Person {name:"Alice"})

Parameters can be supplied as command line options::

    $ cypher -p name Bob "CREATE (a:Person {name:{name}}) RETURN a"
       | a
    ---+------------------------------
     1 | (n2:Person {name:"Bob"})

Alternatively, to pass multiple sets of parameters, a separate parameter file can be used::

    $ cypher -f params.csv "CREATE (a:Person {name:{name},age:{age}}) RETURN a.name, a.age"
       | a.name | a.age
    ---+--------+-------
     1 | Carol  |    55

       | a.name | a.age
    ---+--------+-------
     1 | Dave   |    66

       | a.name | a.age
    ---+--------+-------
     1 | Ethel  |    77

       | a.name | a.age
    ---+--------+-------
     1 | Frank  |    88

The parameter file must be in CSV format with each value encoded as JSON. String values must
therefore be enclosed in double quotes. For the example above, the file would look like this::

    name,age
    "Carol",55
    "Dave",66
    "Ethel",77
    "Frank",88

By default, statements will be executed against the server at ``http://localhost:7474/db/data/``.
To change the database used, set the ``NEO4J_URI`` environment variable prior to execution.

As well as plain text, there are several other formats available for query output. The full list
of formats is below.

- Comma-separated values (``-c``, ``--csv``)
- Geoff (``-g``, ``--geoff``)
- Human-readable text (``-h``, ``--human``)
- JSON (``-j``, ``--json``)
- Tab-separated values (``-t``, ``--tsv``)
