********************************************
``py2neo`` -- Core Connectivity and Workflow
********************************************

.. automodule:: py2neo


Getting connected
=================

The :class:`.GraphService`, :class:`.Graph`, and :class:`.SystemGraph`
classes all accept an argument called `profile` plus individual keyword
`settings`. Internally, these arguments are used to construct a
:class:`.ConnectionProfile` object which holds these details.

The `profile` can either be a URI or a base :class:`.ConnectionProfile`
object. The `settings` are individual overrides for the values within
that, such as ``host`` or ``password``. This override mechanism allows
several ways of specifying the same information. For example, the three
variants below are all equivalent::

    >>> from py2neo import Graph
    >>> graph_1 = Graph()
    >>> graph_2 = Graph(host="localhost")
    >>> graph_3 = Graph("bolt://localhost:7687")

Omitting the `profile` argument completely falls back to using the
default :class:`.ConnectionProfile`. More on this, and other useful
information, can be found in the documentation for that class.

URIs
----

The general format of a URI is ``<scheme>://[<user>[:<password>]@]<host>[:<port>]``.
Supported URI schemes are:

- ``bolt`` - Bolt (unsecured)
- ``bolt+s`` - Bolt (secured with full certificate checks)
- ``bolt+ssc`` - Bolt (secured with no certificate checks)
- ``http`` - HTTP (unsecured)
- ``https`` - HTTP (secured with full certificate checks)
- ``http+s`` - HTTP (secured with full certificate checks)
- ``http+ssc`` - HTTP (secured with no certificate checks)


Note that py2neo does not support routing URIs like ``neo4j://...``
for use with Neo4j causal clusters. To enable routing, instead pass
a ``routing=True`` keyword argument to the :class:`.Graph` or
:class:`.GraphService` constructor.

Routing is only available for Bolt-enabled servers. No equivalent
currently exists for HTTP.

Individual settings
-------------------

The full set of supported `settings` are:

============  =========================================  =====  =========================
Keyword       Description                                Type   Default
============  =========================================  =====  =========================
``scheme``    Use a specific URI scheme                  str    ``'bolt'``
``secure``    Use a secure connection (TLS)              bool   ``False``
``verify``    Verify the server certificate (if secure)  bool   ``True``
``host``      Database server host name                  str    ``'localhost'``
``port``      Database server port                       int    ``7687``
``address``   Colon-separated host and port string       str    ``'localhost:7687'``
``user``      User to authenticate as                    str    ``'neo4j'``
``password``  Password to use for authentication         str    ``'password'``
``auth``      A 2-tuple of (user, password)              tuple  ``('neo4j', 'password')``
``routing``   Route connections across multiple servers  bool   ``False``
============  =========================================  =====  =========================

``ConnectionProfile`` objects
-----------------------------

.. autoclass:: ConnectionProfile
    :members:


Graph Databases
===============

``GraphService`` objects
------------------------

.. autoclass:: GraphService
    :members:

``Graph`` objects
-----------------

.. autoclass:: Graph
    :members:

``SystemGraph`` objects
-----------------------

.. autoclass:: SystemGraph
    :members:

``Schema`` objects
------------------

.. autoclass:: Schema
    :members:

Running procedures
------------------

.. autoclass:: ProcedureLibrary
    :members:

.. autoclass:: Procedure
    :members:


Transactions
============

``Transaction`` objects
-----------------------

.. autoclass:: Transaction(manager, autocommit=False, readonly=False)

    .. autoattribute:: graph

    .. autoattribute:: readonly

    .. raw:: html

        <h3>Cypher execution</h3>

    The :meth:`.run`, :meth:`.evaluate` and :meth:`.update` methods are
    used to execute Cypher queries within the transactional context.
    Each is intended for use with a particular kind of query:
    :meth:`.run` for general-purpose query execution, :meth:`.evaluate`
    for retrieving single aggregation values and :meth:`.update` for
    executing Cypher that has no return value.

    .. automethod:: evaluate

    .. automethod:: run

    .. automethod:: update

    .. raw:: html

        <h3>Subgraph operations</h3>

    The methods below all operate on :class:`.Subgraph` objects, such
    as nodes and relationships.

    .. automethod:: create

    .. automethod:: delete

    .. automethod:: exists

    .. automethod:: merge

    .. automethod:: pull

    .. automethod:: push

    .. automethod:: separate

    .. raw:: html

        <h3>Deprecated methods</h3>

    The :meth:`.commit` and :meth:`.rollback` methods are deprecated.
    Instead, the similarly-named methods on the parent :class:`.Graph`
    should be used, with this transaction as an argument.

    .. automethod:: commit

    .. automethod:: rollback

.. autoclass:: TransactionSummary
   :members:

``Cursor`` objects
------------------

.. autoclass:: Cursor
   :members:


``Record`` objects
------------------

.. autoclass:: Record
    :members:
