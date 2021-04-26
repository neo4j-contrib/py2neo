********
Workflow
********

.. module:: py2neo
.. module:: py2neo.database


``GraphService`` objects
------------------------

.. autoclass:: py2neo.GraphService
    :members:

``Graph`` objects
-----------------

.. autoclass:: py2neo.Graph
    :members:

``SystemGraph`` objects
-----------------------

.. autoclass:: py2neo.SystemGraph
    :members:

``Schema`` objects
------------------

.. autoclass:: py2neo.Schema
    :members:


``Transaction`` objects
-----------------------

.. autoclass:: py2neo.Transaction(manager, autocommit=False, readonly=False)

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
