*********************************************
``py2neo.database.work`` -- Database workflow
*********************************************

.. module:: py2neo.database.work


``Procedure`` objects
=====================

.. autoclass:: Procedure
    :members:


``Transaction`` objects
=======================

.. autoclass:: Transaction(autocommit=False)
   :members:

.. autoexception:: TransactionFinished
   :members:


``Cursor`` objects
==================

.. autoclass:: Cursor
   :members:


``Record`` objects
==================

.. class:: Record(iterable=())

    A :class:`.Record` object holds an ordered, keyed collection of values.
    It is in many ways similar to a `namedtuple` but allows field access only through bracketed syntax and provides more functionality.
    :class:`.Record` extends both :class:`tuple` and :class:`Mapping`.

    .. describe:: record[index]
                  record[key]

        Return the value of *record* with the specified *key* or *index*.

    .. describe:: len(record)

        Return the number of fields in *record*.

    .. describe:: dict(record)

        Return a `dict` representation of *record*.

    .. automethod:: data

    .. automethod:: get

    .. automethod:: index

    .. automethod:: items

    .. automethod:: keys

    .. automethod:: to_subgraph

    .. automethod:: values


``Table`` objects
=================

.. class:: Table(records, keys=None)

    A :class:`.Table` holds a list of :class:`.Record` objects, typically received as the result of a Cypher query.
    It provides a convenient container for working with a result in its entirety and provides methods for conversion into various output formats.
    :class:`.Table` extends ``list``.

    .. describe:: repr(table)

        Return a string containing an ASCII art representation of this table.
        Internally, this method calls :meth:`.write` with `header=True`, writing the output into an ``io.StringIO`` instance.

    .. automethod:: _repr_html_()

    .. automethod:: keys

    .. automethod:: field

    .. automethod:: write

    .. automethod:: write_html

    .. automethod:: write_separated_values

    .. automethod:: write_csv

    .. automethod:: write_tsv

.. autoclass:: CypherSummary
   :members:

.. autoclass:: CypherStats
   :members:

.. autoclass:: CypherPlan
   :members:


Errors & Warnings
=================

.. autoclass:: GraphError
   :members:

.. autoclass:: ClientError
   :members:

.. autoclass:: DatabaseError
   :members:

.. autoclass:: TransientError
   :members:
