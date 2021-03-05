****************************************************
``py2neo.pep249`` -- Python DB API 2.0 Compatibility
****************************************************

.. automodule:: py2neo.pep249


Module attributes and functions
===============================

.. autoattribute:: py2neo.pep249::apilevel

    This module supports version 2.0 of the DB API.

.. autoattribute:: py2neo.pep249::threadsafety

    This module is not guaranteed to be thread-safe.

.. autoattribute:: py2neo.pep249::paramstyle

    This module uses the Cypher query language parameter notation.
    Note that this is not a standard setting according to PEP 249.

.. autofunction:: py2neo.pep249::connect


Connection objects
==================

.. autoclass:: Connection

    .. autoattribute:: in_transaction

    .. automethod:: cursor

    .. automethod:: execute

    .. automethod:: executemany

    .. automethod:: begin

    .. automethod:: commit

    .. automethod:: rollback

    .. automethod:: close


Cursor objects
==============

.. autoclass:: Cursor

    .. autoattribute:: arraysize

    .. autoattribute:: connection

    .. autoattribute:: description

    .. autoattribute:: rowcount

    .. autoattribute:: summary

    .. automethod:: execute

    .. automethod:: executemany

    .. automethod:: fetchone

    .. automethod:: fetchmany

    .. automethod:: fetchall

    .. automethod:: close


Exceptions
==========

.. autoexception:: Warning

.. autoexception:: Error

.. autoexception:: InterfaceError

.. autoexception:: DatabaseError

.. autoexception:: DataError

.. autoexception:: OperationalError

.. autoexception:: IntegrityError

.. autoexception:: InternalError

.. autoexception:: ProgrammingError

.. autoexception:: NotSupportedError
