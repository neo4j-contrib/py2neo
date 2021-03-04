#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2021, Nigel Small
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


"""
This module provides an implementation of the Python Database API v2.0
(as specified in PEP 249) for Neo4j. The classes here are thin wrappers
over the regular py2neo functionality, so will provide a very similar
behaviour and experience to the rest of the library.

To get started, create a :class:`Connection` object using the
:func:`connect` function::

>>> from py2neo.pep249 import connect
>>> con = connect()

The arguments accepted by this function are identical to those accepted
by the :class:`.Graph` class in the core API. Therefore, a URI and any
combination of individual settings may be specified.

The :class:`Connection` object represents a single
client-server connection and can maintain a single transaction at any
point in time. Transactions are implicitly created through using a
method like :meth:`Cursor.execute` or can be explicitly
created using the :meth:`Connection.begin` method.

"""


from six import raise_from

from py2neo.client import (
    Connection as _Connection,
    ConnectionProfile as _ConnectionProfile,
    ConnectionUnavailable as _ConnectionUnavailable,
)


apilevel = "2.0"
threadsafety = 0        # TODO
paramstyle = "cypher"   # TODO


class Connection(object):
    """ PEP249-compliant connection to a Neo4j server.
    """

    def __init__(self, profile=None, **settings):
        profile = _ConnectionProfile(profile, **settings)
        try:
            self._cx = _Connection.open(profile)
        except _ConnectionUnavailable as error:
            raise_from(OperationalError("Connection unavailable"), error)
        self._tx = None
        self._db = None

    def __check__(self):
        if self._cx is None or self._cx.closed:
            raise ProgrammingError("Connection is closed")
        if self._cx.broken:
            raise OperationalError("Connection is broken")

    def __execute__(self, query, parameters=None):
        result = self._cx.run_query(self._tx, query, parameters)
        self._cx.pull(result)
        self._cx.sync(result)
        return result

    def begin(self):
        """ Begin a transaction.
        """
        self.rollback()
        self._tx = self._cx.begin(self._db)

    def commit(self):
        """ Commit any pending transaction to the database.
        """
        self.__check__()
        if self._tx is not None:
            self._cx.commit(self._tx)
            self._tx = None

    def rollback(self):
        """ Rollback any pending transaction.
        """
        self.__check__()
        if self._tx is not None:
            self._cx.rollback(self._tx)
            self._tx = None

    @property
    def in_transaction(self):
        """ True if a transaction is active, False otherwise.
        """
        return self._tx is not None

    def cursor(self):
        """ Construct a new :class:`.Cursor` object for this connection.
        """
        self.__check__()
        return Cursor(self)

    def execute(self, query, parameters=None):
        """ Execute a query on this connection.
        """
        self.__check__()
        cursor = self.cursor()
        cursor.execute(query, parameters)
        return cursor

    def executemany(self, query, seq_of_parameters):
        """ Execute a query on this connection once for each parameter
        set.
        """
        self.__check__()
        cursor = self.cursor()
        cursor.executemany(query, seq_of_parameters)

    def close(self):
        """ Close the connection now.

        The connection will be unusable from this point forward; an
        :class:`.Error` exception will be raised if any operation is
        attempted with the connection. The same applies to all cursor
        objects trying to use the connection. Note that closing a
        connection without committing the changes first will cause an
        implicit rollback to be performed.
        """
        if self._cx is not None and not self._cx.closed and not self._cx.broken:
            self.rollback()
            self._cx.close()
            self._cx = None


class Cursor(object):
    """ PEP249-compliant cursor attached to a Neo4j server.
    """

    arraysize = 1

    def __init__(self, connection):
        self._connection = connection
        self._result = None
        self._closed = False

    def __iter__(self):
        self.__check__()
        if self._result is None:
            return
        while True:
            record = self._result.fetch()
            if record is None:
                break
            yield tuple(record)

    def __check__(self):
        if self._closed:
            raise ProgrammingError("Cursor is closed")
        self.connection.__check__()

    @property
    def connection(self):
        """ Connection to which this cursor is bound.
        """
        return self._connection

    @property
    def description(self):
        """ Field details, each represented as a 7-tuple.
        """
        if self._result is None:
            return None
        return [(name, None, None, None, None, None, None)
                for name in self._result.fields()]

    @property
    def rowcount(self):
        """ Number of rows affected by the last query executed.
        """
        return -1

    @property
    def summary(self):
        """ Dictionary of summary information relating to the last
        query executed.
        """
        if self._result is None:
            return None
        return self._result.summary()

    def close(self):
        """ Close the cursor.
        """
        if not self._closed:
            if self._result is not None:
                self._result.buffer()
            self._closed = True

    def execute(self, query, parameters=None):
        """ Execute a query.
        """
        self.__check__()
        if self._result is not None:
            self._result.buffer()
        if not self.connection.in_transaction:
            self.connection.begin()
        self._result = self.connection.__execute__(query, parameters)
        return self

    def executemany(self, query, seq_of_parameters):
        """ Execute query multiple times with different parameters sets.
        """
        self.__check__()
        for parameters in seq_of_parameters:
            self.execute(query, parameters)

    def fetchone(self):
        """

        :return:
        """
        self.__check__()
        if self._result is None:
            return None
        record = self._result.fetch()
        if record is None:
            return None
        return tuple(record)

    def fetchmany(self, size=None):
        """

        :param size:
        :return:
        """
        self.__check__()
        if self._result is None:
            return []
        if size is None:
            size = self.arraysize
        records = []
        for _ in range(size):
            record = self._result.fetch()
            if record is None:
                break
            records.append(tuple(record))
        return records

    def fetchall(self):
        """

        :return:
        """
        self.__check__()
        if self._result is None:
            return []
        records = []
        while True:
            record = self._result.fetch()
            if record is None:
                break
            records.append(tuple(record))
        return records

    def setinputsizes(self, sizes):
        """

        :param sizes:
        :return:
        """

    def setoutputsize(self, size, column=None):
        """

        :param size:
        :param column:
        :return:
        """


def connect(profile=None, **settings):
    """ Constructor for creating a connection to the database.
    """
    return Connection(profile, **settings)


# noinspection PyShadowingBuiltins
class Warning(Exception):
    """ Exception raised for important warnings like data truncations
    while inserting, etc.
    """


class Error(Exception):
    """ Exception that is the base class of all other error exceptions.
    You can use this to catch all errors with one single except
    statement. Warnings are not considered errors and thus should not
    use this class as base.
    """


class InterfaceError(Error):
    """ Exception raised for errors that are related to the database
    interface rather than the database itself.
    """


class DatabaseError(Error):
    """ Exception raised for errors that are related to the database.
    """


class DataError(DatabaseError):
    """ Exception raised for errors that are due to problems with the
    processed data like division by zero, numeric value out of range,
    etc.
    """


class OperationalError(DatabaseError):
    """ Exception raised for errors that are related to the database's
    operation and not necessarily under the control of the programmer,
    e.g. an unexpected disconnect occurs, the data source name is not
    found, a transaction could not be processed, a memory allocation
    error occurred during processing, etc.
    """


class IntegrityError(DatabaseError):
    """ Exception raised when the relational integrity of the database
    is affected, e.g. a foreign key check fails.
    """


class InternalError(DatabaseError):
    """ Exception raised when the database encounters an internal
    error, e.g. the cursor is not valid anymore, the transaction is
    out of sync, etc.
    """


class ProgrammingError(DatabaseError):
    """ Exception raised for programming errors, e.g. table not found
    or already exists, syntax error in the SQL statement, wrong number
    of parameters specified, etc.
    """


class NotSupportedError(DatabaseError):
    """ Exception raised in case a method or database API was used
    which is not supported by the database, e.g. requesting a
    .rollback() on a connection that does not support transaction or
    has transactions turned off.
    """
