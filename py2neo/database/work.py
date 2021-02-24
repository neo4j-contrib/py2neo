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
The ``py2neo.database.work`` package contains classes pertaining to the
execution of Cypher queries and transactions.
"""

from __future__ import absolute_import, print_function, unicode_literals

from collections import OrderedDict
from functools import reduce
from io import StringIO
from operator import xor as xor_operator
from warnings import warn

from py2neo.compat import Mapping, numeric_types, ustr, xstr, deprecated
from py2neo.cypher import cypher_repr, cypher_str


class TransactionManager(object):
    """ Transaction manager.

    *New in version 2021.1.*
    """

    def __init__(self, graph):
        self.graph = graph
        self._connector = self.graph.service.connector

    def auto(self, readonly=False,
             # after=None, metadata=None, timeout=None
             ):
        """ Create a new auto-commit :class:`~py2neo.database.work.Transaction`.

        :param readonly: if :py:const:`True`, will begin a readonly
            transaction, otherwise will begin as read-write
        """
        return Transaction(self, autocommit=True, readonly=readonly,
                           # after, metadata, timeout
                           )

    def begin(self, readonly=False,
              # after=None, metadata=None, timeout=None
              ):
        """ Begin a new :class:`~py2neo.database.work.Transaction`.

        :param readonly: if :py:const:`True`, will begin a readonly
            transaction, otherwise will begin as read-write
        :returns: new :class:`~py2neo.database.work.Transaction`.
            object
        """
        return Transaction(self, autocommit=False, readonly=readonly,
                           # after, metadata, timeout
                           )

    def commit(self, tx):
        """ Commit the transaction.

        :returns: :class:`.TransactionSummary` object
        """
        if not isinstance(tx, Transaction):
            raise TypeError("Bad transaction %r" % tx)
        if tx._finished:
            raise TypeError("Cannot commit finished transaction")
        try:
            summary = self._connector.commit(tx._transaction)
            return TransactionSummary(**summary)
        finally:
            tx._finished = True

    def rollback(self, tx):
        """ Roll back the current transaction, undoing all actions
        previously taken.

        :returns: :class:`.TransactionSummary` object
        """
        if not isinstance(tx, Transaction):
            raise TypeError("Bad transaction %r" % tx)
        if tx._finished:
            raise TypeError("Cannot rollback finished transaction")
        try:
            summary = self._connector.rollback(tx._transaction)
            return TransactionSummary(**summary)
        finally:
            tx._finished = True


class Transaction(object):
    """ Logical context for one or more graph operations.

    Transaction objects are typically constructed by the
    :meth:`.Graph.auto` and :meth:`.Graph.begin` methods. User
    applications should not generally need to create these objects
    directly.

    Each transaction has a lifetime which ends by a call to either
    :meth:`.commit` or :meth:`.rollback`. In the case of an error, the
    server can also prematurely end transactions. The
    :meth:`.finished` method can be used to determine whether or not
    any of these cases have occurred.

    The :meth:`.run` and :meth:`.evaluate` methods are used to execute
    Cypher queries within the transactional context. The remaining
    methods operate on :class:`.Subgraph` objects, including
    derivatives such as :class:`.Node` and :class:`.Relationship`.
    """

    _finished = False

    def __init__(self, manager, autocommit=False, readonly=False,
                 # after=None, metadata=None, timeout=None
                 ):
        self._tx_manager = manager
        self._autocommit = autocommit
        self._connector = self._tx_manager.graph.service.connector
        if autocommit:
            self._transaction = None
        else:
            self._transaction = self._connector.begin(self._tx_manager.graph.name, readonly=readonly,
                                                      # after, metadata, timeout
                                                      )
        self._readonly = readonly

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self._tx_manager.commit(self)
        else:
            self._tx_manager.rollback(self)

    @property
    def graph(self):
        return self._tx_manager.graph

    @property
    def readonly(self):
        return self._readonly

    def run(self, cypher, parameters=None, **kwparameters):
        """ Send a Cypher query to the server for execution and return
        a :py:class:`.Cursor` for navigating its result.

        :param cypher: Cypher query
        :param parameters: dictionary of parameters
        :returns: :py:class:`.Cursor` object
        """
        from py2neo.client import Connection
        if self._finished:
            raise TypeError("Cannot run query in finished transaction")

        try:
            hydrant = Connection.default_hydrant(self._connector.profile, self.graph)
            parameters = dict(parameters or {}, **kwparameters)
            if self._transaction:
                result = self._connector.run_in_tx(self._transaction, cypher, parameters)
            else:
                result = self._connector.auto_run(self.graph.name, cypher, parameters,
                                                  readonly=self.readonly)
            return Cursor(result, hydrant)
        finally:
            if not self._transaction:
                self._finished = True

    def evaluate(self, cypher, parameters=None, **kwparameters):
        """ Execute a single Cypher statement and return the value from
        the first column of the first record.

        :param cypher: Cypher statement
        :param parameters: dictionary of parameters
        :returns: single return value or :const:`None`
        """
        return self.run(cypher, parameters, **kwparameters).evaluate(0)

    def update(self, cypher, parameters=None, **kwparameters):
        """ Execute a single Cypher statement and discard any result
        returned.

        :param cypher: Cypher statement
        :param parameters: dictionary of parameters
        """
        self.run(cypher, parameters, **kwparameters).close()

    @deprecated("The transaction.commit() method is deprecated, "
                "use graph.commit(transaction) instead")
    def commit(self):
        """ Commit the transaction.

        :returns: :class:`.TransactionSummary` object
        """
        return self._tx_manager.commit(self)

    @deprecated("The transaction.rollback() method is deprecated, "
                "use graph.rollback(transaction) instead")
    def rollback(self):
        """ Roll back the current transaction, undoing all actions
        previously taken.

        :returns: :class:`.TransactionSummary` object
        """
        return self._tx_manager.rollback(self)

    def create(self, subgraph):
        """ Create remote nodes and relationships that correspond to those in a
        local subgraph. Any entities in *subgraph* that are already bound to
        remote entities will remain unchanged, those which are not will become
        bound to their newly-created counterparts.

        For example::

            >>> from py2neo import Graph, Node, Relationship
            >>> g = Graph()
            >>> tx = g.begin()
            >>> a = Node("Person", name="Alice")
            >>> tx.create(a)
            >>> b = Node("Person", name="Bob")
            >>> ab = Relationship(a, "KNOWS", b)
            >>> tx.create(ab)
            >>> tx.commit()
            >>> g.exists(ab)
            True

        :param subgraph: a :class:`.Node`, :class:`.Relationship` or other
                    creatable object
        """
        try:
            create = subgraph.__db_create__
        except AttributeError:
            raise TypeError("No method defined to create object %r" % subgraph)
        else:
            create(self)

    def delete(self, subgraph):
        """ Delete the remote nodes and relationships that correspond to
        those in a local subgraph. To delete only the relationships, use
        the :meth:`.separate` method.

        :param subgraph: a :class:`.Node`, :class:`.Relationship` or other
                       :class:`.Subgraph`
        """
        try:
            delete = subgraph.__db_delete__
        except AttributeError:
            raise TypeError("No method defined to delete object %r" % subgraph)
        else:
            delete(self)

    def exists(self, subgraph):
        """ Determine whether one or more entities all exist within the
        graph. Note that if any nodes or relationships in *subgraph* are not
        bound to remote counterparts, this method will return ``False``.

        :param subgraph: a :class:`.Node`, :class:`.Relationship` or other
                       :class:`.Subgraph`
        :returns: ``True`` if all entities exist remotely, ``False`` otherwise
        """
        try:
            exists = subgraph.__db_exists__
        except AttributeError:
            raise TypeError("No method defined to check existence of object %r" % subgraph)
        else:
            return exists(self)

    def merge(self, subgraph, primary_label=None, primary_key=None):
        """ Create or update the nodes and relationships of a local
        subgraph in the remote database. Note that the functionality of
        this operation is not strictly identical to the Cypher MERGE
        clause, although there is some overlap.

        Each node and relationship in the local subgraph is merged
        independently, with nodes merged first and relationships merged
        second.

        For each node, the merge is carried out by comparing that node with
        a potential remote equivalent on the basis of a single label and
        property value. If no remote match is found, a new node is created;
        if a match is found, the labels and properties of the remote node
        are updated. The label and property used for comparison are determined
        by the `primary_label` and `primary_key` arguments but may be
        overridden for individual nodes by the of `__primarylabel__` and
        `__primarykey__` attributes on the node itself.

        For each relationship, the merge is carried out by comparing that
        relationship with a potential remote equivalent on the basis of matching
        start and end nodes plus relationship type. If no remote match is found,
        a new relationship is created; if a match is found, the properties of
        the remote relationship are updated.

        :param subgraph: a :class:`.Node`, :class:`.Relationship` or other
                       :class:`.Subgraph` object
        :param primary_label: label on which to match any existing nodes
        :param primary_key: property key(s) on which to match any existing
                            nodes
        """
        try:
            merge = subgraph.__db_merge__
        except AttributeError:
            raise TypeError("No method defined to merge object %r" % subgraph)
        else:
            merge(self, primary_label, primary_key)

    def pull(self, subgraph):
        """ Update local entities from their remote counterparts.

        For any nodes and relationships that exist in both the local
        :class:`.Subgraph` and the remote :class:`.Graph`, pull properties
        and node labels into the local copies. This operation does not
        create or delete any entities.

        :param subgraph: a :class:`.Node`, :class:`.Relationship` or other
                       :class:`.Subgraph`
        """
        try:
            pull = subgraph.__db_pull__
        except AttributeError:
            raise TypeError("No method defined to pull object %r" % subgraph)
        else:
            return pull(self)

    def push(self, subgraph):
        """ Update remote entities from their local counterparts.

        For any nodes and relationships that exist in both the local
        :class:`.Subgraph` and the remote :class:`.Graph`, push properties
        and node labels into the remote copies. This operation does not
        create or delete any entities.

        :param subgraph: a :class:`.Node`, :class:`.Relationship` or other
                       :class:`.Subgraph`
        """
        try:
            push = subgraph.__db_push__
        except AttributeError:
            raise TypeError("No method defined to push object %r" % subgraph)
        else:
            return push(self)

    def separate(self, subgraph):
        """ Delete the remote relationships that correspond to those in a local
        subgraph. This leaves any nodes untouched.

        :param subgraph: a :class:`.Node`, :class:`.Relationship` or other
                       :class:`.Subgraph`
        """
        try:
            separate = subgraph.__db_separate__
        except AttributeError:
            raise TypeError("No method defined to separate object %r" % subgraph)
        else:
            separate(self)


class TransactionSummary(object):
    """ Summary information produced as the result of a
    :class:`.Transaction` commit or rollback.
    """

    def __init__(self, bookmark=None, profile=None, time=None):
        self.bookmark = bookmark
        self.profile = profile
        self.time = time


class Cursor(object):
    """ A `Cursor` is a navigator for a stream of records.

    A cursor can be thought of as a window onto an underlying data
    stream. All cursors in py2neo are "forward-only", meaning that
    navigation starts before the first record and may proceed only in a
    forward direction.

    It is not generally necessary for application code to instantiate a
    cursor directly as one will be returned by any Cypher execution method.
    However, cursor creation requires only a :class:`.DataSource` object
    which contains the logic for how to access the source data that the
    cursor navigates.

    Many simple cursor use cases require only the :meth:`.forward` method
    and the :attr:`.current` attribute. To navigate through all available
    records, a `while` loop can be used::

        while cursor.forward():
            print(cursor.current["name"])

    If only the first record is of interest, a similar `if` structure will
    do the job::

        if cursor.forward():
            print(cursor.current["name"])

    To combine `forward` and `current` into a single step, use the built-in
    py:func:`next` function::

        print(next(cursor)["name"])

    Cursors are also iterable, so can be used in a loop::

        for record in cursor:
            print(record["name"])

    For queries that are expected to return only a single value within a
    single record, use the :meth:`.evaluate` method. This will return the
    first value from the next record or :py:const:`None` if neither the
    field nor the record are present::

        print(cursor.evaluate())

    """

    def __init__(self, result, hydrant=None):
        self._result = result
        self._fields = self._result.fields()
        self._hydrant = hydrant
        self._current = None
        self._closed = False

    def __repr__(self):
        preview = self.preview(3)
        if preview:
            return repr(preview)
        else:
            return "(No data)"

    def __next__(self):
        if self.forward():
            return self._current
        else:
            raise StopIteration()

    # Exists only for Python 2 iteration compatibility
    next = __next__

    def __iter__(self):
        while self.forward():
            yield self._current

    def __getitem__(self, key):
        return self._current[key]

    @property
    def current(self):
        """ Returns the current record or :py:const:`None` if no record
        has yet been selected.
        """
        return self._current

    def close(self):
        """ Close this cursor and free up all associated resources.
        """
        if not self._closed:
            self._result.buffer()   # force consumption of remaining data
            self._closed = True

    def keys(self):
        """ Return the field names for the records in the stream.
        """
        return self._fields

    def summary(self):
        """ Return the result summary.
        """
        self._result.buffer()
        metadata = self._result.summary()
        return CypherSummary(**metadata)

    def plan(self):
        """ Return the plan returned with this result, if any.
        """
        self._result.buffer()
        metadata = self._result.summary()
        if "plan" in metadata:
            return CypherPlan(**metadata["plan"])
        elif "profile" in metadata:
            return CypherPlan(**metadata["profile"])
        else:
            return None

    def stats(self):
        """ Return the query statistics.

        This contains details of the activity undertaken by the database
        kernel for the query, such as the number of entities created or
        deleted. Specifically, this returns a :class:`.CypherStats` object.

        >>> from py2neo import Graph
        >>> g = Graph()
        >>> g.run("CREATE (a:Person) SET a.name = 'Alice'").stats()
        constraints_added: 0
        constraints_removed: 0
        contained_updates: True
        indexes_added: 0
        indexes_removed: 0
        labels_added: 1
        labels_removed: 0
        nodes_created: 1
        nodes_deleted: 0
        properties_set: 1
        relationships_created: 0
        relationships_deleted: 0

        """
        self._result.buffer()
        metadata = self._result.summary()
        return CypherStats(**metadata.get("stats", {}))

    def forward(self, amount=1):
        """ Attempt to move the cursor one position forward (or by
        another amount if explicitly specified). The cursor will move
        position by up to, but never more than, the amount specified.
        If not enough scope for movement remains, only that remainder
        will be consumed. The total amount moved is returned.

        :param amount: the amount to move the cursor
        :returns: the amount that the cursor was able to move
        """
        if amount == 0:
            return 0
        if amount < 0:
            raise ValueError("Cursor can only move forwards")
        amount = int(amount)
        moved = 0
        while moved != amount:
            values = self._result.fetch()
            if values is None:
                break
            if self._hydrant:
                values = self._hydrant.hydrate_list(values)
            self._current = Record(self._fields, values)
            moved += 1
        return moved

    def preview(self, limit=1):
        """ Construct a :class:`.Table` containing a preview of
        upcoming records, including no more than the given `limit`.

        :param limit: maximum number of records to include in the
            preview
        :returns: :class:`.Table` containing the previewed records
        """
        if limit < 0:
            raise ValueError("Illegal preview size")
        records = []
        if self._fields:
            for values in self._result.peek_records(int(limit)):
                if self._hydrant:
                    values = self._hydrant.hydrate_list(values)
                records.append(values)
            return Table(records, self._fields)
        else:
            return None

    def evaluate(self, field=0):
        """ Return the value of the first field from the next record
        (or the value of another field if explicitly specified).

        This method attempts to move the cursor one step forward and,
        if successful, selects and returns an individual value from
        the new current record. By default, this value will be taken
        from the first value in that record but this can be overridden
        with the `field` argument, which can represent either a
        positional index or a textual key.

        If the cursor cannot be moved forward or if the record contains
        no values, :py:const:`None` will be returned instead.

        This method is particularly useful when it is known that a
        Cypher query returns only a single value.

        :param field: field to select value from (optional)
        :returns: value of the field or :py:const:`None`

        Example:
            >>> from py2neo import Graph
            >>> g = Graph()
            >>> g.run("MATCH (a) WHERE a.email=$x RETURN a.name", x="bob@acme.com").evaluate()
            'Bob Robertson'
        """
        if self.forward():
            try:
                return self[field]
            except IndexError:
                return None
        else:
            return None

    def data(self, *keys):
        """ Consume and extract the entire result as a list of
        dictionaries.

        ::

            >>> from py2neo import Graph
            >>> graph = Graph()
            >>> graph.run("MATCH (a:Person) RETURN a.name, a.born LIMIT 4").data()
            [{'a.born': 1964, 'a.name': 'Keanu Reeves'},
             {'a.born': 1967, 'a.name': 'Carrie-Anne Moss'},
             {'a.born': 1961, 'a.name': 'Laurence Fishburne'},
             {'a.born': 1960, 'a.name': 'Hugo Weaving'}]

        :param keys: indexes or keys of the items to include; if none
                     are provided, all values will be included
        :returns: list of dictionary of values, keyed by field name
        :raises IndexError: if an out-of-bounds index is specified
        """
        return [record.data(*keys) for record in self]

    def to_table(self):
        """ Consume and extract the entire result as a :class:`.Table`
        object.

        :return: the full query result
        """
        return Table(self)

    def to_subgraph(self):
        """ Consume and extract the entire result as a :class:`.Subgraph`
        containing the union of all the graph structures within.

        :return: :class:`.Subgraph` object
        """
        s = None
        for record in self:
            s_ = record.to_subgraph()
            if s_ is not None:
                if s is None:
                    s = s_
                else:
                    s |= s_
        return s

    def to_ndarray(self, dtype=None, order='K'):
        """ Consume and extract the entire result as a
        `numpy.ndarray <https://docs.scipy.org/doc/numpy/reference/generated/numpy.ndarray.html>`_.

        .. note::
           This method requires `numpy` to be installed.

        :param dtype:
        :param order:
        :warns: If `numpy` is not installed
        :returns: `ndarray
            <https://docs.scipy.org/doc/numpy/reference/generated/numpy.ndarray.html>`__ object.
        """
        try:
            # noinspection PyPackageRequirements
            from numpy import array
        except ImportError:
            warn("Numpy is not installed.")
            raise
        else:
            return array(list(map(list, self)), dtype=dtype, order=order)

    def to_series(self, field=0, index=None, dtype=None):
        """ Consume and extract one field of the entire result as a
        `pandas.Series <http://pandas.pydata.org/pandas-docs/stable/dsintro.html#series>`_.

        .. note::
           This method requires `pandas` to be installed.

        :param field:
        :param index:
        :param dtype:
        :warns: If `pandas` is not installed
        :returns: `Series
            <http://pandas.pydata.org/pandas-docs/stable/dsintro.html#series>`__ object.
        """
        try:
            # noinspection PyPackageRequirements
            from pandas import Series
        except ImportError:
            warn("Pandas is not installed.")
            raise
        else:
            return Series([record[field] for record in self], index=index, dtype=dtype)

    def to_data_frame(self, index=None, columns=None, dtype=None):
        """ Consume and extract the entire result as a
        `pandas.DataFrame <http://pandas.pydata.org/pandas-docs/stable/dsintro.html#dataframe>`_.

        ::

            >>> from py2neo import Graph
            >>> graph = Graph()
            >>> graph.run("MATCH (a:Person) RETURN a.name, a.born LIMIT 4").to_data_frame()
               a.born              a.name
            0    1964        Keanu Reeves
            1    1967    Carrie-Anne Moss
            2    1961  Laurence Fishburne
            3    1960        Hugo Weaving

        .. note::
           This method requires `pandas` to be installed.

        :param index: Index to use for resulting frame.
        :param columns: Column labels to use for resulting frame.
        :param dtype: Data type to force.
        :warns: If `pandas` is not installed
        :returns: `DataFrame
            <http://pandas.pydata.org/pandas-docs/stable/dsintro.html#series>`__ object.
        """
        try:
            # noinspection PyPackageRequirements
            from pandas import DataFrame
        except ImportError:
            warn("Pandas is not installed.")
            raise
        else:
            return DataFrame(list(map(dict, self)), index=index, columns=columns, dtype=dtype)

    def to_matrix(self, mutable=False):
        """ Consume and extract the entire result as a
        `sympy.Matrix <http://docs.sympy.org/latest/tutorial/matrices.html>`_.

        .. note::
           This method requires `sympy` to be installed.

        :param mutable:
        :returns: `Matrix
            <http://docs.sympy.org/latest/tutorial/matrices.html>`_ object.
        """
        try:
            # noinspection PyPackageRequirements
            from sympy import MutableMatrix, ImmutableMatrix
        except ImportError:
            warn("Sympy is not installed.")
            raise
        else:
            if mutable:
                return MutableMatrix(list(map(list, self)))
            else:
                return ImmutableMatrix(list(map(list, self)))


class Record(tuple, Mapping):
    """ A :class:`.Record` object holds an ordered, keyed collection of
    values. It is in many ways similar to a :class:`namedtuple` but
    allows field access only through bracketed syntax, and provides
    more functionality. :class:`.Record` extends both :class:`tuple`
    and :class:`Mapping`.

    .. describe:: record[index]
                  record[key]

        Return the value of *record* with the specified *key* or *index*.

    .. describe:: len(record)

        Return the number of fields in *record*.

    .. describe:: dict(record)

        Return a `dict` representation of *record*.

    """

    __keys = None

    def __new__(cls, keys, values):
        inst = tuple.__new__(cls, values)
        inst.__keys = keys
        return inst

    def __repr__(self):
        return "Record({%s})" % ", ".join("%r: %r" % (field, self[i])
                                          for i, field in enumerate(self.__keys))

    def __str__(self):
        return "\t".join(map(repr, (self[i] for i, _ in enumerate(self.__keys))))

    def __eq__(self, other):
        return dict(self) == dict(other)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return reduce(xor_operator, map(hash, self.items()))

    def __getitem__(self, key):
        if isinstance(key, slice):
            keys = self.__keys[key]
            values = super(Record, self).__getitem__(key)
            return self.__class__(zip(keys, values))
        index = self.index(key)
        if 0 <= index < len(self):
            return super(Record, self).__getitem__(index)
        else:
            return None

    def __getslice__(self, start, stop):
        key = slice(start, stop)
        keys = self.__keys[key]
        values = tuple(self)[key]
        return self.__class__(zip(keys, values))

    def get(self, key, default=None):
        """ Obtain a single value from the record by index or key. If the
        specified item does not exist, the default value is returned.

        :param key: index or key
        :param default: default value to be returned if `key` does not exist
        :return: selected value
        """
        try:
            index = self.__keys.index(ustr(key))
        except ValueError:
            return default
        if 0 <= index < len(self):
            return super(Record, self).__getitem__(index)
        else:
            return default

    def index(self, key):
        """ Return the index of the given item.
        """
        from six import integer_types, string_types
        if isinstance(key, integer_types):
            if 0 <= key < len(self.__keys):
                return key
            raise IndexError(key)
        elif isinstance(key, string_types):
            try:
                return self.__keys.index(key)
            except ValueError:
                raise KeyError(key)
        else:
            raise TypeError(key)

    def keys(self):
        """ Return the keys of the record.

        :return: list of key names
        """
        return list(self.__keys)

    def values(self, *keys):
        """ Return the values of the record, optionally filtering to
        include only certain values by index or key.

        :param keys: indexes or keys of the items to include; if none
                     are provided, all values will be included
        :return: list of values
        """
        if keys:
            d = []
            for key in keys:
                try:
                    i = self.index(key)
                except KeyError:
                    d.append(None)
                else:
                    d.append(self[i])
            return d
        return list(self)

    def items(self, *keys):
        """ Return the fields of the record as a list of key and value tuples

        :param keys: indexes or keys of the items to include; if none
                     are provided, all values will be included
        :return: list of (key, value) tuples
        """
        if keys:
            d = []
            for key in keys:
                try:
                    i = self.index(key)
                except KeyError:
                    d.append((key, None))
                else:
                    d.append((self.__keys[i], self[i]))
            return d
        return list((self.__keys[i], super(Record, self).__getitem__(i)) for i in range(len(self)))

    def data(self, *keys):
        """ Return the keys and values of this record as a dictionary,
        optionally including only certain values by index or key. Keys
        provided that do not exist within the record will be included
        but with a value of :py:const:`None`; indexes provided
        that are out of bounds will trigger an :exc:`IndexError`.

        :param keys: indexes or keys of the items to include; if none
                     are provided, all values will be included
        :return: dictionary of values, keyed by field name
        :raises: :exc:`IndexError` if an out-of-bounds index is specified
        """
        if keys:
            d = {}
            for key in keys:
                try:
                    i = self.index(key)
                except KeyError:
                    d[key] = None
                else:
                    d[self.__keys[i]] = self[i]
            return d
        return dict(self)

    def to_subgraph(self):
        """ Return a :class:`.Subgraph` containing the union of all the
        graph structures within this :class:`.Record`.

        :return: :class:`.Subgraph` object
        """
        from py2neo.data import Subgraph
        s = None
        for value in self.values():
            if isinstance(value, Subgraph):
                if s is None:
                    s = value
                else:
                    s |= value
        return s


class Table(list):
    """ Immutable list of records.

    A :class:`.Table` holds a list of :class:`.Record` objects, typically received as the result of a Cypher query.
    It provides a convenient container for working with a result in its entirety and provides methods for conversion into various output formats.
    :class:`.Table` extends ``list``.

    .. describe:: repr(table)

        Return a string containing an ASCII art representation of this table.
        Internally, this method calls :meth:`.write` with `header=True`, writing the output into an ``io.StringIO`` instance.

    """

    def __init__(self, records, keys=None):
        super(Table, self).__init__(map(tuple, records))
        if keys:
            k = list(map(ustr, keys))
        else:
            try:
                k = records.keys()
            except AttributeError:
                raise ValueError("Missing keys")
        if not k:
            raise ValueError("Missing keys")
        width = len(k)
        t = [set() for _ in range(width)]
        o = [False] * width
        for record in self:
            for i, value in enumerate(record):
                if value is None:
                    o[i] = True
                else:
                    t[i].add(type(value))
        f = []
        for i, _ in enumerate(k):
            f.append({
                "type": t[i].copy().pop() if len(t[i]) == 1 else tuple(t[i]),
                "numeric": all(t_ in numeric_types for t_ in t[i]),
                "optional": o[i],
            })
        self._keys = k
        self._fields = f

    def __repr__(self):
        s = StringIO()
        self.write(file=s, header=True)
        return s.getvalue()

    def _repr_html_(self):
        """ Return a string containing an HTML representation of this table.
        This method is used by Jupyter notebooks to display the table natively within a browser.
        Internally, this method calls :meth:`.write_html` with `header=True`, writing the output into an ``io.StringIO`` instance.
        """
        s = StringIO()
        self.write_html(file=s, header=True)
        return s.getvalue()

    def keys(self):
        """ Return a list of field names for this table.
        """
        return list(self._keys)

    def field(self, key):
        """ Return a dictionary of metadata for a given field.
        The metadata includes the following values:

        `type`
            Single class or tuple of classes representing the
            field values.

        `numeric`
            :const:`True` if all field values are of a numeric
            type, :const:`False` otherwise.

        `optional`
            :const:`True` if any field values are :const:`None`,
            :const:`False` otherwise.

        """
        from six import integer_types, string_types
        if isinstance(key, integer_types):
            return self._fields[key]
        elif isinstance(key, string_types):
            try:
                index = self._keys.index(key)
            except ValueError:
                raise KeyError(key)
            else:
                return self._fields[index]
        else:
            raise TypeError(key)

    def _range(self, skip, limit):
        if skip is None:
            skip = 0
        if limit is None or skip + limit > len(self):
            return range(skip, len(self))
        else:
            return range(skip, skip + limit)

    def write(self, file=None, header=None, skip=None, limit=None, auto_align=True,
              padding=1, separator=u"|", newline=u"\r\n"):
        """ Write data to a human-readable ASCII art table.

        :param file: file-like object capable of receiving output
        :param header: boolean flag for addition of column headers
        :param skip: number of records to skip before beginning output
        :param limit: maximum number of records to include in output
        :param auto_align: if :const:`True`, right-justify numeric values
        :param padding: number of spaces to include between column separator and value
        :param separator: column separator character
        :param newline: newline character sequence
        :return: the number of records included in output
        """

        space = u" " * padding
        widths = [1 if header else 0] * len(self._keys)

        def calc_widths(values, **_):
            strings = [cypher_str(value).splitlines(False) for value in values]
            for i, s in enumerate(strings):
                w = max(map(len, s)) if s else 0
                if w > widths[i]:
                    widths[i] = w

        def write_line(values, underline=u""):
            strings = [cypher_str(value).splitlines(False) for value in values]
            height = max(map(len, strings)) if strings else 1
            for y in range(height):
                line_text = u""
                underline_text = u""
                for x, _ in enumerate(values):
                    try:
                        text = strings[x][y]
                    except IndexError:
                        text = u""
                    if auto_align and self._fields[x]["numeric"]:
                        text = space + text.rjust(widths[x]) + space
                        u_text = underline * len(text)
                    else:
                        text = space + text.ljust(widths[x]) + space
                        u_text = underline * len(text)
                    if x > 0:
                        text = separator + text
                        u_text = separator + u_text
                    line_text += text
                    underline_text += u_text
                if underline:
                    line_text += newline + underline_text
                line_text += newline
                print(line_text, end=u"", file=file)

        def apply(f):
            count = 0
            for count, index in enumerate(self._range(skip, limit), start=1):
                if count == 1 and header:
                    f(self.keys(), underline=u"-")
                f(self[index])
            return count

        apply(calc_widths)
        return apply(write_line)

    def write_html(self, file=None, header=None, skip=None, limit=None, auto_align=True):
        """ Write data to an HTML table.

        :param file: file-like object capable of receiving output
        :param header: boolean flag for addition of column headers
        :param skip: number of records to skip before beginning output
        :param limit: maximum number of records to include in output
        :param auto_align: if :const:`True`, right-justify numeric values
        :return: the number of records included in output
        """

        def html_escape(s):
            return (s.replace(u"&", u"&amp;")
                     .replace(u"<", u"&lt;")
                     .replace(u">", u"&gt;")
                     .replace(u'"', u"&quot;")
                     .replace(u"'", u"&#039;"))

        def write_tr(values, tag):
            print(u"<tr>", end="", file=file)
            for i, value in enumerate(values):
                if tag == "th":
                    template = u'<{}>{}</{}>'
                elif auto_align and self._fields[i]["numeric"]:
                    template = u'<{} style="text-align:right">{}</{}>'
                else:
                    template = u'<{} style="text-align:left">{}</{}>'
                print(template.format(tag, html_escape(cypher_str(value)), tag), end="", file=file)
            print(u"</tr>", end="", file=file)

        count = 0
        print(u"<table>", end="", file=file)
        for count, index in enumerate(self._range(skip, limit), start=1):
            if count == 1 and header:
                write_tr(self.keys(), u"th")
            write_tr(self[index], u"td")
        print(u"</table>", end="", file=file)
        return count

    def write_separated_values(self, separator, file=None, header=None, skip=None, limit=None,
                               newline=u"\r\n", quote=u"\""):
        """ Write data to a delimiter-separated file.

        :param separator: field separator character
        :param file: file-like object capable of receiving output
        :param header: boolean flag or string style tag, such as 'i' or 'cyan',
            for addition of column headers
        :param skip: number of records to skip before beginning output
        :param limit: maximum number of records to include in output
        :param newline: newline character sequence
        :param quote: quote character
        :return: the number of records included in output
        """
        from pansi import ansi
        from six import string_types

        escaped_quote = quote + quote
        quotable = separator + newline + quote

        def header_row(names):
            if isinstance(header, string_types):
                if hasattr(ansi, header):
                    template = "{%s}{}{_}" % header
                else:
                    t = [tag for tag in dir(ansi) if
                         not tag.startswith("_") and isinstance(getattr(ansi, tag), str)]
                    raise ValueError("Unknown style tag %r\n"
                                     "Available tags are: %s" % (header, ", ".join(map(repr, t))))
            else:
                template = "{}"
            for name in names:
                yield template.format(name, **ansi)

        def data_row(values):
            for value in values:
                if value is None:
                    yield ""
                    continue
                if isinstance(value, string_types):
                    value = ustr(value)
                    if any(ch in value for ch in quotable):
                        value = quote + value.replace(quote, escaped_quote) + quote
                else:
                    value = cypher_repr(value)
                yield value

        count = 0
        for count, index in enumerate(self._range(skip, limit), start=1):
            if count == 1 and header:
                print(*header_row(self.keys()), sep=separator, end=newline, file=file)
            print(*data_row(self[index]), sep=separator, end=newline, file=file)
        return count

    def write_csv(self, file=None, header=None, skip=None, limit=None):
        """ Write the data as RFC4180-compatible comma-separated values.
        This is a customised call to :meth:`.write_separated_values`.
        """
        return self.write_separated_values(u",", file, header, skip, limit)

    def write_tsv(self, file=None, header=None, skip=None, limit=None):
        """ Write the data as tab-separated values.
        This is a customised call to :meth:`.write_separated_values`.
        """
        return self.write_separated_values(u"\t", file, header, skip, limit)


class CypherSummary(object):

    def __init__(self, **data):
        self._data = data

    @property
    def connection(self):
        return self._data.get("connection")


class CypherStats(Mapping):
    """ Container for a set of statistics drawn from Cypher query execution.

    Each value can be accessed as either an attribute or via a string index.
    This class implements :py:class:`.Mapping` to allow it to be used as a
    dictionary.
    """

    #: Boolean flag to indicate whether or not the query contained an update.
    contained_updates = False
    #: Number of nodes created.
    nodes_created = 0
    #: Number of nodes deleted.
    nodes_deleted = 0
    #: Number of property values set.
    properties_set = 0
    #: Number of relationships created.
    relationships_created = 0
    #: Number of relationships deleted.
    relationships_deleted = 0
    #: Number of node labels added.
    labels_added = 0
    #: Number of node labels removed.
    labels_removed = 0
    #: Number of indexes added.
    indexes_added = 0
    #: Number of indexes removed.
    indexes_removed = 0
    #: Number of constraints added.
    constraints_added = 0
    #: Number of constraints removed.
    constraints_removed = 0

    def __init__(self, **stats):
        for key, value in stats.items():
            key = key.replace("-", "_")
            if key.startswith("relationship_"):
                # hack for server bug
                key = "relationships_" + key[13:]
            if hasattr(self.__class__, key):
                setattr(self, key, value)
            self.contained_updates = bool(sum(getattr(self, k, 0)
                                              for k in self.keys()))

    def __repr__(self):
        lines = []
        for key in sorted(self.keys()):
            lines.append("{}: {}".format(key, getattr(self, key)))
        return "\n".join(lines)

    def __getitem__(self, key):
        return getattr(self, key)

    def __len__(self):
        return len(self.keys())

    def __iter__(self):
        return iter(self.keys())

    def keys(self):
        """ Full list of the key or attribute names of the statistics
        available.
        """
        return [key for key in vars(self.__class__).keys()
                if not key.startswith("_") and key != "keys"]


class CypherPlan(Mapping):

    @classmethod
    def _clean_key(cls, key):
        from english.casing import Words
        return Words(key).snake()

    @classmethod
    def _clean_keys(cls, data):
        return OrderedDict(sorted((cls._clean_key(key), value)
                                  for key, value in dict(data).items()))

    def __init__(self, **kwargs):
        data = self._clean_keys(kwargs)
        if "root" in data:
            data = self._clean_keys(data["root"])
        self.operator_type = data.pop("operator_type", None)
        self.identifiers = data.pop("identifiers", [])
        self.children = [CypherPlan(**self._clean_keys(child))
                         for child in data.pop("children", [])]
        try:
            args = data.pop("args")
        except KeyError:
            self.args = data
        else:
            self.args = self._clean_keys(args)

    def __repr__(self):
        return ("%s(operator_type=%r, identifiers=%r, children=%r, args=%r)" %
                (self.__class__.__name__, self.operator_type,
                 self.identifiers, self.children, self.args))

    def __getitem__(self, key):
        return getattr(self, key)

    def __len__(self):
        return len(self.keys())

    def __iter__(self):
        return iter(self.keys())

    def keys(self):
        return ["operator_type", "identifiers", "children", "args"]


class Neo4jError(Exception):
    """ Base exception for all errors signalled by Neo4j.
    """

    classification = None
    category = None
    title = None
    code = None
    message = None

    @classmethod
    def hydrate(cls, data):
        try:
            code = data["code"]
            message = data["message"]
        except KeyError:
            classification = None
            category = None
            title = None
            code = None
            message = None
        else:
            _, classification, category, title = code.split(".")
        if classification == "ClientError":
            error_cls = ClientError
        elif classification == "DatabaseError":
            error_cls = DatabaseError
        elif classification == "TransientError":
            error_cls = TransientError
        else:
            error_cls = cls
        error_text = message or "<Unknown>"
        if category or title:
            error_text = "[%s.%s] %s" % (category, title, error_text)
        inst = error_cls(error_text)
        inst.classification = classification
        inst.category = category
        inst.title = title
        inst.code = code
        inst.message = message
        return inst

    def __new__(cls, *args, **kwargs):
        try:
            exception = kwargs["exception"]
            error_cls = type(xstr(exception), (cls,), {})
        except KeyError:
            error_cls = cls
        return Exception.__new__(error_cls, *args)

    def __init__(self, *args, **kwargs):
        Exception.__init__(self, *args)
        for key, value in kwargs.items():
            setattr(self, key.lower(), value)


class ClientError(Neo4jError):
    """ The Client sent a bad request - changing the request might yield a successful outcome.
    """


class DatabaseError(Neo4jError):
    """ The database failed to service the request.
    """


class TransientError(Neo4jError):
    """ The database cannot service the request right now, retrying
    later might yield a successful outcome.
    """
