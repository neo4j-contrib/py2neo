#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2020, Nigel Small
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


from __future__ import absolute_import

from collections import deque, OrderedDict
from functools import reduce
from operator import xor as xor_operator
from warnings import warn

from six import integer_types, string_types

from py2neo.collections import iter_items
from py2neo.client import Connection
from py2neo.compat import Mapping, ustr
from py2neo.database import GraphError
from py2neo.text import Words
from py2neo.text.table import Table


class Transaction(object):
    """ A logical context for one or more graph operations.
    """

    _finished = False

    def __init__(self, graph, autocommit=False, readonly=False,
                 after=None, metadata=None, timeout=None):
        self._graph = graph
        self._autocommit = autocommit
        self._entities = deque()
        self._connector = self.graph.service.connector
        if autocommit:
            self._transaction = None
        else:
            self._transaction = self._connector.begin(self._graph.name,
                                                      readonly, after, metadata, timeout)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self.commit()
        else:
            self.rollback()

    def _assert_unfinished(self):
        if self._finished:
            raise TransactionFinished(self)

    @property
    def graph(self):
        return self._graph

    @property
    def entities(self):
        return self._entities

    def finished(self):
        """ Indicates whether or not this transaction has been completed
        or is still open.
        """
        return self._finished

    def run(self, cypher, parameters=None, **kwparameters):
        """ Send a Cypher statement to the server for execution and return
        a :py:class:`.Cursor` for navigating its result.

        :param cypher: Cypher statement
        :param parameters: dictionary of parameters
        :returns: :py:class:`.Cursor` object
        """
        self._assert_unfinished()
        try:
            entities = self._entities.popleft()
        except IndexError:
            entities = {}

        try:
            hydrant = Connection.default_hydrant(self._connector.profile, self.graph)
            parameters = dict(parameters or {}, **kwparameters)
            if self._transaction:
                result = self._connector.run_in_tx(self._transaction, cypher, parameters, hydrant)
            else:
                result = self._connector.auto_run(self.graph.name, cypher, parameters, hydrant)
            return Cursor(result, hydrant, entities)
        finally:
            if not self._transaction:
                self.finish()

    def finish(self):
        self._assert_unfinished()
        self._finished = True

    def commit(self):
        """ Commit the transaction.
        """
        self._assert_unfinished()
        try:
            return self._connector.commit(self._transaction)
        finally:
            self._finished = True

    def rollback(self):
        """ Roll back the current transaction, undoing all actions previously taken.
        """
        self._assert_unfinished()
        try:
            return self._connector.rollback(self._transaction)
        finally:
            self._finished = True

    def evaluate(self, cypher, parameters=None, **kwparameters):
        """ Execute a single Cypher statement and return the value from
        the first column of the first record.

        :param cypher: Cypher statement
        :param parameters: dictionary of parameters
        :returns: single return value or :const:`None`
        """
        return self.run(cypher, parameters, **kwparameters).evaluate(0)

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

    def __init__(self, result, hydrant=None, entities=None):
        self._result = result
        self._hydrant = hydrant
        self._entities = entities
        self._current = None
        self._closed = False

    def __del__(self):
        try:
            self.close()
        except OSError:
            pass

    def __repr__(self):
        return repr(self.preview(3))

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
        return self._result.fields()

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
        v = self._result.protocol_version
        while moved != amount:
            values = self._result.fetch()
            if values is None:
                break
            else:
                keys = self._result.fields()  # TODO: don't do this for every record
                if self._hydrant:
                    values = self._hydrant.hydrate(keys, values, entities=self._entities, version=v)
                self._current = Record(zip(keys, values))
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
        v = self._result.protocol_version
        records = []
        keys = self._result.fields()
        for values in self._result.peek_records(int(limit)):
            if self._hydrant:
                values = self._hydrant.hydrate(keys, values, entities=self._entities, version=v)
            records.append(values)
        return Table(records, keys)

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
    """ A :class:`.Record` is an immutable ordered collection of key-value
    pairs. It is generally closer to a :class:`namedtuple` than to a
    :class:`OrderedDict` inasmuch as iteration of the collection will
    yield values rather than keys.
    """

    __keys = None

    def __new__(cls, iterable=()):
        keys = []
        values = []
        for key, value in iter_items(iterable):
            keys.append(key)
            values.append(value)
        inst = tuple.__new__(cls, values)
        inst.__keys = tuple(keys)
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


class ClientError(GraphError):
    """ The Client sent a bad request - changing the request might yield a successful outcome.
    """

    @classmethod
    def get_mapped_class(cls, status):
        # TODO: mappings to error subclasses:
        #     {
        #         #
        #         # ConstraintError
        #         "Neo.ClientError.Schema.ConstraintValidationFailed": ConstraintError,
        #         "Neo.ClientError.Schema.ConstraintViolation": ConstraintError,
        #         "Neo.ClientError.Statement.ConstraintVerificationFailed": ConstraintError,
        #         "Neo.ClientError.Statement.ConstraintViolation": ConstraintError,
        #         #
        #         # CypherSyntaxError
        #         "Neo.ClientError.Statement.InvalidSyntax": CypherSyntaxError,
        #         "Neo.ClientError.Statement.SyntaxError": CypherSyntaxError,
        #         #
        #         # CypherTypeError
        #         "Neo.ClientError.Procedure.TypeError": CypherTypeError,
        #         "Neo.ClientError.Statement.InvalidType": CypherTypeError,
        #         "Neo.ClientError.Statement.TypeError": CypherTypeError,
        #         #
        #         # Forbidden
        #         "Neo.ClientError.General.ForbiddenOnReadOnlyDatabase": Forbidden,
        #         "Neo.ClientError.General.ReadOnly": Forbidden,
        #         "Neo.ClientError.Schema.ForbiddenOnConstraintIndex": Forbidden,
        #         "Neo.ClientError.Schema.IndexBelongsToConstrain": Forbidden,
        #         "Neo.ClientError.Security.Forbidden": Forbidden,
        #         "Neo.ClientError.Transaction.ForbiddenDueToTransactionType": Forbidden,
        #         #
        #         # Unauthorized
        #         "Neo.ClientError.Security.AuthorizationFailed": AuthError,
        #         "Neo.ClientError.Security.Unauthorized": AuthError,
        #         #
        #     }
        raise KeyError(status)


class DatabaseError(GraphError):
    """ The database failed to service the request.
    """


class TransientError(GraphError):
    """ The database cannot service the request right now, retrying
    later might yield a successful outcome.
    """


class TransactionFinished(GraphError):
    """ Raised when actions are attempted against a
    :class:`.GraphTransaction` that is no longer available for use.
    """
