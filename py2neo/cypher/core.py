#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2014, Nigel Small
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


from collections import OrderedDict
import logging

from py2neo import Bindable, Resource, Node, Relationship, Subgraph, Path, Finished
from py2neo.compat import integer, xstr, ustr
from py2neo.lang import cypher_escape, TextTable
from py2neo.cypher.error.core import CypherError, TransactionError
from py2neo.primitive import Record
from py2neo.util import is_collection, deprecated


__all__ = ["CypherEngine", "Transaction", "Result"]


log = logging.getLogger("py2neo.cypher")


def first_node(x):
    if hasattr(x, "__nodes__"):
        try:
            return next(x.__nodes__())
        except StopIteration:
            raise ValueError("No such node: %r" % x)
    raise ValueError("No such node: %r" % x)


def last_node(x):
    if hasattr(x, "__nodes__"):
        nodes = list(x.__nodes__())
        if nodes:
            return nodes[-1]
    raise ValueError("No such node: %r" % x)


def presubstitute(statement, parameters):
    more = True
    presub_parameters = []
    while more:
        before, opener, key = statement.partition(u"«")
        if opener:
            key, closer, after = key.partition(u"»")
            try:
                value = parameters[key]
                presub_parameters.append(key)
            except KeyError:
                raise KeyError("Expected a presubstitution parameter named %r" % key)
            if isinstance(value, integer):
                value = ustr(value)
            elif isinstance(value, tuple) and all(map(lambda x: isinstance(x, integer), value)):
                value = u"%d..%d" % (value[0], value[-1])
            elif is_collection(value):
                value = ":".join(map(cypher_escape, value))
            else:
                value = cypher_escape(value)
            statement = before + value + after
        else:
            more = False
    parameters = {k:v for k,v in parameters.items() if k not in presub_parameters}
    return statement, parameters


class CypherEngine(Bindable):
    """ Service wrapper for all Cypher functionality, providing access
    to transactions as well as single statement execution and streaming.

    This class will usually be instantiated via a :class:`py2neo.Graph`
    object and will be made available through the
    :attr:`py2neo.Graph.cypher` attribute. Therefore, for single
    statement execution, simply use the :func:`execute` method::

        from py2neo import Graph
        graph = Graph()
        results = graph.cypher.execute("MATCH (n:Person) RETURN n")

    """

    error_class = CypherError

    __instances = {}

    def __new__(cls, transaction_uri):
        try:
            inst = cls.__instances[transaction_uri]
        except KeyError:
            inst = super(CypherEngine, cls).__new__(cls)
            inst.bind(transaction_uri)
            cls.__instances[transaction_uri] = inst
        return inst

    def post(self, statement, parameters=None, **kwparameters):
        """ Post a Cypher statement to this resource, optionally with
        parameters.

        :arg statement: A Cypher statement to execute.
        :arg parameters: A dictionary of parameters.
        :arg kwparameters: Extra parameters supplied by keyword.
        """
        tx = Transaction(self)
        result = tx.run(statement, parameters, **kwparameters)
        tx.post(commit=True)
        return result

    def run(self, statement, parameters=None, **kwparameters):
        """ Execute a single Cypher statement, ignoring any return value.

        :arg statement: A Cypher statement to execute.
        :arg parameters: A dictionary of parameters.
        """
        tx = Transaction(self)
        result = tx.run(statement, parameters, **kwparameters)
        tx.commit()
        return result

    def evaluate(self, statement, parameters=None, **kwparameters):
        """ Execute a single Cypher statement and return the value from
        the first column of the first record returned.

        :arg statement: A Cypher statement to execute.
        :arg parameters: A dictionary of parameters.
        :return: Single return value or :const:`None`.
        """
        tx = Transaction(self)
        result = tx.run(statement, parameters, **kwparameters)
        tx.commit()
        return result.value()

    def begin(self):
        """ Begin a new transaction.

        :rtype: :class:`py2neo.cypher.Transaction`
        """
        return Transaction(self)

    @deprecated("CypherEngine.execute(...) is deprecated, "
                "use CypherEngine.run(...) instead")
    def execute(self, statement, parameters=None, **kwparameters):
        """ Execute a single Cypher statement.

        :arg statement: A Cypher statement to execute.
        :arg parameters: A dictionary of parameters.
        :rtype: :class:`py2neo.cypher.Result`
        """
        tx = Transaction(self)
        result = tx.run(statement, parameters, **kwparameters)
        tx.commit()
        return result


class Transaction(object):
    """ A transaction is a transient resource that allows multiple Cypher
    statements to be executed within a single server transaction.
    """

    error_class = TransactionError

    def __init__(self, cypher):
        log.info("begin")
        self.statements = []
        self.results = []
        self.cypher = cypher
        uri = self.cypher.resource.uri.string
        self._begin = Resource(uri)
        self._begin_commit = Resource(uri + "/commit")
        self._execute = None
        self._commit = None
        self._finished = False
        self.graph = self._begin.graph

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self.commit()
        else:
            self.rollback()

    def _assert_unfinished(self):
        if self._finished:
            raise Finished(self)

    @property
    def _id(self):
        """ The internal server ID of this transaction, if available.
        """
        if self._execute is None:
            return None
        else:
            return int(self._execute.uri.path.segments[-1])

    def post(self, commit=False, hydrate=False):
        self._assert_unfinished()
        if commit:
            log.info("commit")
            resource = self._commit or self._begin_commit
            self._finished = True
        else:
            log.info("process")
            resource = self._execute or self._begin
        rs = resource.post({"statements": self.statements})
        location = rs.location
        if location:
            self._execute = Resource(location)
        j = rs.content
        rs.close()
        self.statements = []
        if "commit" in j:
            self._commit = Resource(j["commit"])
        for j_error in j["errors"]:
            raise self.error_class.hydrate(j_error)
        for j_result in j["results"]:
            result = self.results.pop(0)
            keys = j_result["columns"]
            if hydrate:
                result._process(keys, [Record(keys, self.graph.hydrate(data["rest"]))
                                       for data in j_result["data"]])
            else:
                result._process(keys, [data["rest"] for data in j_result["data"]])

    def process(self):
        """ Send all pending statements to the server for execution, leaving
        the transaction open for further statements. Along with
        :meth:`append <.Transaction.append>`, this method can be used to
        batch up a number of individual statements into a single HTTP request::

            from py2neo import Graph

            graph = Graph()
            statement = "MERGE (n:Person {name:{N}}) RETURN n"

            tx = graph.cypher.begin()

            def add_names(*names):
                for name in names:
                    tx.append(statement, {"N": name})
                tx.process()

            add_names("Homer", "Marge", "Bart", "Lisa", "Maggie")
            add_names("Peter", "Lois", "Chris", "Meg", "Stewie")

            tx.commit()

        """
        self.post(hydrate=True)

    def commit(self):
        """ Send all pending statements to the server for execution and commit
        the transaction.
        """
        self.post(commit=True, hydrate=True)

    def rollback(self):
        """ Rollback the current transaction.
        """
        self._assert_unfinished()
        log.info("rollback")
        try:
            if self._execute:
                self._execute.delete()
        finally:
            self._finished = True

    @deprecated("Transaction.append(...) is deprecated, use Transaction.run(...) instead")
    def append(self, statement, parameters=None, **kwparameters):
        return self.run(statement, parameters, **kwparameters)

    def run(self, statement, parameters=None, **kwparameters):
        """ Add a statement to the current queue of statements to be
        executed.

        :arg statement: the statement to append
        :arg parameters: a dictionary of execution parameters
        """
        self._assert_unfinished()

        s = ustr(statement)
        p = {}

        def add_parameters(params):
            if params:
                for k, v in dict(params).items():
                    if isinstance(v, (Node, Relationship)):
                        v = v._id
                    p[k] = v

        if hasattr(statement, "parameters"):
            add_parameters(statement.parameters)
        add_parameters(dict(parameters or {}, **kwparameters))

        s, p = presubstitute(s, p)

        # OrderedDict is used here to avoid statement/parameters ordering bug
        log.info("append %r %r", s, p)
        self.statements.append(OrderedDict([
            ("statement", s),
            ("parameters", p),
            ("resultDataContents", ["REST"]),
        ]))
        result = Result(self)
        self.results.append(result)
        return result

    def evaluate(self, statement, parameters=None, **kwparameters):
        return self.run(statement, parameters, **kwparameters).value()

    def finished(self):
        """ Indicates whether or not this transaction has been completed or is
        still open.

        :return: :py:const:`True` if this transaction has finished,
                 :py:const:`False` otherwise
        """
        return self._finished


class Result(object):
    """ A stream of records returned from the execution of a Cypher statement.
    """

    def __init__(self, transaction=None):
        assert transaction is None or isinstance(transaction, Transaction)
        self.transaction = transaction
        self._keys = []
        self._records = []
        self._processed = False

    def __repr__(self):
        return "<Result>"

    def __str__(self):
        return xstr(self.__unicode__())

    def __unicode__(self):
        self._ensure_processed()
        out = ""
        if self._keys:
            table = TextTable([None] + self._keys, border=True)
            for i, record in enumerate(self._records):
                table.append([i + 1] + list(record))
            out = repr(table)
        return out

    def __len__(self):
        self._ensure_processed()
        return len(self._records)

    def __getitem__(self, item):
        self._ensure_processed()
        return self._records[item]

    def __iter__(self):
        self._ensure_processed()
        return iter(self._records)

    def _ensure_processed(self):
        if not self._processed:
            self.transaction.process()

    def _process(self, keys, records):
        self._keys = keys
        self._records = records
        self._processed = True

    def keys(self):
        return self._keys

    def value(self, index=0):
        """ A single value from the first record of this result. If no records
        are available, :const:`None` is returned.
        """
        self._ensure_processed()
        try:
            record = self[0]
        except IndexError:
            return None
        else:
            if len(record) > index:
                return record[index]
            else:
                return None

    def to_subgraph(self):
        """ Convert a Result into a Subgraph.
        """
        self._ensure_processed()
        entities = []
        for record in self._records:
            for value in record:
                if isinstance(value, (Node, Relationship, Path)):
                    entities.append(value)
        return Subgraph(*entities)
