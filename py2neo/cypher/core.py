#!/usr/bin/env python
# -*- coding: utf-8 -*-

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


from __future__ import unicode_literals

from collections import OrderedDict
import logging

from py2neo.core import Service, Resource, Node, Rel, Relationship, Subgraph, Path
from py2neo.cypher.error.core import CypherError, CypherTransactionError
from py2neo.packages.jsonstream import assembled
from py2neo.packages.tart.tables import TextTable
from py2neo.util import is_integer, is_string, xstr


__all__ = ["CypherResource", "CypherTransaction", "RecordListList", "RecordList", "RecordStream",
           "Record", "RecordProducer", "TransactionFinished"]


log = logging.getLogger("py2neo.cypher")


class CypherResource(Service):
    """ Wrapper for the standard Cypher endpoint, providing
    non-transactional Cypher execution capabilities. Instances
    of this class will generally be created by and accessed via
    the associated Graph object::

        from py2neo import Graph
        graph = Graph()
        results = graph.cypher.execute("MATCH (n:Person) RETURN n")

    """

    error_class = CypherError

    __instances = {}

    def __new__(cls, uri, transaction_uri=None):
        key = (uri, transaction_uri)
        try:
            inst = cls.__instances[key]
        except KeyError:
            inst = super(CypherResource, cls).__new__(cls)
            inst.bind(uri)
            inst.transaction_uri = transaction_uri
            cls.__instances[key] = inst
        return inst

    def post(self, statement, parameters=None):
        log.info("Statement: %r", statement)
        payload = {"query": statement}
        if parameters:
            payload["params"] = {}
            for key, value in parameters.items():
                if isinstance(value, (Node, Rel, Relationship)):
                    value = value._id
                payload["params"][key] = value
            log.info("Parameters: %r", payload["params"])
        return self.resource.post(payload)

    def run(self, statement, parameters=None):
        self.post(statement, parameters).close()

    def execute(self, statement, parameters=None):
        response = self.post(statement, parameters)
        try:
            return self.graph.hydrate(response.content)
        finally:
            response.close()

    def execute_one(self, statement, parameters=None):
        response = self.post(statement, parameters)
        results = self.graph.hydrate(response.content)
        try:
            return results[0][0]
        except IndexError:
            return None
        finally:
            response.close()

    def stream(self, statement, parameters=None):
        """ Execute the query and return a result iterator.
        """
        return RecordStream(self.graph, self.post(statement, parameters))

    def begin(self):
        if self.transaction_uri:
            return CypherTransaction(self.transaction_uri)
        else:
            raise NotImplementedError("Transaction support not available from this "
                                      "Neo4j server version")


class CypherTransaction(object):
    """ A transaction is a transient resource that allows multiple Cypher
    statements to be executed within a single server transaction.
    """

    error_class = CypherTransactionError

    def __init__(self, uri):
        self.statements = []
        self.__begin = Resource(uri)
        self.__begin_commit = Resource(uri + "/commit")
        self.__execute = None
        self.__commit = None
        self.__finished = False
        self.graph = self.__begin.graph

    def __assert_unfinished(self):
        if self.__finished:
            raise TransactionFinished()

    @property
    def _id(self):
        """ The internal server ID of this transaction, if available.
        """
        if self.__execute is None:
            return None
        else:
            return int(self.__execute.uri.path.segments[-1])

    @property
    def finished(self):
        """ Indicates whether or not this transaction has been completed or is
        still open.

        :return: :py:const:`True` if this transaction has finished,
                 :py:const:`False` otherwise
        """
        return self.__finished

    def execute(self, statement, parameters=None):
        """ Add a statement to the current queue of statements to be
        executed.

        :param statement: the statement to execute
        :param parameters: a dictionary of execution parameters
        """
        self.__assert_unfinished()
        # OrderedDict is used here to avoid statement/parameters ordering bug
        self.statements.append(OrderedDict([
            ("statement", statement),
            ("parameters", dict(parameters or {})),
            ("resultDataContents", ["REST"]),
        ]))

    def post(self, resource):
        self.__assert_unfinished()
        rs = resource.post({"statements": self.statements})
        location = rs.location
        if location:
            self.__execute = Resource(location)
        j = rs.content
        rs.close()
        self.statements = []
        if "commit" in j:
            self.__commit = Resource(j["commit"])
        if "errors" in j:
            errors = j["errors"]
            if len(errors) >= 1:
                error = errors[0]
                raise self.error_class.hydrate(error)
        out = RecordListList()
        for result in j["results"]:
            columns = result["columns"]
            producer = RecordProducer(columns)
            out.append(RecordList(columns, [producer.produce(self.graph.hydrate(r["rest"]))
                                            for r in result["data"]]))
        return out

    def flush(self):
        """ Send all pending statements to the server for execution, leaving
        the transaction open for further statements.

        :return: list of results from pending statements
        """
        return self.post(self.__execute or self.__begin)

    def commit(self):
        """ Send all pending statements to the server for execution and commit
        the transaction.

        :return: list of results from pending statements
        """
        try:
            return self.post(self.__commit or self.__begin_commit)
        finally:
            self.__finished = True

    def rollback(self):
        """ Rollback the current transaction.
        """
        self.__assert_unfinished()
        try:
            if self.__execute:
                self.__execute.delete()
        finally:
            self.__finished = True


class RecordListList(list):
    """ Container for multiple RecordList instances that presents a more
    consistent representation.
    """

    def __repr__(self):
        out = []
        for i in self:
            out.append(repr(i))
        return "\n".join(out)


class RecordList(object):
    """ A list of records returned from the execution of a Cypher statement.
    """

    @classmethod
    def hydrate(cls, data, graph):
        columns = data["columns"]
        rows = data["data"]
        producer = RecordProducer(columns)
        return cls(columns, [producer.produce(graph.hydrate(row)) for row in rows])

    def __init__(self, columns, records):
        self.columns = columns
        self.records = records

    def __repr__(self):
        out = ""
        if self.columns:
            table = TextTable([None] + self.columns, border=True)
            for i, record in enumerate(self.records):
                table.append([i + 1] + list(record))
            out = repr(table)
        return out

    def __len__(self):
        return len(self.records)

    def __getitem__(self, item):
        return self.records[item]

    def __iter__(self):
        return iter(self.records)

    def to_subgraph(self):
        """ Convert a RecordList into a Subgraph.
        """
        entities = []
        for record in self.records:
            for value in record:
                if isinstance(value, (Node, Path)):
                    entities.append(value)
        return Subgraph(*entities)


class RecordStream(object):
    """ An accessor for a sequence of records yielded by a streamed Cypher statement.

    ::

        for record in graph.cypher.stream("START n=node(*) RETURN n LIMIT 10")
            print record[0]

    Each record returned is cast into a :py:class:`namedtuple` with names
    derived from the resulting column names.

    .. note ::
        Results are available as returned from the server and are decoded
        incrementally. This means that there is no need to wait for the
        entire response to be received before processing can occur.
    """

    def __init__(self, graph, response):
        self.graph = graph
        self.__response = response
        self.__response_item = self.__response_iterator()
        self.columns = next(self.__response_item)

    def __response_iterator(self):
        producer = None
        columns = []
        record_data = None
        for key, value in self.__response:
            key_len = len(key)
            if key_len > 0:
                section = key[0]
                if section == "columns":
                    if key_len > 1:
                        columns.append(value)
                elif section == "data":
                    if key_len == 1:
                        producer = RecordProducer(columns)
                        yield tuple(columns)
                    elif key_len == 2:
                        if record_data is not None:
                            yield producer.produce(self.graph.hydrate(assembled(record_data)))
                        record_data = []
                    else:
                        record_data.append((key[2:], value))
        if record_data is not None:
            yield producer.produce(self.graph.hydrate(assembled(record_data)))
        self.close()

    def __iter__(self):
        return self

    def __next__(self):
        return next(self.__response_item)

    def next(self):
        return self.__next__()

    def close(self):
        """ Close results and free resources.
        """
        self.__response.close()


class Record(object):
    """ A single row of a Cypher execution result.
    """

    __producer__ = None

    def __init__(self, values):
        columns = self.__producer__.columns
        for i, column in enumerate(columns):
            setattr(self, column, values[i])

    def __repr__(self):
        out = ""
        columns = self.__producer__.columns
        if columns:
            table = TextTable(columns, border=True)
            table.append([getattr(self, column) for column in columns])
            out = repr(table)
        return out

    def __eq__(self, other):
        return vars(self) == vars(other)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __len__(self):
        return self.__producer__.__len__()

    def __getitem__(self, item):
        if is_string(item):
            return getattr(self, item)
        elif is_integer(item):
            return getattr(self, self.__producer__.columns[item])
        else:
            raise LookupError(item)


class RecordProducer(object):

    def __init__(self, columns):
        self.__columns = tuple(column for column in columns if not column.startswith("_"))
        self.__len = len(self.__columns)
        dct = dict.fromkeys(self.__columns)
        dct["__producer__"] = self
        self.__type = type(xstr("Record"), (Record,), dct)

    def __repr__(self):
        return "RecordProducer(columns=%r)" % (self.__columns,)

    def __len__(self):
        return self.__len

    @property
    def columns(self):
        return self.__columns

    def produce(self, values):
        """ Produce a record from a set of values.
        """
        return self.__type(values)


class TransactionFinished(Exception):
    """ Raised when actions are attempted against a finished Transaction.
    """

    def __init__(self):
        pass

    def __repr__(self):
        return "Transaction finished"
