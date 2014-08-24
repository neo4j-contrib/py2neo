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

from py2neo.core import Service, Resource, Node, Rel, Relationship
from py2neo.cypher.error import CypherError, TransactionError, TransactionFinished
from py2neo.packages.jsonstream import assembled, grouped
from py2neo.util import ustr

__all__ = ["CypherResource", "CypherTransaction", "CypherResults", "IterableCypherResults",
           "Record", "RecordProducer"]


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
        log.debug("Statement: %r", statement)
        payload = {"query": statement}
        if parameters:
            payload["params"] = {}
            for key, value in parameters.items():
                if isinstance(value, (Node, Rel, Relationship)):
                    value = value._id
                payload["params"][key] = value
            log.debug("Params: %r", payload["params"])
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
            return results.data[0][0]
        except IndexError:
            return None
        finally:
            response.close()

    def stream(self, statement, parameters=None):
        """ Execute the query and return a result iterator.
        """
        return IterableCypherResults(self.graph, self.post(statement, parameters))

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

    def __init__(self, uri):
        self.statements = []
        self.__begin = Resource(uri)
        self.__begin_commit = Resource(uri + "/commit")
        self.__execute = None
        self.__commit = None
        self.__finished = False

    def __assert_unfinished(self):
        if self.__finished:
            raise TransactionFinished()

    @property
    def finished(self):
        """ Indicates whether or not this transaction has been completed or is
        still open.

        :return: :py:const:`True` if this transaction has finished,
                 :py:const:`False` otherwise
        """
        return self.__finished

    def append(self, statement, parameters=None):
        """ Append a statement to the current queue of statements to be
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
        location = dict(rs.headers).get("location")
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
                raise TransactionError.new(error["code"], error["message"])
        out = []
        for result in j["results"]:
            producer = RecordProducer(result["columns"])
            out.append([
                producer.produce(self.__begin.service_root.graph.hydrate(r["rest"]))
                for r in result["data"]
            ])
        return out

    def execute(self):
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


class CypherResults(object):
    """ A static set of results from a Cypher query.
    """

    @classmethod
    def hydrate(cls, data, graph):
        columns = data["columns"]
        rows = data["data"]
        producer = RecordProducer(columns)
        return cls(columns, [producer.produce(graph.hydrate(row)) for row in rows])

    def __init__(self, columns, data):
        self.columns = columns
        self.data = data

    def __repr__(self):
        column_widths = list(map(len, self.columns))
        for row in self.data:
            for i, value in enumerate(row):
                column_widths[i] = max(column_widths[i], len(str(value)))
        out = [" " + " | ".join(
            column.ljust(column_widths[i])
            for i, column in enumerate(self.columns)
        ) + " "]
        out += ["-" + "-+-".join(
            "-" * column_widths[i]
            for i, column in enumerate(self.columns)
        ) + "-"]
        for row in self.data:
            out.append(" " + " | ".join(ustr(value).ljust(column_widths[i])
                                        for i, value in enumerate(row)) + " ")
        out = "\n".join(out)
        if len(self.data) == 1:
            out += "\n(1 row)\n"
        else:
            out += "\n({0} rows)\n".format(len(self.data))
        return out

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

    def __len__(self):
        return len(self.data)

    def __getitem__(self, item):
        return self.data[item]

    def __iter__(self):
        return iter(self.data)


class IterableCypherResults(object):
    """ An iterable set of results from a Cypher query.

    ::

        query = graph.cypher.query("START n=node(*) RETURN n LIMIT 10")
        for record in query.stream():
            print record[0]

    Each record returned is cast into a :py:class:`namedtuple` with names
    derived from the resulting column names.

    .. note ::
        Results are available as returned from the server and are decoded
        incrementally. This means that there is no need to wait for the
        entire response to be received before processing can occur.
    """

    def __init__(self, graph, response):
        self.__graph = graph
        self._response = response
        self._redo_buffer = []
        self._buffered = self._buffered_results()
        self._columns = None
        self._fetch_columns()
        self._producer = RecordProducer(self._columns)

    def _fetch_columns(self):
        redo = []
        section = []
        for key, value in self._buffered:
            if key and key[0] == "columns":
                section.append((key, value))
            else:
                redo.append((key, value))
                if key and key[0] == "data":
                    break
        self._redo_buffer.extend(redo)
        self._columns = tuple(assembled(section)["columns"])

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def _buffered_results(self):
        for result in self._response:
            while self._redo_buffer:
                yield self._redo_buffer.pop(0)
            yield result

    def __iter__(self):
        for key, section in grouped(self._buffered):
            if key[0] == "data":
                for i, row in grouped(section):
                    yield self._producer.produce(self.__graph.hydrate(assembled(row)))

    @property
    def graph(self):
        return self.__graph

    @property
    def columns(self):
        """ Column names.
        """
        return self._columns

    def close(self):
        """ Close results and free resources.
        """
        self._response.close()


class Record(object):
    """ A single row of a Cypher execution result, holding a sequence of named
    values.
    """

    def __init__(self, producer, values):
        self._producer = producer
        self._values = tuple(values)

    def __repr__(self):
        return "Record(columns={0}, values={1})".format(self._producer.columns, self._values)

    def __getattr__(self, attr):
        return self._values[self._producer.column_indexes[attr]]

    def __getitem__(self, item):
        if isinstance(item, (int, slice)):
            return self._values[item]
        else:
            return self._values[self._producer.column_indexes[item]]

    def __len__(self):
        return len(self._producer.columns)

    @property
    def columns(self):
        """ The column names defined for this record.

        :return: tuple of column names
        """
        return self._producer.columns

    @property
    def values(self):
        """ The values stored in this record.

        :return: tuple of values
        """
        return self._values


class RecordProducer(object):

    def __init__(self, columns):
        self.__columns = tuple(columns)
        self.__column_indexes = dict((b, a) for a, b in enumerate(columns))

    def __repr__(self):
        return "RecordProducer(columns={0})".format(self.__columns)

    @property
    def columns(self):
        return self.__columns

    @property
    def column_indexes(self):
        return self.__column_indexes

    def produce(self, values):
        """ Produce a record from a set of values.
        """
        return Record(self, values)
