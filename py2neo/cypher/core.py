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
    """ A single row of a Cypher execution result, holding a sequence of named
    values.
    """

    def __init__(self, producer, values):
        self.producer = producer
        self.values = tuple(values)

    def __repr__(self):
        return "Record(columns=%r, values=%r)" % (self.producer.columns, self.values)

    def __getattr__(self, attr):
        return self.values[self.producer.column_indexes[attr]]

    def __getitem__(self, item):
        if isinstance(item, (int, slice)):
            return self.values[item]
        else:
            return self.values[self.producer.column_indexes[item]]

    def __len__(self):
        return len(self.producer.columns)

    def __eq__(self, other):
        return self.columns == other.columns and self.values == other.values

    def __ne__(self, other):
        return not self.__eq__(other)

    @property
    def columns(self):
        """ The column names defined for this record.

        :return: tuple of column names
        """
        return self.producer.columns


class RecordProducer(object):

    def __init__(self, columns):
        self.__columns = tuple(columns)
        self.__len = len(self.__columns)
        self.__column_indexes = dict((name, i) for i, name in enumerate(columns))

    def __repr__(self):
        return "RecordProducer(columns=%r)" % (self.__columns,)

    def __len__(self):
        return self.__len

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
