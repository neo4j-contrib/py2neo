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

from py2neo import Service, Resource, Node, Rel, Relationship, Subgraph, Path, Finished
from py2neo.cypher.lang import cypher_escape
from py2neo.cypher.error.core import CypherError, CypherTransactionError
from py2neo.packages.jsonstream import assembled
from py2neo.packages.tart.tables import TextTable
from py2neo.util import is_integer, is_string, xstr, ustr, is_collection


__all__ = ["CypherResource", "CypherTransaction", "RecordListList", "RecordList", "RecordStream",
           "Record", "RecordProducer"]


log = logging.getLogger("py2neo.cypher")


def presubstitute(statement, parameters):
    more = True
    while more:
        before, opener, key = statement.partition(u"«")
        if opener:
            key, closer, after = key.partition(u"»")
            try:
                value = parameters.pop(key)
            except KeyError:
                raise KeyError("Expected a presubstitution parameter named %r" % key)
            if is_integer(value):
                value = ustr(value)
            elif isinstance(value, tuple) and all(map(is_integer, value)):
                value = u"%d..%d" % (value[0], value[-1])
            elif is_collection(value):
                value = ":".join(map(cypher_escape, value))
            else:
                value = cypher_escape(value)
            statement = before + value + after
        else:
            more = False
    return statement, parameters


class CypherResource(Service):
    """ Service wrapper for all Cypher functionality, providing access
    to transactions (if available) as well as single statement execution
    and streaming. If the server supports Cypher transactions, these
    will be used for single statement execution; if not, the vanilla
    Cypher endpoint will be used.

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

    def post(
            self, statement, parameters=None, result_formats=None, **kwparameters):
        """ Post a Cypher statement to this resource, optionally with
        parameters.

        :arg statement: A Cypher statement to execute.
        :arg parameters: A dictionary of parameters.
        :arg result_formats: A list of Cypher result formats, defaults to ["REST"]
        :arg kwparameters: Extra parameters supplied by keyword.
        :rtype: :class:`httpstream.Response`
        """
        payload = {"query": statement, "params": {}}
        parameters = dict(parameters or {}, **kwparameters)
        result_formats = result_formats or ["REST"]
        parameters['resultDataContent'] = result_formats

        for key, value in parameters.items():
            if isinstance(value, (Node, Rel, Relationship)):
                value = value._id
            payload["params"][key] = value

        log.info("execute %r %r", payload["query"], payload["params"])
        return self.resource.post(payload)

    def run(self, statement, parameters=None, **kwparameters):
        """ Execute a single Cypher statement, ignoring any return value.

        :arg statement: A Cypher statement to execute.
        :arg parameters: A dictionary of parameters.
        """
        if self.transaction_uri:
            tx = CypherTransaction(self.transaction_uri)
            tx.append(statement, parameters, **kwparameters)
            tx.commit()
        else:
            self.post(statement, parameters, **kwparameters).close()

    def execute(self, statement, parameters=None, **kwparameters):
        """ Execute a single Cypher statement.

        :arg statement: A Cypher statement to execute.
        :arg parameters: A dictionary of parameters.
        :rtype: :class:`py2neo.cypher.RecordList`
        """
        if self.transaction_uri:
            tx = CypherTransaction(self.transaction_uri)
            tx.append(statement, parameters, **kwparameters)
            results = tx.commit()
            return results[0]
        else:
            response = self.post(statement, parameters, **kwparameters)
            try:
                return self.graph.hydrate(response.content)
            finally:
                response.close()

    def execute_one(self, statement, parameters=None, **kwparameters):
        """ Execute a single Cypher statement and return the value from
        the first column of the first record returned.

        :arg statement: A Cypher statement to execute.
        :arg parameters: A dictionary of parameters.
        :return: Single return value or :const:`None`.
        """
        if self.transaction_uri:
            tx = CypherTransaction(self.transaction_uri)
            tx.append(statement, parameters, **kwparameters)
            results = tx.commit()
            try:
                return results[0][0][0]
            except IndexError:
                return None
        else:
            response = self.post(statement, parameters, **kwparameters)
            results = self.graph.hydrate(response.content)
            try:
                return results[0][0]
            except IndexError:
                return None
            finally:
                response.close()

    def execute_for_visualisation(
            self, statement, parameters=None, **kwparameters):
        """ Execute a single cypher query and return the result in graphJSON
        format ready for visualisation.

        The format collates all the nodes and relationships from all columns of
        the result, and also flattens collections of nodes and relationships,
        including paths.

        """
        if self.transaction_uri:
            tx = CypherTransaction(self.transaction_uri)
            tx.append(
                statement, parameters, result_formats=["graph"], **kwparameters)
            results = tx.commit()
            try:
                return [d['graph'] for d in resuts[0]]
            except IndexError:
                return None

        else:
            response = self.post(
                statement, parameters, result_formats=["graph"], **kwparameters)
            results = response.content
            try:
                return [d['graph'] for d in resuts[0]]
            except IndexError:
                return None
            finally:
                response.close()

    def stream(self, statement, parameters=None, **kwparameters):
        """ Execute the query and return a result iterator.

        :arg statement: A Cypher statement to execute.
        :arg parameters: A dictionary of parameters.
        :rtype: :class:`py2neo.cypher.RecordStream`
        """
        return RecordStream(self.graph, self.post(statement, parameters, **kwparameters))

    def begin(self):
        """ Begin a new transaction.

        :rtype: :class:`py2neo.cypher.CypherTransaction`
        """
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
        log.info("begin")
        self.statements = []
        self.__begin = Resource(uri)
        self.__begin_commit = Resource(uri + "/commit")
        self.__execute = None
        self.__commit = None
        self.__finished = False
        self.graph = self.__begin.graph

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.commit()

    def __assert_unfinished(self):
        if self.__finished:
            raise Finished(self)

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

    def append(
            self, statement, parameters=None, result_formats=None,
            **kwparameters):
        """ Add a statement to the current queue of statements to be
        executed.

        :arg statement: the statement to append
        :arg parameters: a dictionary of execution parameters
        :arg result_formats: A list of Cypher result formats, defaults to ["REST"]
        """
        self.__assert_unfinished()

        s = ustr(statement)
        p = {}
        result_formats = result_formats or ["REST"]

        def add_parameters(params):
            if params:
                for k, v in dict(params).items():
                    if isinstance(v, (Node, Rel, Relationship)):
                        v = v._id
                    p[k] = v

        try:
            add_parameters(statement.parameters)
        except AttributeError:
            pass
        add_parameters(dict(parameters or {}, **kwparameters))

        s, p = presubstitute(s, p)

        # OrderedDict is used here to avoid statement/parameters ordering bug
        log.info("append %r %r", s, p)
        self.statements.append(OrderedDict([
            ("statement", s),
            ("parameters", p),
            ("resultDataContents", result_formats),
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

    def process(self):
        """ Send all pending statements to the server for execution, leaving
        the transaction open for further statements. Along with
        :meth:`append <.CypherTransaction.append>`, this method can be used to
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

        :return: list of results from pending statements
        """
        log.info("process")
        return self.post(self.__execute or self.__begin)

    def commit(self):
        """ Send all pending statements to the server for execution and commit
        the transaction.

        :return: list of results from pending statements
        """
        log.info("commit")
        try:
            return self.post(self.__commit or self.__begin_commit)
        finally:
            self.__finished = True

    def rollback(self):
        """ Rollback the current transaction.
        """
        self.__assert_unfinished()
        log.info("rollback")
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
        log.info("result %r %r", columns, len(records))

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

    @property
    def one(self):
        """ The first record from this result, reduced to a single value
        if that record only consists of a single column. If no records
        are available, :const:`None` is returned.
        """
        try:
            record = self[0]
        except IndexError:
            return None
        else:
            if len(record) == 0:
                return None
            elif len(record) == 1:
                return record[0]
            else:
                return record

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

        for record in graph.cypher.stream("MATCH (n) RETURN n LIMIT 10")
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
        log.info("stream %r", self.columns)

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
    """ A simple object containing values from a single row of a Cypher
    result. Each value can be retrieved by column position or name,
    supplied as either an index key or an attribute name.

    Consider the record below::

           | person                     | name
        ---+----------------------------+-------
         1 | (n1:Person {name:"Alice"}) | Alice

    If this record is named ``r``, the following expressions
    are equivalent and will return the value ``'Alice'``::

        r[1]
        r["name"]
        r.name

    """

    __producer__ = None

    def __init__(self, values):
        self.__values__ = tuple(values)
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
        try:
            return vars(self) == vars(other)
        except TypeError:
            return tuple(self) == tuple(other)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __len__(self):
        return len(self.__values__)

    def __iter__(self):
        return iter(self.__values__)

    def __getitem__(self, item):
        if is_integer(item):
            return self.__values__[item]
        elif is_string(item):
            return getattr(self, item)
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
