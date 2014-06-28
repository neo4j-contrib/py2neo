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


from py2neo.packages.jsonstream import assembled, grouped
from py2neo.util import ustr


class CypherResults(object):
    """ A static set of results from a Cypher query.
    """

    # TODO: refactor
    @classmethod
    def _hydrated(cls, graph, data):
        """ Takes assembled data...
        """
        producer = RecordProducer(data["columns"])
        return [
            producer.produce(graph.hydrate(row))
            for row in data["data"]
        ]

    def __init__(self, columns, data):
        self.columns = columns
        self.data = data

    def __repr__(self):
        column_widths = [len(column) for column in self.columns]
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
