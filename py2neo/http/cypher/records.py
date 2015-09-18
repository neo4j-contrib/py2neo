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


from py2neo import Node, Subgraph, Path
from py2neo.compat import integer, string, xstr
from py2neo.packages.tart.tables import TextTable

__all__ = ["RecordListList", "RecordList", "Record", "RecordProducer"]


class RecordListList(list):
    """ Container for multiple RecordList instances that presents a more
    consistent representation.
    """

    @classmethod
    def from_results(cls, results, graph):
        return cls(RecordList(graph, result["columns"], result["data"]) for result in results)

    def __repr__(self):
        out = []
        for i in self:
            out.append(repr(i))
        return "\n".join(out)


class RecordList(object):
    """ A list of records returned from the execution of a Cypher statement.
    """

    def __init__(self, graph, columns, records):
        self.graph = graph
        self.producer = RecordProducer(columns)
        self.__records = records

    def __repr__(self):
        out = ""
        if self.columns:
            table = TextTable((None,) + self.columns, border=True)
            for i, record in enumerate(self):
                table.append([i + 1] + list(record))
            out = repr(table)
        return out

    def __len__(self):
        return len(self.__records)

    def __getitem__(self, index):
        if isinstance(self.__records[index], list):
            self.__records[index] = self.producer.produce(self.graph.hydrate(self.__records[index]))
        return self.__records[index]

    def __iter__(self):
        for index, record in enumerate(self.__records):
            if isinstance(record, list):
                record = self.__records[index] = self.producer.produce(self.graph.hydrate(record))
            yield record

    @property
    def columns(self):
        return self.producer.columns

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
        for record in self:
            for value in record:
                if isinstance(value, (Node, Path)):
                    entities.append(value)
        return Subgraph(*entities)


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
        if isinstance(item, integer):
            return self.__values__[item]
        elif isinstance(item, string):
            return getattr(self, item)
        else:
            raise LookupError(item)


class RecordProducer(object):

    def __init__(self, columns):
        self.columns = tuple(column for column in columns if not column.startswith("_"))
        dct = dict.fromkeys(self.columns)
        dct["__producer__"] = self
        self.__type = type(xstr("Record"), (Record,), dct)

    def __repr__(self):
        return "RecordProducer(columns=%r)" % (self.columns,)

    def __len__(self):
        return len(self.columns)

    def produce(self, values):
        """ Produce a record from a set of values.
        """
        return self.__type(values)
