#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2015, Nigel Small
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

""" Usage: {script} [«options»] «statement» [ [«options»] «statement» ... ]

Execute a Cypher statement against a Neo4j database.

General Options:
  -? --help              display this help text
  -A --auth «user:pass»  set auth details

Parameter Options:
  -f «parameter-file»
  -p «name» «value»

Environment:
  NEO4J_URI - base URI of Neo4j database, e.g. http://localhost:7474

Report bugs to nigel@py2neo.org
"""


from collections import OrderedDict
import json
import logging
import os
from io import StringIO
from sys import stdout

from py2neo.compat import integer, string, ustr
from py2neo.graph import Node, Relationship, Path, Subgraph, Walkable, Entity
from py2neo.env import NEO4J_URI
from py2neo.http import Resource, authenticate
from py2neo.status import CypherError, Finished
from py2neo.util import is_collection, deprecated


log = logging.getLogger("py2neo.cypher")


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
    parameters = {k: v for k, v in parameters.items() if k not in presub_parameters}
    return statement, parameters


def cypher_request(statement, parameters, **kwparameters):
    s = ustr(statement)
    p = {}

    def add_parameters(params):
        if params:
            for k, v in dict(params).items():
                if isinstance(v, Entity):
                    if v.resource:
                        v = v.resource._id
                    else:
                        raise TypeError("Cannot pass an unbound entity as a parameter")
                p[k] = v

    add_parameters(dict(parameters or {}, **kwparameters))

    s, p = presubstitute(s, p)

    # OrderedDict is used here to avoid statement/parameters ordering bug
    return OrderedDict([
        ("statement", s),
        ("parameters", p),
        ("resultDataContents", ["REST"]),
    ])


class CypherEngine(object):
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

    def __init__(self, transaction_uri):
        self.transaction_uri = transaction_uri

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
        cursor = tx.run(statement, parameters, **kwparameters)
        tx.commit()
        return cursor.evaluate()

    def create(self, g):
        tx = Transaction(self)
        tx.create(g)
        tx.commit()

    def create_unique(self, t):
        tx = Transaction(self)
        tx.create_unique(t)
        tx.commit()

    def degree(self, g):
        tx = Transaction(self)
        value = tx.degree(g)
        tx.commit()
        return value

    def delete(self, g):
        tx = Transaction(self)
        tx.delete(g)
        tx.commit()

    def separate(self, g):
        tx = Transaction(self)
        tx.separate(g)
        tx.commit()

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
        :rtype: :class:`py2neo.cypher.Cursor`
        """
        tx = Transaction(self)
        result = tx.run(statement, parameters, **kwparameters)
        tx.commit()
        return result


class Transaction(object):
    """ A transaction is a transient resource that allows multiple Cypher
    statements to be executed within a single server transaction.
    """

    error_class = CypherError

    def __init__(self, cypher):
        log.info("begin")
        self.statements = []
        self.results = []
        uri = cypher.transaction_uri
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
        raw = rs.content
        rs.close()
        self.statements = []
        if "commit" in raw:
            self._commit = Resource(raw["commit"])
        for raw_error in raw["errors"]:
            raise self.error_class.hydrate(raw_error)
        for raw_result in raw["results"]:
            result = self.results.pop(0)
            result._hydrate = hydrate
            result._process(raw_result)

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
        self.statements.append(cypher_request(statement, parameters, **kwparameters))
        result = Cursor(self.graph, self, hydrate=True)
        self.results.append(result)
        return result

    def evaluate(self, statement, parameters=None, **kwparameters):
        return self.run(statement, parameters, **kwparameters).evaluate(0)

    def create(self, g):
        try:
            nodes = list(g.nodes())
            relationships = list(g.relationships())
        except AttributeError:
            raise TypeError("Object %r is not graphy" % g)
        reads = []
        writes = []
        parameters = {}
        returns = {}
        for i, node in enumerate(nodes):
            node_id = "a%d" % i
            param_id = "x%d" % i
            if node.resource:
                reads.append("MATCH (%s) WHERE id(%s)={%s}" % (node_id, node_id, param_id))
                parameters[param_id] = node.resource._id
            else:
                label_string = "".join(":" + cypher_escape(label)
                                       for label in sorted(node.labels()))
                writes.append("CREATE (%s%s {%s})" % (node_id, label_string, param_id))
                parameters[param_id] = dict(node)
                node._set_resource_pending(self)
            returns[node_id] = node
        for i, relationship in enumerate(relationships):
            if not relationship.resource:
                rel_id = "r%d" % i
                start_node_id = "a%d" % nodes.index(relationship.start_node())
                end_node_id = "a%d" % nodes.index(relationship.end_node())
                type_string = cypher_escape(relationship.type())
                param_id = "y%d" % i
                writes.append("CREATE UNIQUE (%s)-[%s:%s]->(%s) SET %s={%s}" %
                              (start_node_id, rel_id, type_string, end_node_id, rel_id, param_id))
                parameters[param_id] = dict(relationship)
                returns[rel_id] = relationship
                relationship._set_resource_pending(self)
        statement = "\n".join(reads + writes + ["RETURN %s LIMIT 1" % ", ".join(returns)])
        result = self.run(statement, parameters)
        result.cache.update(returns)

    def create_unique(self, t):
        if not isinstance(t, Walkable):
            raise ValueError("Object %r is not walkable" % t)
        if not any(node.resource for node in t.nodes()):
            raise ValueError("At least one node must be bound")
        matches = []
        pattern = []
        writes = []
        parameters = {}
        returns = {}
        node = None
        for i, entity in enumerate(t.walk()):
            if i % 2 == 0:
                # node
                node_id = "a%d" % i
                param_id = "x%d" % i
                if entity.resource:
                    matches.append("MATCH (%s) "
                                   "WHERE id(%s)={%s}" % (node_id, node_id, param_id))
                    pattern.append("(%s)" % node_id)
                    parameters[param_id] = entity.resource._id
                else:
                    label_string = "".join(":" + cypher_escape(label)
                                           for label in sorted(entity.labels()))
                    pattern.append("(%s%s {%s})" % (node_id, label_string, param_id))
                    parameters[param_id] = dict(entity)
                    entity._set_resource_pending(self)
                returns[node_id] = node = entity
            else:
                # relationship
                rel_id = "r%d" % i
                param_id = "x%d" % i
                type_string = cypher_escape(entity.type())
                template = "-[%s:%s]->" if entity.start_node() == node else "<-[%s:%s]-"
                pattern.append(template % (rel_id, type_string))
                writes.append("SET %s={%s}" % (rel_id, param_id))
                parameters[param_id] = dict(entity)
                if not entity.resource:
                    entity._set_resource_pending(self)
                returns[rel_id] = entity
        statement = "\n".join(matches + ["CREATE UNIQUE %s" % "".join(pattern)] + writes +
                              ["RETURN %s LIMIT 1" % ", ".join(returns)])
        result = self.run(statement, parameters)
        result.cache.update(returns)

    def degree(self, g):
        try:
            nodes = list(g.nodes())
        except AttributeError:
            raise TypeError("Object %r is not graphy" % g)
        node_ids = []
        for i, node in enumerate(nodes):
            resource = node.resource
            if resource:
                node_ids.append(resource._id)
        statement = "MATCH (a)-[r]-() WHERE id(a) IN {x} RETURN count(DISTINCT r)"
        parameters = {"x": node_ids}
        return self.evaluate(statement, parameters)

    def delete(self, g):
        try:
            nodes = list(g.nodes())
            relationships = list(g.relationships())
        except AttributeError:
            raise TypeError("Object %r is not graphy" % g)
        matches = []
        deletes = []
        parameters = {}
        for i, relationship in enumerate(relationships):
            if relationship.resource:
                rel_id = "r%d" % i
                param_id = "y%d" % i
                matches.append("MATCH ()-[%s]->() "
                               "WHERE id(%s)={%s}" % (rel_id, rel_id, param_id))
                deletes.append("DELETE %s" % rel_id)
                parameters[param_id] = relationship.resource._id
                relationship._del_resource()
        for i, node in enumerate(nodes):
            if node.resource:
                node_id = "a%d" % i
                param_id = "x%d" % i
                matches.append("MATCH (%s) "
                               "WHERE id(%s)={%s}" % (node_id, node_id, param_id))
                deletes.append("DELETE %s" % node_id)
                parameters[param_id] = node.resource._id
                node._del_resource()
        statement = "\n".join(matches + deletes)
        self.run(statement, parameters)

    def separate(self, g):
        try:
            relationships = list(g.relationships())
        except AttributeError:
            raise TypeError("Object %r is not graphy" % g)
        matches = []
        deletes = []
        parameters = {}
        for i, relationship in enumerate(relationships):
            if relationship.resource:
                rel_id = "r%d" % i
                param_id = "y%d" % i
                matches.append("MATCH ()-[%s]->() "
                               "WHERE id(%s)={%s}" % (rel_id, rel_id, param_id))
                deletes.append("DELETE %s" % rel_id)
                parameters[param_id] = relationship.resource._id
                relationship._del_resource()
        statement = "\n".join(matches + deletes)
        self.run(statement, parameters)

    def finished(self):
        """ Indicates whether or not this transaction has been completed or is
        still open.

        :return: :py:const:`True` if this transaction has finished,
                 :py:const:`False` otherwise
        """
        return self._finished


class Cursor(object):
    """ A navigable reader for the stream of records made available from running
    a Cypher statement.
    """

    def __init__(self, graph, transaction=None, hydrate=False):
        assert transaction is None or isinstance(transaction, Transaction)
        self.graph = graph
        self.transaction = transaction
        self._keys = ()
        self._records = []
        self._position = 0
        self._processed = False
        self._hydrate = hydrate     # TODO  hydrate to record or leave raw
        self.cache = {}

    def __repr__(self):
        return "<Cursor position=%r keys=%r>" % (self._position, self.keys())

    def __len__(self):
        record = self.current()
        if record is None:
            raise TypeError("No current record")
        else:
            return len(record)

    def __getitem__(self, item):
        record = self.current()
        if record is None:
            raise TypeError("No current record")
        else:
            return record[item]

    def __iter__(self):
        record = self.current()
        if record is None:
            raise TypeError("No current record")
        else:
            return iter(record)

    def _process(self, raw):
        self._keys = keys = tuple(raw["columns"])
        if self._hydrate:
            hydrate = self.graph.hydrate
            records = []
            for record in raw["data"]:
                values = []
                for i, value in enumerate(record["rest"]):
                    key = keys[i]
                    cached = self.cache.get(key)
                    values.append(hydrate(value, inst=cached))
                records.append(Record(keys, values))
            self._records = records
        else:
            self._records = [values["rest"] for values in raw["data"]]
        self._processed = True

    def keys(self):
        """ Return the keys for the currently selected record.
        """
        if self._position == 0:
            return None
        else:
            return self._keys

    def position(self):
        """ Return the current cursor position. Position zero indicates
        that no record is currently selected, position one is that of
        the first record available, and so on.
        """
        return self._position

    def move(self, amount=1):
        """ Attempt to move the cursor one position forward (or by
        another amount if explicitly specified). The cursor will move
        position by up to, but never more than, the amount specified.
        If not enough scope for movement remains, only that remainder
        will be consumed. The total amount moved is returned.

        :param amount: the amount by which to move the cursor
        :return: the amount that the cursor was able to move
        """
        if not self._processed:
            self.transaction.process()
        amount = int(amount)
        step = 1 if amount >= 0 else -1
        moved = 0
        record_count = len(self._records)
        while moved != amount:
            position = self._position
            new_position = position + step
            if 0 <= new_position <= record_count:
                self._position = new_position
                moved += step
            else:
                break
        return moved

    def current(self, *keys):
        """ Return the current record.

        :param keys:
        :return:
        """
        if self._position == 0:
            return None
        elif keys:
            return self._records[self._position - 1].select(*keys)
        else:
            return self._records[self._position - 1]

    def select(self, *keys):
        """ Fetch and return the next record, if available.

        :param keys:
        :return:
        """
        if self.move():
            return self.current(*keys)
        else:
            return None

    def collect(self, *keys):
        """ Consume and yield all remaining records.

        :param keys:
        :return:
        """
        while self.move():
            yield self.current(*keys)

    def evaluate(self, key=0):
        """ Select the next available record and return the value from
        its first field (or another field if explicitly specified).

        :param key:
        :return:
        """
        record = self.select()
        if record is None:
            return None
        else:
            return record[key]

    def close(self):
        """ Close this cursor and free up all associated resources.
        """
        pass

    def dump(self, out=None, keys=None):
        """ Consume all records from this cursor and write in tabular
        form to the console.

        :param out:
        :param keys:
        """
        if out is None:
            out = stdout
        if keys:
            records = list(self.collect(*keys))
        else:
            records = list(self.collect())
            keys = self._keys
        widths = [len(key) for key in keys]
        for record in records:
            for i, value in enumerate(record):
                widths[i] = max(widths[i], len(ustr(value)))
        templates = [u" {:%d} " % width for width in widths]
        out.write(u"".join(templates[i].format(key) for i, key in enumerate(keys)))
        out.write(u"\n")
        out.write(u"".join("-" * (width + 2) for width in widths))
        out.write(u"\n")
        for i, record in enumerate(records):
            out.write(u"".join(templates[i].format(value) for i, value in enumerate(record)))
            out.write(u"\n")


class Record(tuple, Subgraph):

    def __new__(cls, keys, values):
        if len(keys) == len(values):
            return super(Record, cls).__new__(cls, values)
        else:
            raise ValueError("Keys and values must be of equal length")

    def __init__(self, keys, values):
        self.__keys = tuple(keys)
        nodes = []
        relationships = []
        for value in values:
            if hasattr(value, "nodes"):
                nodes.extend(value.nodes())
            if hasattr(value, "relationships"):
                relationships.extend(value.relationships())
        Subgraph.__init__(self, nodes, relationships)
        self.__repr = None

    def __repr__(self):
        r = self.__repr
        if r is None:
            s = ["("]
            for i, key in enumerate(self.__keys):
                if i > 0:
                    s.append(", ")
                s.append(repr(key))
                s.append(": ")
                s.append(repr(self[i]))
            s.append(")")
            r = self.__repr = "".join(s)
        return r

    def __getitem__(self, item):
        if isinstance(item, string):
            try:
                return tuple.__getitem__(self, self.__keys.index(item))
            except ValueError:
                raise KeyError(item)
        elif isinstance(item, slice):
            return self.__class__(self.__keys[item.start:item.stop],
                                  tuple.__getitem__(self, item))
        else:
            return tuple.__getitem__(self, item)

    def __getslice__(self, i, j):
        return self.__class__(self.__keys[i:j], tuple.__getslice__(self, i, j))

    def keys(self):
        return self.__keys

    def values(self):
        return tuple(self)

    def select(self, *keys):
        return Record(keys, [self[key] for key in keys])


class CypherCommandLine(object):

    def __init__(self, graph):
        self.parameters = {}
        self.parameter_filename = None
        self.graph = graph
        self.tx = None

    def begin(self):
        self.tx = self.graph.cypher.begin()

    def set_parameter(self, key, value):
        try:
            self.parameters[key] = json.loads(value)
        except ValueError:
            self.parameters[key] = value

    def set_parameter_filename(self, filename):
        self.parameter_filename = filename

    def run(self, statement):
        import codecs
        cursors = []
        if self.parameter_filename:
            keys = None
            with codecs.open(self.parameter_filename, encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if keys is None:
                        keys = line.split(",")
                    elif line:
                        values = json.loads("[" + line + "]")
                        p = dict(self.parameters)
                        p.update(zip(keys, values))
                        cursors.append(self.tx.run(statement, p))
        else:
            cursors.append(self.tx.run(statement, self.parameters))
        self.tx.process()
        return cursors

    def commit(self):
        self.tx.commit()


class CypherWriter(object):
    """ Writer for Cypher data. This can be used to write to any
    file-like object, such as standard output.
    """

    safe_first_chars = u"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz_"
    safe_chars = u"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz_"

    default_sequence_separator = u","
    default_key_value_separator = u":"

    def __init__(self, file=None, **kwargs):
        self.file = file or stdout
        self.sequence_separator = kwargs.get("sequence_separator", self.default_sequence_separator)
        self.key_value_separator = \
            kwargs.get("key_value_separator", self.default_key_value_separator)

    def write(self, obj):
        """ Write any entity, value or collection.
        """
        if obj is None:
            pass
        elif isinstance(obj, Node):
            self.write_node(obj)
        elif isinstance(obj, Relationship):
            self.write_relationship(obj)
        elif isinstance(obj, Path):
            self.write_walkable(obj)
        elif isinstance(obj, dict):
            self.write_map(obj)
        elif is_collection(obj):
            self.write_list(obj)
        else:
            self.write_value(obj)

    def write_value(self, value):
        """ Write a value.
        """
        self.file.write(ustr(json.dumps(value, ensure_ascii=False)))

    def write_identifier(self, identifier):
        """ Write an identifier.
        """
        if not identifier:
            raise ValueError("Invalid identifier")
        identifier = ustr(identifier)
        safe = (identifier[0] in self.safe_first_chars and
                all(ch in self.safe_chars for ch in identifier[1:]))
        if not safe:
            self.file.write(u"`")
            self.file.write(identifier.replace(u"`", u"``"))
            self.file.write(u"`")
        else:
            self.file.write(identifier)

    def write_list(self, collection):
        """ Write a list.
        """
        self.file.write(u"[")
        link = u""
        for value in collection:
            self.file.write(link)
            self.write(value)
            link = self.sequence_separator
        self.file.write(u"]")

    def write_literal(self, text):
        """ Write literal text.
        """
        self.file.write(ustr(text))

    def write_map(self, mapping):
        """ Write a map.
        """
        self.file.write(u"{")
        link = u""
        for key, value in sorted(dict(mapping).items()):
            self.file.write(link)
            self.write_identifier(key)
            self.file.write(self.key_value_separator)
            self.write(value)
            link = self.sequence_separator
        self.file.write(u"}")

    def write_node(self, node, name=None, full=True):
        """ Write a node.
        """
        self.file.write(u"(")
        if name is None:
            from py2neo.graph import entity_name
            name = entity_name(node)
        self.write_identifier(name)
        if full:
            for label in sorted(node.labels()):
                self.write_literal(u":")
                self.write_identifier(label)
            if node:
                self.file.write(u" ")
                self.write_map(dict(node))
        self.file.write(u")")

    def write_relationship(self, relationship, name=None):
        """ Write a relationship (including nodes).
        """
        self.write_node(relationship.start_node(), full=False)
        self.file.write(u"-")
        self.write_relationship_detail(relationship, name)
        self.file.write(u"->")
        self.write_node(relationship.end_node(), full=False)

    def write_relationship_detail(self, relationship, name=None):
        """ Write a relationship (excluding nodes).
        """
        self.file.write(u"[")
        if name is not None:
            self.write_identifier(name)
        if type:
            self.file.write(u":")
            self.write_identifier(relationship.type())
        if relationship:
            self.file.write(u" ")
            self.write_map(relationship)
        self.file.write(u"]")

    def write_subgraph(self, subgraph):
        """ Write a subgraph.
        """
        self.write_literal("{")
        for i, node in enumerate(subgraph.nodes()):
            if i > 0:
                self.write_literal(", ")
            self.write_node(node)
        for relationship in subgraph.relationships():
            self.write_literal(", ")
            self.write_relationship(relationship)
        self.write_literal("}")

    def write_walkable(self, walkable):
        """ Write a walkable.
        """
        nodes = walkable.nodes()
        for i, relationship in enumerate(walkable):
            node = nodes[i]
            self.write_node(node, full=False)
            forward = relationship.start_node() == node
            self.file.write(u"-" if forward else u"<-")
            self.write_relationship_detail(relationship)
            self.file.write(u"->" if forward else u"-")
        self.write_node(nodes[-1], full=False)


def cypher_escape(identifier):
    """ Escape a Cypher identifier in backticks.

    ::

        >>> cypher_escape("this is a `label`")
        '`this is a ``label```'

    """
    s = StringIO()
    writer = CypherWriter(s)
    writer.write_identifier(identifier)
    return s.getvalue()


def cypher_repr(obj):
    """ Generate the Cypher representation of an object.
    """
    s = StringIO()
    writer = CypherWriter(s)
    writer.write(obj)
    return s.getvalue()


def main():
    import sys
    from py2neo.graph import DBMS
    script, args = sys.argv[0], sys.argv[1:]
    if not args:
        args = ["-?"]
    uri = NEO4J_URI.resolve("/")
    dbms = DBMS(uri.string)
    out = sys.stdout
    command_line = CypherCommandLine(dbms.graph)
    while args:
        arg = args.pop(0)
        if arg.startswith("-"):
            if arg in ("-?", "--help"):
                sys.stderr.write(__doc__.format(script=os.path.basename(script)))
                sys.stderr.write("\n")
                sys.exit(0)
            elif arg in ("-A", "--auth"):
                user_name, password = args.pop(0).partition(":")[0::2]
                authenticate(dbms.uri.host_port, user_name, password)
            elif arg in ("-p", "--parameter"):
                key = args.pop(0)
                value = args.pop(0)
                command_line.set_parameter(key, value)
            elif arg in ("-f",):
                command_line.set_parameter_filename(args.pop(0))
            else:
                raise ValueError("Unrecognised option %s" % arg)
        else:
            if not command_line.tx:
                command_line.begin()
            try:
                cursors = command_line.run(arg)
            except CypherError as error:
                sys.stderr.write("%s: %s\n\n" % (error.__class__.__name__, error.args[0]))
            else:
                for cursor in cursors:
                    cursor.dump(out)
                    out.write("\n")
    if command_line.tx:
        try:
            command_line.commit()
        except CypherError as error:
            sys.stderr.write(error.args[0])
            sys.stderr.write("\n")


if __name__ == "__main__":
    main()
