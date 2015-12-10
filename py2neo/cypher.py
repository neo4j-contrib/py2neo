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

from py2neo import Bindable, Resource, Node, Relationship, Subgraph, Path, Finished, authenticate
from py2neo.env import NEO4J_URI
from py2neo.compat import integer, xstr, ustr
from py2neo.status import CypherError, TransactionError
from py2neo.primitive import TraversableSubgraph, Record
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
                if isinstance(v, (Node, Relationship)):
                    v = v._id
                p[k] = v

    if hasattr(statement, "parameters"):
        add_parameters(statement.parameters)
    add_parameters(dict(parameters or {}, **kwparameters))

    s, p = presubstitute(s, p)

    # OrderedDict is used here to avoid statement/parameters ordering bug
    return OrderedDict([
        ("statement", s),
        ("parameters", p),
        ("resultDataContents", ["REST"]),
    ])


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

    def create(self, g):
        tx = Transaction(self)
        tx.create(g)
        tx.commit()

    def create_unique(self, t):
        tx = Transaction(self)
        tx.create_unique(t)
        tx.commit()

    def delete(self, g):
        tx = Transaction(self)
        tx.delete(g)
        tx.commit()

    def detach(self, g):
        tx = Transaction(self)
        tx.detach(g)
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
        result = Result(self.graph, self, hydrate=True)
        self.results.append(result)
        return result

    def evaluate(self, statement, parameters=None, **kwparameters):
        return self.run(statement, parameters, **kwparameters).value()

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
            if node.bound:
                reads.append("MATCH (%s) WHERE id(%s)={%s}" % (node_id, node_id, param_id))
                parameters[param_id] = node._id
            else:
                label_string = "".join(":" + cypher_escape(label)
                                       for label in sorted(node.labels()))
                writes.append("CREATE (%s%s {%s})" % (node_id, label_string, param_id))
                parameters[param_id] = dict(node)
                node.set_bind_pending(self)
            returns[node_id] = node
        for i, relationship in enumerate(relationships):
            if not relationship.bound:
                rel_id = "r%d" % i
                start_node_id = "a%d" % nodes.index(relationship.start_node())
                end_node_id = "a%d" % nodes.index(relationship.end_node())
                type_string = cypher_escape(relationship.type())
                param_id = "y%d" % i
                writes.append("CREATE UNIQUE (%s)-[%s:%s]->(%s) SET %s={%s}" %
                              (start_node_id, rel_id, type_string, end_node_id, rel_id, param_id))
                parameters[param_id] = dict(relationship)
                returns[rel_id] = relationship
                relationship.set_bind_pending(self)
        statement = "\n".join(reads + writes + ["RETURN %s LIMIT 1" % ", ".join(returns)])
        result = self.run(statement, parameters)
        result.cache.update(returns)

    def create_unique(self, t):
        if not isinstance(t, TraversableSubgraph):
            raise ValueError("Object %r is not traversable" % t)
        if not any(node.bound for node in t.nodes()):
            raise ValueError("At least one node must be bound")
        matches = []
        pattern = []
        writes = []
        parameters = {}
        returns = {}
        node = None
        for i, entity in enumerate(t.traverse()):
            if i % 2 == 0:
                # node
                node_id = "a%d" % i
                param_id = "x%d" % i
                if entity.bound:
                    matches.append("MATCH (%s) "
                                   "WHERE id(%s)={%s}" % (node_id, node_id, param_id))
                    pattern.append("(%s)" % node_id)
                    parameters[param_id] = entity._id
                else:
                    label_string = "".join(":" + cypher_escape(label)
                                           for label in sorted(entity.labels()))
                    pattern.append("(%s%s {%s})" % (node_id, label_string, param_id))
                    parameters[param_id] = dict(entity)
                    entity.set_bind_pending(self)
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
                if not entity.bound:
                    entity.set_bind_pending(self)
                returns[rel_id] = entity
        statement = "\n".join(matches + ["CREATE UNIQUE %s" % "".join(pattern)] + writes +
                              ["RETURN %s LIMIT 1" % ", ".join(returns)])
        result = self.run(statement, parameters)
        result.cache.update(returns)

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
            if relationship.bound:
                rel_id = "r%d" % i
                param_id = "y%d" % i
                matches.append("MATCH ()-[%s]->() "
                               "WHERE id(%s)={%s}" % (rel_id, rel_id, param_id))
                deletes.append("DELETE %s" % rel_id)
                parameters[param_id] = relationship._id
                relationship.unbind()
        for i, node in enumerate(nodes):
            if node.bound:
                node_id = "a%d" % i
                param_id = "x%d" % i
                matches.append("MATCH (%s) "
                               "WHERE id(%s)={%s}" % (node_id, node_id, param_id))
                deletes.append("DELETE %s" % node_id)
                parameters[param_id] = node._id
                node.unbind()
        statement = "\n".join(matches + deletes)
        self.run(statement, parameters)

    def detach(self, g):
        try:
            relationships = list(g.relationships())
        except AttributeError:
            raise TypeError("Object %r is not graphy" % g)
        matches = []
        deletes = []
        parameters = {}
        for i, relationship in enumerate(relationships):
            if relationship.bound:
                rel_id = "r%d" % i
                param_id = "y%d" % i
                matches.append("MATCH ()-[%s]->() "
                               "WHERE id(%s)={%s}" % (rel_id, rel_id, param_id))
                deletes.append("DELETE %s" % rel_id)
                parameters[param_id] = relationship._id
                relationship.unbind()
        statement = "\n".join(matches + deletes)
        self.run(statement, parameters)

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

    def __init__(self, graph, transaction=None, hydrate=False):
        assert transaction is None or isinstance(transaction, Transaction)
        self.graph = graph
        self.transaction = transaction
        self._keys = []
        self._records = []
        self._processed = False
        self._hydrate = hydrate     # TODO  hydrate to record or leave raw
        self.cache = {}

    def __repr__(self):
        return "<Result>"

    def __str__(self):
        return xstr(self.__unicode__())

    def __unicode__(self):
        self._ensure_processed()
        widths = [len(key) for key in self._keys]
        for record in self._records:
            for i, value in enumerate(record):
                widths[i] = max(widths[i], len(ustr(value)))
        templates = [u" {:%d} " % width for width in widths]
        out = [u"".join(templates[i].format(key) for i, key in enumerate(self._keys)),
               u"".join("-" * (width + 2) for width in widths)]
        for i, record in enumerate(self._records):
            out.append("".join(templates[i].format(value) for i, value in enumerate(record)))
        return u"\n".join(out) + u"\n"

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

    def _process(self, raw):
        self._keys = keys = raw["columns"]
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
        results = []
        if self.parameter_filename:
            columns = None
            with codecs.open(self.parameter_filename, encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if columns is None:
                        columns = line.split(",")
                    elif line:
                        values = json.loads("[" + line + "]")
                        p = dict(self.parameters)
                        p.update(zip(columns, values))
                        results.append(self.tx.run(statement, p))
        else:
            results.append(self.tx.run(statement, self.parameters))
        self.tx.process()
        return results

    def commit(self):
        self.tx.commit()


class CypherWriter(object):
    """ Writer for Cypher data. This can be used to write to any
    file-like object, such as standard output::

        >>> from py2neo.cypher import CypherWriter
        >>> from py2neo import Node
        >>> from sys import stdout
        >>> writer = CypherWriter(stdout)
        >>> writer.write(Node("Person", name="Alice"))
        (:Person {name:"Alice"})

    """

    safe_first_chars = u"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz_"
    safe_chars = u"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz_"

    default_sequence_separator = ","
    default_key_value_separator = ":"

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
            self.write_relationship(obj, properties=obj)
        elif isinstance(obj, Path):
            self.write_path(obj)
        elif isinstance(obj, Record):
            self.write_record(obj)
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
            self.file.write("`")
            self.file.write(identifier.replace("`", "``"))
            self.file.write("`")
        else:
            self.file.write(identifier)

    def write_list(self, collection):
        """ Write a list.
        """
        self.file.write("[")
        link = ""
        for value in collection:
            self.file.write(link)
            self.write(value)
            link = self.sequence_separator
        self.file.write("]")

    def write_literal(self, text):
        """ Write literal text.
        """
        self.file.write(ustr(text))

    def write_map(self, mapping):
        """ Write a map.
        """
        self.file.write("{")
        link = ""
        for key, value in sorted(dict(mapping).items()):
            self.file.write(link)
            self.write_identifier(key)
            self.file.write(self.key_value_separator)
            self.write(value)
            link = self.sequence_separator
        self.file.write("}")

    def write_node(self, node, name=None, properties=None):
        """ Write a node.
        """
        self.file.write("(")
        if name:
            self.write_identifier(name)
        if node is not None:
            for label in sorted(node.labels()):
                self.write_literal(":")
                self.write_identifier(label)
            if properties is None:
                if node:
                    if name or node.labels():
                        self.file.write(" ")
                    self.write_map(dict(node))
            else:
                self.file.write(" ")
                self.write(properties)
        self.file.write(")")

    def write_path(self, path):
        """ Write a :class:`py2neo.Path`.
        """
        nodes = path.nodes()
        for i, relationship in enumerate(path):
            node = nodes[i]
            self.write_node(node)
            forward = relationship.start_node() == node
            if forward:
                self.file.write("-")
            else:
                self.file.write("<-")
            self.write_relationship_detail(type=relationship.type(), properties=relationship)
            if forward:
                self.file.write("->")
            else:
                self.file.write("-")
        self.write_node(nodes[-1])

    def write_relationship(self, relationship, name=None, properties=None):
        """ Write a relationship (including nodes).
        """
        self.write_node(relationship.start_node())
        self.file.write("-")
        self.write_relationship_detail(name, relationship.type(), properties)
        self.file.write("->")
        self.write_node(relationship.end_node())

    def write_relationship_detail(self, name=None, type=None, properties=None):
        """ Write a relationship (excluding nodes).
        """
        self.file.write("[")
        if name:
            self.write_identifier(name)
        if type:
            self.file.write(":")
            self.write_identifier(type)
        if properties:
            self.file.write(" ")
            self.write_map(properties)
        self.file.write("]")


def cypher_escape(identifier):
    """ Escape a Cypher identifier in backticks.

    ::

        >>> cypher_escape("this is a `label`")
        '`this is a ``label```'

    """
    string = StringIO()
    writer = CypherWriter(string)
    writer.write_identifier(identifier)
    return string.getvalue()


def cypher_repr(obj):
    """ Generate the Cypher representation of an object.
    """
    string = StringIO()
    writer = CypherWriter(string)
    writer.write(obj)
    return string.getvalue()


def main():
    import sys
    from py2neo.core import ServiceRoot
    script, args = sys.argv[0], sys.argv[1:]
    if not args:
        args = ["-?"]
    uri = NEO4J_URI.resolve("/")
    service_root = ServiceRoot(uri.string)
    out = sys.stdout
    command_line = CypherCommandLine(service_root.graph)
    while args:
        arg = args.pop(0)
        if arg.startswith("-"):
            if arg in ("-?", "--help"):
                sys.stderr.write(__doc__.format(script=os.path.basename(script)))
                sys.stderr.write("\n")
                sys.exit(0)
            elif arg in ("-A", "--auth"):
                user_name, password = args.pop(0).partition(":")[0::2]
                authenticate(service_root.uri.host_port, user_name, password)
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
                results = command_line.run(arg)
            except CypherError as error:
                sys.stderr.write("%s: %s\n\n" % (error.__class__.__name__, error.args[0]))
            else:
                for result in results:
                    out.write(ustr(result))
                    out.write("\n")
    if command_line.tx:
        try:
            command_line.commit()
        except TransactionError as error:
            sys.stderr.write(error.args[0])
            sys.stderr.write("\n")


if __name__ == "__main__":
    main()
