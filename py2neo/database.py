#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2018, Nigel Small
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

from collections import deque
from datetime import datetime
from time import sleep
from warnings import warn

from py2neo.cypher.writing import cypher_escape
from py2neo.data import Graph, Node, Record, Table
from py2neo.internal.addressing import get_connection_data
from py2neo.internal.caching import ThreadLocalEntityCache
from py2neo.internal.collections import is_collection
from py2neo.internal.compat import string_types, xstr
from py2neo.internal.util import version_tuple, title_case, snake_case
from py2neo.matching import NodeMatcher


update_stats_keys = [
    "constraints_added",
    "constraints_removed",
    "indexes_added",
    "indexes_removed",
    "labels_added",
    "labels_removed",
    "nodes_created",
    "nodes_deleted",
    "properties_set",
    "relationships_deleted",
    "relationships_created",
]


class Database(object):
    """ Accessor for an entire Neo4j graph database installation over
    Bolt or HTTP. Within the py2neo object hierarchy, a :class:`.Database`
    contains a :class:`.Graph` in which most activity occurs. Currently,
    Neo4j only supports one `Graph` per `Database`.

    An explicit URI can be passed to the constructor::

        >>> from py2neo import Database
        >>> db = Database("bolt://camelot.example.com:7687")

    Alternatively, the default value of ``bolt://localhost:7687`` is
    used::

        >>> default_db = Database()
        >>> default_db
        <Database uri='bolt://localhost:7687'>

    """

    _instances = {}

    _driver = None
    _graphs = None

    @classmethod
    def forget_all(cls):
        """ Forget all cached :class:`.Database` details.
        """
        for _, db in cls._instances.items():
            db._driver.close()
            db._driver = None
        cls._instances.clear()

    def __new__(cls, uri=None, **settings):
        connection_data = get_connection_data(uri, **settings)
        key = connection_data["hash"]
        try:
            inst = cls._instances[key]
        except KeyError:
            inst = super(Database, cls).__new__(cls)
            inst._connection_data = connection_data
            from py2neo.internal.http import HTTPDriver
            HTTPDriver.register()
            from neo4j.v1 import GraphDatabase
            inst._driver = GraphDatabase.driver(connection_data["uri"], auth=connection_data["auth"],
                                                encrypted=connection_data["secure"],
                                                user_agent=connection_data["user_agent"])
            inst._graphs = {}
            cls._instances[key] = inst
        return inst

    def __repr__(self):
        class_name = self.__class__.__name__
        data = self._connection_data
        return "<%s uri=%r secure=%r user_agent=%r>" % (class_name, data["uri"], data["secure"], data["user_agent"])

    def __eq__(self, other):
        try:
            return self.uri == other.uri
        except AttributeError:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash(self._connection_data["hash"])

    def __contains__(self, database):
        return database in self._graphs

    def __getitem__(self, database):
        if database == "data" and database not in self._graphs:
            self._graphs[database] = RemoteGraph(**self._connection_data)
        return self._graphs[database]

    def __setitem__(self, database, graph):
        self._graphs[database] = graph

    def __iter__(self):
        yield "data"

    @property
    def driver(self):
        return self._driver

    @property
    def uri(self):
        """ The URI to which this `Database` is connected.
        """
        return self._connection_data["uri"]

    @property
    def default_graph(self):
        """ The default graph exposed by this database.

        :rtype: :class:`.Graph`
        """
        return self["data"]

    def keys(self):
        return list(self)

    def query_jmx(self, namespace, instance=None, name=None, type=None):
        """ Query the JMX service attached to this database.
        """
        d = {}
        for nom, _, attributes in self.default_graph.run("CALL dbms.queryJmx('')"):
            ns, _, terms = nom.partition(":")
            if ns != namespace:
                continue
            terms = dict(tuple(term.partition("=")[0::2]) for term in terms.split(","))
            if instance is not None and instance != terms["instance"]:
                continue
            if name is not None and name != terms["name"]:
                continue
            if type is not None and type != terms["type"]:
                continue
            for attr_name, attr_data in attributes.items():
                attr_value = attr_data.get("value")
                if attr_value == "true":
                    d[attr_name] = True
                elif attr_value == "false":
                    d[attr_name] = False
                elif isinstance(attr_value, string_types) and "." in attr_value:
                    try:
                        d[attr_name] = float(attr_value)
                    except (TypeError, ValueError):
                        d[attr_name] = attr_value
                else:
                    try:
                        d[attr_name] = int(attr_value)
                    except (TypeError, ValueError):
                        d[attr_name] = attr_value
        return d

    @property
    def database_name(self):
        """ Return the name of the active Neo4j database.
        """
        info = self.query_jmx("org.neo4j", name="Kernel")
        return info.get("DatabaseName")

    @property
    def kernel_start_time(self):
        """ Return the time from which this Neo4j instance was in operational mode.
        """
        info = self.query_jmx("org.neo4j", name="Kernel")
        return datetime.fromtimestamp(info["KernelStartTime"] / 1000.0)

    @property
    def kernel_version(self):
        """ Return the version of Neo4j.
        """
        info = self.query_jmx("org.neo4j", name="Kernel")
        version_string = info["KernelVersion"].partition("version:")[-1].partition(",")[0].strip()
        return version_tuple(version_string)

    @property
    def store_creation_time(self):
        """ Return the time when this Neo4j graph store was created.
        """
        info = self.query_jmx("org.neo4j", name="Kernel")
        return datetime.fromtimestamp(info["StoreCreationDate"] / 1000.0)

    @property
    def store_directory(self):
        """ Return the location of the Neo4j store.
        """
        info = self.query_jmx("org.neo4j", name="Kernel")
        return info.get("StoreDirectory")

    @property
    def store_id(self):
        """ Return an identifier that, together with store creation time,
        uniquely identifies this Neo4j graph store.
        """
        info = self.query_jmx("org.neo4j", name="Kernel")
        return info["StoreId"]

    @property
    def primitive_counts(self):
        """ Return a dictionary of estimates of the numbers of different
        kinds of Neo4j primitives.
        """
        return self.query_jmx("org.neo4j", name="Primitive count")

    @property
    def store_file_sizes(self):
        """ Return a dictionary of file sizes for each file in the Neo4j
        graph store.
        """
        return self.query_jmx("org.neo4j", name="Store file sizes")

    @property
    def config(self):
        """ Return a dictionary of the configuration parameters used to
        configure Neo4j.
        """
        return self.query_jmx("org.neo4j", name="Configuration")


class RemoteGraph(Graph):

    uri_schemes = ("http", "https", "bolt", "bolt+routing")

    #: The :class:`.Database` to which this :class:`.Graph` belongs.
    database = None

    #: The :class:`.Schema` resource for this :class:`.Graph`.
    schema = None

    node_cache = ThreadLocalEntityCache()
    relationship_cache = ThreadLocalEntityCache()

    def __new__(cls, uri=None, **settings):
        name = settings.pop("name", "data")
        database = Database(uri, **settings)
        if name in database:
            inst = database[name]
        else:
            inst = object.__new__(cls)
            inst.database = database
            inst.schema = Schema(inst)
            inst.__name__ = name
            database[name] = inst
        return inst

    def __repr__(self):
        return "<RemoteGraph database=%r name=%r>" % (self.database, self.__name__)

    def __eq__(self, other):
        try:
            return self.database == other.database and self.__name__ == other.__name__
        except (AttributeError, TypeError):
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __graph_order__(self):
        return self.evaluate("MATCH (_) RETURN count(_)")

    def __graph_size__(self):
        return self.evaluate("MATCH ()-[_]->() RETURN count(_)")

    def __len__(self):
        return self.__graph_size__()

    def __bool__(self):
        return True

    def __nonzero__(self):
        return True

    def begin(self, autocommit=False):
        """ Begin a new :class:`.Transaction`.

        :param autocommit: if :py:const:`True`, the transaction will
                         automatically commit after the first operation
        """
        return Transaction(self, autocommit)

    def create(self, subgraph):
        """ Run a :meth:`.Transaction.create` operation within a
        :class:`.Transaction`.

        :param subgraph: a :class:`.Node`, :class:`.Relationship` or other
                       :class:`.Subgraph`
        """
        with self.begin() as tx:
            tx.create(subgraph)

    def degree(self, subgraph):
        """ Run a :meth:`.Transaction.degree` operation within an
        `autocommit` :class:`.Transaction`.

        :param subgraph: a :class:`.Node`, :class:`.Relationship` or other
                       :class:`.Subgraph` object
        :return: the total degree of all nodes in the subgraph
        """
        return self.begin(autocommit=True).degree(subgraph)

    def delete(self, subgraph):
        """ Run a :meth:`.Transaction.delete` operation within an
        `autocommit` :class:`.Transaction`. To delete only the
        relationships, use the :meth:`.separate` method.

        :param subgraph: a :class:`.Node`, :class:`.Relationship` or other
                       :class:`.Subgraph` object
        """
        self.begin(autocommit=True).delete(subgraph)

    def delete_all(self):
        """ Delete all nodes and relationships from this :class:`.Graph`.

        .. warning::
            This method will permanently remove **all** nodes and relationships
            from the graph and cannot be undone.
        """
        self.run("MATCH (a) DETACH DELETE a")
        self.node_cache.clear()
        self.relationship_cache.clear()

    def evaluate(self, cypher, parameters=None, **kwparameters):
        """ Run a :meth:`.Transaction.evaluate` operation within an
        `autocommit` :class:`.Transaction`.

        :param cypher: Cypher statement
        :param parameters: dictionary of parameters
        :return: first value from the first record returned or
                 :py:const:`None`.
        """
        return self.begin(autocommit=True).evaluate(cypher, parameters, **kwparameters)

    def exists(self, subgraph):
        """ Run a :meth:`.Transaction.exists` operation within an
        `autocommit` :class:`.Transaction`.

        :param subgraph: a :class:`.Node`, :class:`.Relationship` or other
                       :class:`.Subgraph` object
        :return:
        """
        return self.begin(autocommit=True).exists(subgraph)

    def match(self, start_node=None, rel_type=None, end_node=None, bidirectional=False, limit=None):
        """ Match and return all relationships with specific criteria.

        For example, to find all of Alice's friends::

            for rel in graph.match(start_node=alice, rel_type="FRIEND"):
                print(rel.end_node()["name"])

        :param start_node: start node of relationships to match (:const:`None` means any node)
        :param rel_type: type of relationships to match (:const:`None` means any type)
        :param end_node: end node of relationships to match (:const:`None` means any node)
        :param bidirectional: :const:`True` if reversed relationships should also be included
        :param limit: maximum number of relationships to match (:const:`None` means unlimited)
        """
        clauses = []
        returns = []
        if start_node is None and end_node is None:
            clauses.append("MATCH (a)")
            parameters = {}
            returns.append("a")
            returns.append("b")
        elif end_node is None:
            clauses.append("MATCH (a) WHERE id(a) = {1}")
            start_node = Node.cast(start_node)
            if start_node.graph != self:
                raise ValueError("Start node does not belong to this graph")
            if start_node.identity is None:
                raise ValueError("Start node is not bound to a graph")
            parameters = {"1": start_node.identity}
            returns.append("b")
        elif start_node is None:
            clauses.append("MATCH (b) WHERE id(b) = {2}")
            end_node = Node.cast(end_node)
            if end_node.graph != self:
                raise ValueError("End node does not belong to this graph")
            if end_node.identity is None:
                raise ValueError("End node is not bound to a graph")
            parameters = {"2": end_node.identity}
            returns.append("a")
        else:
            clauses.append("MATCH (a) WHERE id(a) = {1} MATCH (b) WHERE id(b) = {2}")
            start_node = Node.cast(start_node)
            if start_node.graph != self:
                raise ValueError("Start node does not belong to this graph")
            if start_node.identity is None:
                raise ValueError("Start node is not bound to a graph")
            end_node = Node.cast(end_node)
            if end_node.graph != self:
                raise ValueError("End node does not belong to this graph")
            if end_node.identity is None:
                raise ValueError("End node is not bound to a graph")
            parameters = {"1": start_node.identity, "2": end_node.identity}
        if rel_type is None:
            relationship_detail = ""
        elif is_collection(rel_type):
            relationship_detail = ":" + "|:".join(cypher_escape(t) for t in rel_type)
        else:
            relationship_detail = ":%s" % cypher_escape(rel_type)
        if bidirectional:
            clauses.append("MATCH (a)-[_" + relationship_detail + "]-(b)")
        else:
            clauses.append("MATCH (a)-[_" + relationship_detail + "]->(b)")
        returns.append("_")
        clauses.append("RETURN %s" % ", ".join(returns))
        if limit is not None:
            clauses.append("LIMIT %d" % limit)
        cursor = self.run(" ".join(clauses), parameters)
        while cursor.forward():
            record = cursor.current()
            yield record["_"]

    def match_one(self, start_node=None, rel_type=None, end_node=None, bidirectional=False):
        """ Match and return one relationship with specific criteria.

        :param start_node: start node of relationships to match (:const:`None` means any node)
        :param rel_type: type of relationships to match (:const:`None` means any type)
        :param end_node: end node of relationships to match (:const:`None` means any node)
        :param bidirectional: :const:`True` if reversed relationships should also be included
        """
        rels = list(self.match(start_node, rel_type, end_node,
                               bidirectional, 1))
        if rels:
            return rels[0]
        else:
            return None

    def merge(self, subgraph, label=None, *property_keys):
        """ Run a :meth:`.Transaction.merge` operation within an
        `autocommit` :class:`.Transaction`.

        :param subgraph: a :class:`.Node`, :class:`.Relationship` or other
                       :class:`.Subgraph` object
        :param label: label on which to match any existing nodes
        :param property_keys: property keys on which to match any existing nodes
        """
        with self.begin() as tx:
            tx.merge(subgraph, label, *property_keys)

    @property
    def name(self):
        return self.__name__

    @property
    def nodes(self):
        """ Obtain a :class:`.NodeMatcher` for this graph.

            >>> graph = Graph()
            >>> graph.nodes.get(1234)
            (_1234:Person {name: 'Alice'})
            >>> graph.nodes.match("Person", name="Alice").first()
            (_1234:Person {name: 'Alice'})

        """
        return NodeMatcher(self)

    def order(self):
        """ Count and return the number of nodes in this graph.
        """
        return self.evaluate("MATCH (_) RETURN count(_)")

    def pull(self, subgraph):
        """ Pull data to one or more entities from their remote counterparts.

        :param subgraph: the collection of nodes and relationships to pull
        """
        with self.begin() as tx:
            tx.pull(subgraph)

    def push(self, subgraph):
        """ Push data from one or more entities to their remote counterparts.

        :param subgraph: the collection of nodes and relationships to push
        """
        with self.begin() as tx:
            tx.push(subgraph)

    def relationship(self, identity):
        """ Fetch a relationship by ID.

        :param identity:
        """
        try:
            return self.relationship_cache[identity]
        except KeyError:
            relationship = self.evaluate("MATCH ()-[r]->() WHERE id(r)={x} RETURN r", x=identity)
            if relationship is None:
                raise IndexError("Relationship %d not found" % identity)
            else:
                return relationship

    def run(self, cypher, parameters=None, **kwparameters):
        """ Run a :meth:`.Transaction.run` operation within an
        `autocommit` :class:`.Transaction`.

        :param cypher: Cypher statement
        :param parameters: dictionary of parameters
        :param kwparameters: extra keyword parameters
        :return:
        """
        return self.begin(autocommit=True).run(cypher, parameters, **kwparameters)

    def separate(self, subgraph):
        """ Run a :meth:`.Transaction.separate` operation within an
        `autocommit` :class:`.Transaction`.

        :param subgraph: a :class:`.Node`, :class:`.Relationship` or other
                       :class:`.Subgraph`
        """
        self.begin(autocommit=True).separate(subgraph)

    def size(self):
        """ Count and return the number of relationships in this graph.
        """
        return self.evaluate("MATCH ()-[_]->() RETURN count(_)")


class Schema(object):
    """ The schema resource attached to a `Graph` instance.
    """

    def __init__(self, graph):
        self.graph = graph

    @property
    def node_labels(self):
        """ The set of node labels currently defined within the graph.
        """
        return frozenset(record[0] for record in self.graph.run("CALL db.labels"))

    @property
    def relationship_types(self):
        """ The set of relationship types currently defined within the graph.
        """
        return frozenset(record[0] for record in self.graph.run("CALL db.relationshipTypes"))

    def create_index(self, label, *property_keys):
        """ Create a schema index for a label and property
        key combination.
        """
        self.graph.run("CREATE INDEX ON :%s(%s)" %
                       (cypher_escape(label), ",".join(map(cypher_escape, property_keys)))).close()
        while property_keys not in self.get_indexes(label):
            sleep(0.1)

    def create_uniqueness_constraint(self, label, *property_keys):
        """ Create a uniqueness constraint for a label.
        """
        self.graph.run("CREATE CONSTRAINT ON (a:%s) "
                       "ASSERT a.%s IS UNIQUE" %
                       (cypher_escape(label), ",".join(map(cypher_escape, property_keys)))).close()
        while property_keys not in self.get_uniqueness_constraints(label):
            sleep(0.1)

    def drop_index(self, label, *property_keys):
        """ Remove label index for a given property key.
        """
        self.graph.run("DROP INDEX ON :%s(%s)" %
                       (cypher_escape(label), ",".join(map(cypher_escape, property_keys)))).close()

    def drop_uniqueness_constraint(self, label, *property_keys):
        """ Remove the uniqueness constraint for a given property key.
        """
        self.graph.run("DROP CONSTRAINT ON (a:%s) "
                       "ASSERT a.%s IS UNIQUE" %
                       (cypher_escape(label), ",".join(map(cypher_escape, property_keys)))).close()

    def _get_indexes(self, label, t=None):
        indexes = []
        for record in self.graph.run("CALL db.indexes"):
            lbl = None
            properties = []
            if len(record) == 6:
                description, lbl, properties, state, typ, provider = record
            elif len(record) == 3:
                description, state, typ = record
            else:
                raise RuntimeError("Unexpected response from procedure db.indexes (%d fields)" % len(record))
            if state not in (u"ONLINE", u"online"):
                continue
            if t and typ != t:
                continue
            if not lbl or not properties:
                from py2neo.cypher.reading import CypherLexer
                from pygments.token import Token
                tokens = list(CypherLexer().get_tokens(description))
                for token_type, token_value in tokens:
                    if token_type is Token.Name.Label:
                        lbl = token_value.strip("`")
                    elif token_type is Token.Name.Variable:
                        properties.append(token_value.strip("`"))
            if not lbl or not properties:
                continue
            if lbl == label:
                indexes.append(tuple(properties))
        return indexes

    def get_indexes(self, label):
        """ Fetch a list of indexed property keys for a label.
        """
        return self._get_indexes(label)

    def get_uniqueness_constraints(self, label):
        """ Fetch a list of unique constraints for a label.
        """
        return self._get_indexes(label, "node_unique_property")


class Result(object):
    """ Wraps a BoltStatementResult
    """

    def __init__(self, graph, entities, result):
        from neo4j.v1 import BoltStatementResult
        from py2neo.internal.http import HTTPStatementResult
        from py2neo.internal.packstream import PackStreamHydrator
        self.result = result
        self.result.error_class = GraphError.hydrate
        # TODO: un-yuk this
        if isinstance(result, HTTPStatementResult):
            self.result._hydrant.entities = entities
            self.result_iterator = iter(self.result)
        elif isinstance(result, BoltStatementResult):
            self.result._hydrant = PackStreamHydrator(graph, result.keys(), entities)
            self.result_iterator = iter(map(Record, self.result))
        else:
            raise RuntimeError("Unexpected statement result class %r" % result.__class__.__name__)

    def keys(self):
        """ Return the keys for the whole data set.
        """
        return self.result.keys()

    def plan(self):
        """ Return the query plan, if available.
        """
        metadata = self.result.summary().metadata
        plan = {}
        if "plan" in metadata:
            plan.update(metadata["plan"])
        if "profile" in metadata:
            plan.update(metadata["profile"])
        if "http_plan" in metadata:
            plan.update(metadata["http_plan"]["root"])

        def collapse_args(data):
            if "args" in data:
                for key in data["args"]:
                    data[key] = data["args"][key]
                del data["args"]
            if "children" in data:
                for child in data["children"]:
                    collapse_args(child)

        def snake_keys(data):
            if isinstance(data, list):
                for item in data:
                    snake_keys(item)
                return
            if not isinstance(data, dict):
                return
            for key, value in list(data.items()):
                new_key = snake_case(key)
                if new_key != key:
                    data[new_key] = value
                    del data[key]
                if isinstance(value, (list, dict)):
                    snake_keys(value)

        collapse_args(plan)
        snake_keys(plan)
        return plan

    def stats(self):
        """ Return the query statistics.
        """
        return vars(self.result.summary().counters)

    def fetch(self):
        """ Fetch and return the next item.
        """
        try:
            return next(self.result_iterator)
        except StopIteration:
            return None


class GraphError(Exception):
    """
    """

    __cause__ = None

    http_status_code = None
    code = None
    message = None

    @classmethod
    def hydrate(cls, data):
        code = data["code"]
        message = data["message"]
        _, classification, category, title = code.split(".")
        if classification == "ClientError":
            try:
                error_cls = ClientError.get_mapped_class(code)
            except KeyError:
                error_cls = ClientError
                message = "%s: %s" % (title_case(title), message)
        elif classification == "DatabaseError":
            error_cls = DatabaseError
        elif classification == "TransientError":
            error_cls = TransientError
        else:
            error_cls = cls
        inst = error_cls(message)
        inst.code = code
        inst.message = message
        return inst

    def __new__(cls, *args, **kwargs):
        try:
            exception = kwargs["exception"]
            error_cls = type(xstr(exception), (cls,), {})
        except KeyError:
            error_cls = cls
        return Exception.__new__(error_cls, *args)

    def __init__(self, *args, **kwargs):
        Exception.__init__(self, *args)
        for key, value in kwargs.items():
            setattr(self, key.lower(), value)


class ClientError(GraphError):
    """ The Client sent a bad request - changing the request might yield a successful outcome.
    """

    @classmethod
    def get_mapped_class(cls, status):
        from neo4j.exceptions import ConstraintError, CypherSyntaxError, CypherTypeError, Forbidden, AuthError
        return {

            # ConstraintError
            "Neo.ClientError.Schema.ConstraintValidationFailed": ConstraintError,
            "Neo.ClientError.Schema.ConstraintViolation": ConstraintError,
            "Neo.ClientError.Statement.ConstraintVerificationFailed": ConstraintError,
            "Neo.ClientError.Statement.ConstraintViolation": ConstraintError,

            # CypherSyntaxError
            "Neo.ClientError.Statement.InvalidSyntax": CypherSyntaxError,
            "Neo.ClientError.Statement.SyntaxError": CypherSyntaxError,

            # CypherTypeError
            "Neo.ClientError.Procedure.TypeError": CypherTypeError,
            "Neo.ClientError.Statement.InvalidType": CypherTypeError,
            "Neo.ClientError.Statement.TypeError": CypherTypeError,

            # Forbidden
            "Neo.ClientError.General.ForbiddenOnReadOnlyDatabase": Forbidden,
            "Neo.ClientError.General.ReadOnly": Forbidden,
            "Neo.ClientError.Schema.ForbiddenOnConstraintIndex": Forbidden,
            "Neo.ClientError.Schema.IndexBelongsToConstrain": Forbidden,
            "Neo.ClientError.Security.Forbidden": Forbidden,
            "Neo.ClientError.Transaction.ForbiddenDueToTransactionType": Forbidden,

            # Unauthorized
            "Neo.ClientError.Security.AuthorizationFailed": AuthError,
            "Neo.ClientError.Security.Unauthorized": AuthError,

        }[status]


class DatabaseError(GraphError):
    """ The database failed to service the request.
    """


class TransientError(GraphError):
    """ The database cannot service the request right now, retrying later might yield a successful outcome.
    """


class TransactionFinished(GraphError):
    """ Raised when actions are attempted against a :class:`.Transaction`
    that is no longer available for use.
    """


class Transaction(object):
    """ A transaction is a logical container for multiple Cypher statements.
    """

    session = None

    _finished = False

    def __init__(self, graph, autocommit=False):
        self.graph = graph
        self.autocommit = autocommit
        self.entities = deque()
        self.driver = driver = self.graph.database.driver
        self.session = driver.session()
        self.results = []
        if autocommit:
            self.transaction = None
        else:
            self.transaction = self.session.begin_transaction()

    def __del__(self):
        if self.session:
            self.session.close()

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
        from neo4j.v1 import CypherError

        self._assert_unfinished()
        try:
            entities = self.entities.popleft()
        except IndexError:
            entities = {}

        try:
            if self.transaction:
                result = self.transaction.run(cypher, parameters, **kwparameters)
            else:
                result = self.session.run(cypher, parameters, **kwparameters)
        except CypherError as error:
            raise GraphError.hydrate({"code": error.code, "message": error.message})
        else:
            r = Result(self.graph, entities, result)
            self.results.append(r)
            return Cursor(r)
        finally:
            if not self.transaction:
                self.finish()

    def process(self):
        """ Send all pending statements to the server for processing.
        """
        self._assert_unfinished()
        self.session.sync()

    def finish(self):
        self.process()
        if self.transaction:
            self.transaction.close()
        self._assert_unfinished()
        self._finished = True
        self.session.close()
        self.session = None

    def commit(self):
        """ Commit the transaction.
        """
        if self.transaction:
            self.transaction.success = True
        self.finish()

    def rollback(self):
        """ Roll back the current transaction, undoing all actions previously taken.
        """
        self._assert_unfinished()
        if self.transaction:
            self.transaction.success = False
        self.finish()

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

    def degree(self, subgraph):
        """ Return the total number of relationships attached to all nodes in
        a subgraph.

        :param subgraph: a :class:`.Node`, :class:`.Relationship` or other
                       :class:`.Subgraph`
        :returns: the total number of distinct relationships
        """
        try:
            degree = subgraph.__db_degree__
        except AttributeError:
            raise TypeError("No method defined to determine the degree of object %r" % subgraph)
        else:
            return degree(self)

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
        """ Determine whether one or more graph entities all exist within the
        database. Note that if any nodes or relationships in *subgraph* are not
        bound to remote counterparts, this method will return ``False``.

        :param subgraph: a :class:`.Node`, :class:`.Relationship` or other
                       :class:`.Subgraph`
        :returns: ``True`` if all entities exist remotely, ``False`` otherwise
        """
        try:
            exists = subgraph.__db_exists__
        except AttributeError:
            raise TypeError("No method defined to determine the existence of object %r" % subgraph)
        else:
            return exists(self)

    def merge(self, subgraph, primary_label=None, primary_key=None):
        """ Merge nodes and relationships from a local subgraph into the
        database. Each node and relationship is merged independently, with
        nodes merged first and relationships merged second.

        For each node, the merge is carried out by comparing that node with a
        potential remote equivalent on the basis of a label and property value.
        If no remote match is found, a new node is created. The label and
        property to use for comparison are determined by `primary_label` and
        `primary_key` but may be overridden for individual nodes by the
        presence of `__primarylabel__` and `__primarykey__` attributes on
        the node itself. Note that multiple property keys may be specified by
        using a tuple.

        For each relationship, the merge is carried out by comparing that
        relationship with a potential remote equivalent on the basis of matching
        start and end nodes plus relationship type. If no remote match is found,
        a new relationship is created.

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
        """ TODO

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
        """ TODO

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
            print(cursor.current()["name"])

    If only the first record is of interest, a similar `if` structure will
    do the job::

        if cursor.forward():
            print(cursor.current()["name"])

    To combine `forward` and `current` into a single step, use :attr:`.next`::

        print(cursor.next()["name"])

    Cursors are also iterable, so can be used in a loop::

        for record in cursor:
            print(record["name"])

    For queries that are expected to return only a single value within a
    single record, use the :meth:`.evaluate` method. This will return the
    first value from the next record or :py:const:`None` if neither the
    field nor the record are present::

        print(cursor.evaluate())

    """

    def __init__(self, result):
        self._result = result
        self._current = None

    def __next__(self):
        if self.forward():
            return self._current
        else:
            raise StopIteration()

    def __iter__(self):
        while self.forward():
            yield self._current

    def current(self):
        """ Returns the current record or :py:const:`None` if no record
        has yet been selected.
        """
        return self._current

    def next(self):
        """ Returns the next record in the stream, or raises
        :py:class:`StopIteration` if no more records are available.

            cursor.current if cursor.forward() else None

        """
        return self.__next__()

    def close(self):
        """ Close this cursor and free up all associated resources.
        """
        self._result = None
        self._current = None

    def keys(self):
        """ Return the field names for the records in the stream.
        """
        return self._result.keys()

    def plan(self):
        """ Return the plan returned with this result, if any.
        """
        return self._result.plan()

    def stats(self):
        """ Return the query statistics.
        """
        s = dict.fromkeys(update_stats_keys, 0)
        s.update(self._result.stats())
        s["contains_updates"] = bool(sum(s.get(k, 0) for k in update_stats_keys))
        return s

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
        assert amount > 0
        amount = int(amount)
        moved = 0
        fetch = self._result.fetch
        while moved != amount:
            new_current = fetch()
            if new_current is None:
                break
            else:
                self._current = new_current
                moved += 1
        return moved

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
            >>> g.run("MATCH (a) WHERE a.email={x} RETURN a.name", x="bob@acme.com").evaluate()
            'Bob Robertson'
        """
        if self.forward():
            try:
                return self.current()[field]
            except IndexError:
                return None
        else:
            return None

    def data(self):
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

        :return: the full query result
        :rtype: `list` of `dict`
        """
        return [record.data() for record in self]

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
           This method requires `numpy` to be installed, which can be done directly or via the `sci` extra.

        :param dtype:
        :param order:
        :warns: If `numpy` is not installed
        :returns: `ndarray <https://docs.scipy.org/doc/numpy/reference/generated/numpy.ndarray.html>`__ object.
        """
        try:
            from numpy import array
        except ImportError:
            warn("Numpy is not installed. This can be installed directly or via the [sci] extra.")
            raise
        else:
            return array(list(map(list, self)), dtype=dtype, order=order)

    def to_series(self, field=0, index=None, dtype=None):
        """ Consume and extract one field of the entire result as a
        `pandas.Series <http://pandas.pydata.org/pandas-docs/stable/dsintro.html#series>`_.

        .. note::
           This method requires `pandas` to be installed, which can be done directly or via the `sci` extra.

        :param field:
        :param index:
        :param dtype:
        :warns: If `pandas` is not installed
        :returns: `Series <http://pandas.pydata.org/pandas-docs/stable/dsintro.html#series>`__ object.
        """
        try:
            from pandas import Series
        except ImportError:
            warn("Pandas is not installed. This can be installed directly or via the [sci] extra.")
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
           This method requires `pandas` to be installed, which can be done directly or via the `sci` extra.

        :param index: Index to use for resulting frame.
        :param columns: Column labels to use for resulting frame.
        :param dtype: Data type to force.
        :warns: If `pandas` is not installed
        :returns: `DataFrame <http://pandas.pydata.org/pandas-docs/stable/dsintro.html#series>`__ object.
        """
        try:
            from pandas import DataFrame
        except ImportError:
            warn("Pandas is not installed. This can be installed directly or via the [sci] extra.")
            raise
        else:
            return DataFrame(list(map(dict, self)), index=index, columns=columns, dtype=dtype)

    def to_matrix(self, mutable=False):
        """ Consume and extract the entire result as a
        `sympy.Matrix <http://docs.sympy.org/latest/tutorial/matrices.html>`_.

        .. note::
           This method requires `sympy` to be installed, which can be done directly or via the `sci` extra.

        :param mutable:
        :returns: `Matrix <http://docs.sympy.org/latest/tutorial/matrices.html>`_ object.
        """
        try:
            from sympy import MutableMatrix, ImmutableMatrix
        except ImportError:
            warn("Sympy is not installed. This can be installed directly or via the [sci] extra.")
            raise
        else:
            if mutable:
                return MutableMatrix(list(map(list, self)))
            else:
                return ImmutableMatrix(list(map(list, self)))
