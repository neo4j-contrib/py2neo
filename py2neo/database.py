#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2019, Nigel Small
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

from collections import deque, OrderedDict
from datetime import datetime
from time import sleep
from warnings import warn

from py2neo.cypher import cypher_escape
from py2neo.data import Table
from py2neo.internal.caching import ThreadLocalEntityCache
from py2neo.internal.text import Words
from py2neo.internal.compat import Mapping, string_types, xstr
from py2neo.internal.versioning import Version
from py2neo.matching import NodeMatcher, RelationshipMatcher


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

    _cx_pool = None
    # _driver = None
    _connector = None
    _graphs = None

    @classmethod
    def forget_all(cls):
        """ Forget all cached :class:`.Database` details.
        """
        for _, db in cls._instances.items():
            # db._driver.close()
            # db._driver = None
            db._connector.close()
            db._connector = None
        cls._instances.clear()

    def __new__(cls, uri=None, **settings):
        from py2neo.internal.connectors import get_connection_data
        connection_data = get_connection_data(uri, **settings)
        key = connection_data["hash"]
        try:
            inst = cls._instances[key]
        except KeyError:
            inst = super(Database, cls).__new__(cls)
            inst._connection_data = connection_data
            from py2neo.internal.connectors import Connector
            inst._connector = Connector(connection_data["uri"],
                                        auth=connection_data["auth"],
                                        secure=connection_data["secure"],
                                        user_agent=connection_data["user_agent"])
            inst._graphs = {}
            cls._instances[key] = inst
        return inst

    def __repr__(self):
        class_name = self.__class__.__name__
        data = self._connection_data
        return "<%s uri=%r secure=%r user_agent=%r>" % (
            class_name, data["uri"], data["secure"], data["user_agent"])

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
            self._graphs[database] = Graph(**self._connection_data)
        return self._graphs[database]

    def __setitem__(self, database, graph):
        self._graphs[database] = graph

    def __iter__(self):
        yield "data"

    @property
    def connector(self):
        return self._connector

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
    def name(self):
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
        return Version.parse(version_string).major_minor_patch

    @property
    def product(self):
        """ Return the product name.
        """
        info = self.query_jmx("org.neo4j", name="Kernel")
        return info["KernelVersion"]

    @property
    def store_creation_time(self):
        """ Return the time when this Neo4j graph store was created.
        """
        info = self.query_jmx("org.neo4j", name="Kernel")
        return datetime.fromtimestamp(info["StoreCreationDate"] / 1000.0)

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


class Graph(object):
    """ The `Graph` class represents the graph data storage space within
    a Neo4j graph database. Connection details are provided using URIs
    and/or individual settings.

    Supported URI schemes are:

    - ``http``
    - ``https``
    - ``bolt``

    The full set of supported `settings` are:

    ==============  =============================================  ==============  =============
    Keyword         Description                                    Type            Default
    ==============  =============================================  ==============  =============
    ``auth``        A 2-tuple of (user, password)                  tuple           ``('neo4j', 'password')``
    ``host``        Database server host name                      str             ``'localhost'``
    ``password``    Password to use for authentication             str             ``'password'``
    ``port``        Database server port                           int             ``7687``
    ``scheme``      Use a specific URI scheme                      str             ``'bolt'``
    ``secure``      Use a secure connection (TLS)                  bool            ``False``
    ``user``        User to authenticate as                        str             ``'neo4j'``
    ``user_agent``  User agent to send for all connections         str             `(depends on URI scheme)`
    ==============  =============================================  ==============  =============

    Each setting can be provided as a keyword argument or as part of
    an ``http:``, ``https:`` or ``bolt:`` URI. Therefore, the examples
    below are equivalent::

        >>> from py2neo import Graph
        >>> graph_1 = Graph()
        >>> graph_2 = Graph(host="localhost")
        >>> graph_3 = Graph("bolt://localhost:7687")

    Once obtained, the `Graph` instance provides direct or indirect
    access to most of the functionality available within py2neo.

    Note that py2neo does not support routing with a Neo4j causal cluster
    (bolt+routing). For this functionality, please use the official Neo4j
    Driver for Python.
    """

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
        return "<Graph database=%r name=%r>" % (self.database, self.__name__)

    def __eq__(self, other):
        try:
            return self.database == other.database and self.__name__ == other.__name__
        except (AttributeError, TypeError):
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __len__(self):
        return len(self.relationships)

    def __bool__(self):
        return True

    __nonzero__ = __bool__

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

    def match(self, nodes=None, r_type=None, limit=None):
        """ Match and return all relationships with specific criteria.

        For example, to find all of Alice's friends::

            for rel in graph.match((alice, ), r_type="FRIEND"):
                print(rel.end_node["name"])

        :param nodes: Sequence or Set of start and end nodes (:const:`None` means any node);
                a Set implies a match in any direction
        :param r_type: type of relationships to match (:const:`None` means any type)
        :param limit: maximum number of relationships to match (:const:`None` means unlimited)
        """
        return RelationshipMatcher(self).match(nodes=nodes, r_type=r_type).limit(limit)

    def match_one(self, nodes=None, r_type=None):
        """ Match and return one relationship with specific criteria.

        :param nodes: Sequence or Set of start and end nodes (:const:`None` means any node);
                a Set implies a match in any direction
        :param r_type: type of relationships to match (:const:`None` means any type)
        """
        matches = self.match(nodes=nodes, r_type=r_type, limit=1)
        rels = list(matches)
        if rels:
            return rels[0]
        else:
            return None

    def merge(self, subgraph, label=None, *property_keys):
        """ Run a :meth:`.Transaction.merge` operation within an
        `autocommit` :class:`.Transaction`.

        The example code below shows a simple merge for a new relationship
        between two new nodes:

            >>> from py2neo import Graph, Node, Relationship
            >>> g = Graph()
            >>> a = Node("Person", name="Alice", age=33)
            >>> b = Node("Person", name="Bob", age=44)
            >>> KNOWS = Relationship.type("KNOWS")
            >>> g.merge(KNOWS(a, b), "Person", "name")

        Following on, we then create a third node (of a different type) to
        which both the original nodes connect:

            >>> c = Node("Company", name="ACME")
            >>> c.__primarylabel__ = "Company"
            >>> c.__primarykey__ = "name"
            >>> WORKS_FOR = Relationship.type("WORKS_FOR")
            >>> g.merge(WORKS_FOR(a, c) | WORKS_FOR(b, c))

        For details of how the merge algorithm works, see the
        :meth:`.Transaction.merge` method. Note that this is different
        to a Cypher MERGE.

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

        This can be used to find nodes that match given criteria:

            >>> graph = Graph()
            >>> graph.nodes[1234]
            (_1234:Person {name: 'Alice'})
            >>> graph.nodes.get(1234)
            (_1234:Person {name: 'Alice'})
            >>> graph.nodes.match("Person", name="Alice").first()
            (_1234:Person {name: 'Alice'})

        Nodes can also be efficiently counted using this attribute:

            >>> len(graph.nodes)
            55691
            >>> len(graph.nodes.match("Person", age=33))
            12

        """
        return NodeMatcher(self)

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

    @property
    def relationships(self):
        """ Obtain a :class:`.RelationshipMatcher` for this graph.

        This can be used to find relationships that match given criteria
        as well as efficiently count relationships.
        """
        return RelationshipMatcher(self)

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


class Schema(object):
    """ The schema resource attached to a `Graph` instance.
    """

    def __init__(self, graph):
        self.graph = graph

    @property
    def node_labels(self):
        """ The set of node labels currently defined within the graph.
        """
        return frozenset(record[0] for record in
                         self.graph.run("CALL db.labels"))

    @property
    def relationship_types(self):
        """ The set of relationship types currently defined within the graph.
        """
        return frozenset(record[0] for record in
                         self.graph.run("CALL db.relationshipTypes"))

    def create_index(self, label, *property_keys):
        """ Create a schema index for a label and property
        key combination.
        """
        cypher = "CREATE INDEX ON :{}({})".format(
            cypher_escape(label), ", ".join(map(cypher_escape, property_keys)))
        self.graph.run(cypher).close()
        while property_keys not in self.get_indexes(label):
            sleep(0.1)

    def create_uniqueness_constraint(self, label, property_key):
        """ Create a node uniqueness constraint for a given label and property
        key.

        While indexes support the use of composite keys, unique constraints may
        only be tied to a single property key.
        """
        cypher = "CREATE CONSTRAINT ON (_:{}) ASSERT _.{} IS UNIQUE".format(
            cypher_escape(label), cypher_escape(property_key))
        self.graph.run(cypher).close()
        while property_key not in self.get_uniqueness_constraints(label):
            sleep(0.1)

    def drop_index(self, label, *property_keys):
        """ Remove label index for a given property key.
        """
        cypher = "DROP INDEX ON :{}({})".format(
            cypher_escape(label), ", ".join(map(cypher_escape, property_keys)))
        self.graph.run(cypher).close()

    def drop_uniqueness_constraint(self, label, property_key):
        """ Remove the node uniqueness constraint for a given label and
        property key.
        """
        cypher = "DROP CONSTRAINT ON (_:{}) ASSERT _.{} IS UNIQUE".format(
            cypher_escape(label), cypher_escape(property_key))
        self.graph.run(cypher).close()

    def _get_indexes(self, label, t=None):
        indexes = []
        for record in self.graph.run("CALL db.indexes"):
            properties = []
            # The code branches here depending on the format of the response
            # from the `db.indexes` procedure, which has varied enormously
            # over the entire 3.x series.
            if len(record) == 10:
                # 3.5.0
                (description, index_name, token_names, properties, state,
                 type_, progress, provider, id_, failure_message) = record
            elif len(record) == 7:
                # 3.4.10
                (description, lbl, properties, state,
                 type_, provider, failure_message) = record
                token_names = [lbl]
            elif len(record) == 6:
                # 3.4.7
                description, lbl, properties, state, type_, provider = record
                token_names = [lbl]
            elif len(record) == 3:
                # 3.0.10
                description, state, type_ = record
                token_names = []
            else:
                raise RuntimeError("Unexpected response from procedure "
                                   "db.indexes (%d fields)" % len(record))
            if state not in (u"ONLINE", u"online"):
                continue
            if t and type_ != t:
                continue
            if not token_names or not properties:
                from py2neo.cypher.lexer import CypherLexer
                from pygments.token import Token
                tokens = list(CypherLexer().get_tokens(description))
                for token_type, token_value in tokens:
                    if token_type is Token.Name.Label:
                        token_names.append(token_value.strip("`"))
                    elif token_type is Token.Name.Variable:
                        properties.append(token_value.strip("`"))
            if not token_names or not properties:
                continue
            if label in token_names:
                indexes.append(tuple(properties))
        return indexes

    def get_indexes(self, label):
        """ Fetch a list of indexed property keys for a label.
        """
        return self._get_indexes(label)

    def get_uniqueness_constraints(self, label):
        """ Fetch a list of unique constraints for a label. Each constraint is
        the name of a single property key.
        """
        return [k[0] for k in self._get_indexes(label, "node_unique_property")]


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
                message = "%s: %s" % (Words(title).camel(upper_first=True), message)
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
        raise KeyError(status)
        from neobolt.exceptions import ConstraintError, CypherSyntaxError, CypherTypeError, Forbidden, AuthError
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


class TransactionError(GraphError):
    """ Raised when actions are attempted against a :class:`.Transaction`
    that is no longer available for use, or a transaction is otherwise invalid.
    """


class Transaction(object):
    """ A transaction is a logical container for multiple Cypher statements.
    """

    # session = None

    _finished = False

    def __init__(self, graph, autocommit=False):
        self.graph = graph
        self.autocommit = autocommit
        self.entities = deque()
        self.connector = self.graph.database.connector
        self.results = []
        if autocommit:
            self.transaction = None
        else:
            self.transaction = self.connector.begin()

    # def __del__(self):
    #     if self.session:
    #         self.session.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self.commit()
        else:
            self._rollback()

    def _assert_unfinished(self):
        if self._finished:
            raise TransactionError(self)

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
        from neobolt.exceptions import CypherError

        self._assert_unfinished()
        try:
            entities = self.entities.popleft()
        except IndexError:
            entities = {}

        try:
            return Cursor(self.connector.run(statement=cypher,
                                             parameters=dict(parameters or {}, **kwparameters),
                                             tx=self.transaction,
                                             graph=self.graph,
                                             keys=[],
                                             entities=entities))
        except CypherError as error:
            raise GraphError.hydrate({"code": error.code, "message": error.message})
        finally:
            if not self.transaction:
                self.finish()

    def process(self):
        """ Send all pending statements to the server for processing.
        """
        self._assert_unfinished()
        if self.transaction:
            self.connector.sync(self.transaction)

    def finish(self):
        self.process()
        self._assert_unfinished()
        self._finished = True

    def commit(self):
        """ Commit the transaction.
        """
        self._assert_unfinished()
        self.connector.commit(self.transaction)
        self._finished = True

    def _rollback(self):
        """ Implicit rollback.
        """
        if self.connector.is_valid_transaction(self.transaction):
            self.connector.rollback(self.transaction)
        self._finished = True

    def rollback(self):
        """ Roll back the current transaction, undoing all actions previously taken.
        """
        self._assert_unfinished()
        self.connector.rollback(self.transaction)
        self._finished = True

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
            raise TypeError("No method defined to check existence of object %r" % subgraph)
        else:
            return exists(self)

    def merge(self, subgraph, primary_label=None, primary_key=None):
        """ Create or update the nodes and relationships of a local
        subgraph in the remote database. Note that the functionality of
        this operation is not strictly identical to the Cypher MERGE
        clause, although there is some overlap.

        Each node and relationship in the local subgraph is merged
        independently, with nodes merged first and relationships merged
        second.

        For each node, the merge is carried out by comparing that node with
        a potential remote equivalent on the basis of a single label and
        property value. If no remote match is found, a new node is created;
        if a match is found, the labels and properties of the remote node
        are updated. The label and property used for comparison are determined
        by the `primary_label` and `primary_key` arguments but may be
        overridden for individual nodes by the of `__primarylabel__` and
        `__primarykey__` attributes on the node itself.

        For each relationship, the merge is carried out by comparing that
        relationship with a potential remote equivalent on the basis of matching
        start and end nodes plus relationship type. If no remote match is found,
        a new relationship is created; if a match is found, the properties of
        the remote relationship are updated.

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
        """ Update local entities from their remote counterparts.

        For any nodes and relationships that exist in both the local
        :class:`.Subgraph` and the remote :class:`.Graph`, pull properties
        and node labels into the local copies. This operation does not
        create or delete any entities.

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
        """ Update remote entities from their local counterparts.

        For any nodes and relationships that exist in both the local
        :class:`.Subgraph` and the remote :class:`.Graph`, push properties
        and node labels into the remote copies. This operation does not
        create or delete any entities.

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
            print(cursor.current["name"])

    If only the first record is of interest, a similar `if` structure will
    do the job::

        if cursor.forward():
            print(cursor.current["name"])

    To combine `forward` and `current` into a single step, use the built-in
    py:func:`next` function::

        print(next(cursor)["name"])

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

    def __del__(self):
        try:
            self.close()
        except:
            pass

    def __next__(self):
        if self.forward():
            return self._current
        else:
            raise StopIteration()

    # Exists only for Python 2 iteration compatibility
    next = __next__

    def __iter__(self):
        while self.forward():
            yield self._current

    def __getitem__(self, key):
        return self._current[key]

    @property
    def current(self):
        """ Returns the current record or :py:const:`None` if no record
        has yet been selected.
        """
        return self._current

    def close(self):
        """ Close this cursor and free up all associated resources.
        """
        if self._result is not None:
            self._result.buffer()   # force consumption of remaining data
            self._result = None
        self._current = None

    def keys(self):
        """ Return the field names for the records in the stream.
        """
        return self._result.keys()

    def summary(self):
        """ Return the result summary.
        """
        return self._result.summary()

    def plan(self):
        """ Return the plan returned with this result, if any.
        """
        return self._result.plan()

    def stats(self):
        """ Return the query statistics.

        This contains details of the activity undertaken by the database
        kernel for the query, such as the number of entities created or
        deleted. Specifically, this returns a :class:`.CypherStats` object.

        >>> from py2neo import Graph
        >>> g = Graph()
        >>> g.run("CREATE (a:Person) SET a.name = 'Alice'").stats()
        constraints_added: 0
        constraints_removed: 0
        contains_updates: True
        indexes_added: 0
        indexes_removed: 0
        labels_added: 1
        labels_removed: 0
        nodes_created: 1
        nodes_deleted: 0
        properties_set: 1
        relationships_created: 0
        relationships_deleted: 0

        """
        return self._result.stats()

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
                return self[field]
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
           This method requires `numpy` to be installed.

        :param dtype:
        :param order:
        :warns: If `numpy` is not installed
        :returns: `ndarray <https://docs.scipy.org/doc/numpy/reference/generated/numpy.ndarray.html>`__ object.
        """
        try:
            from numpy import array
        except ImportError:
            warn("Numpy is not installed.")
            raise
        else:
            return array(list(map(list, self)), dtype=dtype, order=order)

    def to_series(self, field=0, index=None, dtype=None):
        """ Consume and extract one field of the entire result as a
        `pandas.Series <http://pandas.pydata.org/pandas-docs/stable/dsintro.html#series>`_.

        .. note::
           This method requires `pandas` to be installed.

        :param field:
        :param index:
        :param dtype:
        :warns: If `pandas` is not installed
        :returns: `Series <http://pandas.pydata.org/pandas-docs/stable/dsintro.html#series>`__ object.
        """
        try:
            from pandas import Series
        except ImportError:
            warn("Pandas is not installed.")
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
           This method requires `pandas` to be installed.

        :param index: Index to use for resulting frame.
        :param columns: Column labels to use for resulting frame.
        :param dtype: Data type to force.
        :warns: If `pandas` is not installed
        :returns: `DataFrame <http://pandas.pydata.org/pandas-docs/stable/dsintro.html#series>`__ object.
        """
        try:
            from pandas import DataFrame
        except ImportError:
            warn("Pandas is not installed.")
            raise
        else:
            return DataFrame(list(map(dict, self)), index=index, columns=columns, dtype=dtype)

    def to_matrix(self, mutable=False):
        """ Consume and extract the entire result as a
        `sympy.Matrix <http://docs.sympy.org/latest/tutorial/matrices.html>`_.

        .. note::
           This method requires `sympy` to be installed.

        :param mutable:
        :returns: `Matrix <http://docs.sympy.org/latest/tutorial/matrices.html>`_ object.
        """
        try:
            from sympy import MutableMatrix, ImmutableMatrix
        except ImportError:
            warn("Sympy is not installed.")
            raise
        else:
            if mutable:
                return MutableMatrix(list(map(list, self)))
            else:
                return ImmutableMatrix(list(map(list, self)))


class CypherSummary(object):

    def __init__(self, **data):
        self._data = data

    @property
    def connection(self):
        return self._data.get("connection")


class CypherStats(Mapping):
    """ Container for a set of statistics drawn from Cypher query execution.

    Each value can be accessed as either an attribute or via a string index.
    This class implements :py:class:`.Mapping` to allow it to be used as a
    dictionary.
    """

    #: Boolean flag to indicate whether or not the query contained an update.
    contained_updates = False
    #: Number of nodes created.
    nodes_created = 0
    #: Number of nodes deleted.
    nodes_deleted = 0
    #: Number of property values set.
    properties_set = 0
    #: Number of relationships created.
    relationships_created = 0
    #: Number of relationships deleted.
    relationships_deleted = 0
    #: Number of node labels added.
    labels_added = 0
    #: Number of node labels removed.
    labels_removed = 0
    #: Number of indexes added.
    indexes_added = 0
    #: Number of indexes removed.
    indexes_removed = 0
    #: Number of constraints added.
    constraints_added = 0
    #: Number of constraints removed.
    constraints_removed = 0

    def __init__(self, **stats):
        for key, value in stats.items():
            key = key.replace("-", "_")
            if key.startswith("relationship_"):
                # hack for server bug
                key = "relationships_" + key[13:]
            if hasattr(self.__class__, key):
                setattr(self, key, value)
            self.contained_updates = bool(sum(getattr(self, k, 0)
                                              for k in self.keys()))

    def __repr__(self):
        lines = []
        for key in sorted(self.keys()):
            lines.append("{}: {}".format(key, getattr(self, key)))
        return "\n".join(lines)

    def __getitem__(self, key):
        return getattr(self, key)

    def __len__(self):
        return len(self.keys())

    def __iter__(self):
        return iter(self.keys())

    def keys(self):
        """ Full list of the key or attribute names of the statistics
        available.
        """
        return [key for key in vars(self.__class__).keys()
                if not key.startswith("_") and key != "keys"]


class CypherPlan(Mapping):

    @classmethod
    def _clean_key(cls, key):
        return Words(key).snake()

    @classmethod
    def _clean_keys(cls, data):
        return OrderedDict(sorted((cls._clean_key(key), value) for key, value in dict(data).items()))

    def __init__(self, **kwargs):
        data = self._clean_keys(kwargs)
        if "root" in data:
            data = self._clean_keys(data["root"])
        self.operator_type = data.pop("operator_type", None)
        self.identifiers = data.pop("identifiers", [])
        self.children = [CypherPlan(**self._clean_keys(child)) for child in data.pop("children", [])]
        try:
            args = data.pop("args")
        except KeyError:
            self.args = data
        else:
            self.args = self._clean_keys(args)

    def __repr__(self):
        return ("%s(operator_type=%r, identifiers=%r, children=%r, args=%r)" %
                (self.__class__.__name__, self.operator_type, self.identifiers, self.children, self.args))

    def __getitem__(self, key):
        return getattr(self, key)

    def __len__(self):
        return len(self.keys())

    def __iter__(self):
        return iter(self.keys())

    def keys(self):
        return ["operator_type", "identifiers", "children", "args"]
