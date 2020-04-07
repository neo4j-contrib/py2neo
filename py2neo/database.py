#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2020, Nigel Small
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

from py2neo.connect import Connector, Connection, ConnectionProfile, TransactionError
from py2neo.cypher import cypher_escape
from py2neo.data import Record, Table
from py2neo.internal.caching import ThreadLocalEntityCache
from py2neo.internal.compat import Mapping, string_types, xstr
from py2neo.internal.operations import OperationError
from py2neo.internal.text import Words
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


class GraphService(object):
    """ Accessor for an entire Neo4j graph database installation over
    Bolt or HTTP. Within the py2neo object hierarchy, a
    :class:`.GraphService` contains a :class:`.Graph` in which most
    activity occurs.

    .. note ::
        In earlier versions, this class was known as `Database`.

    An explicit URI can be passed to the constructor::

        >>> from py2neo import GraphService
        >>> gs = GraphService("bolt://camelot.example.com:7687")

    Alternatively, the default value of ``bolt://localhost:7687`` is
    used::

        >>> default_gs = GraphService()
        >>> default_gs
        <GraphService uri='bolt://localhost:7687'>

    """

    _instances = {}

    _connector = None

    _graphs = None

    @classmethod
    def forget_all(cls):
        """ Forget all cached :class:`.GraphService` details.
        """
        for _, db in cls._instances.items():
            db._connector.close()
            db._connector = None
        cls._instances.clear()

    def __new__(cls, uri=None, **settings):
        profile = ConnectionProfile(uri, **settings)
        try:
            inst = cls._instances[profile]
        except KeyError:
            inst = super(GraphService, cls).__new__(cls)
            connector_settings = {
                "user_agent": settings.get("user_agent"),
                "init_size": settings.get("init_size"),
                "max_size": settings.get("max_size"),
                "max_age": settings.get("max_age"),
            }
            inst._connector = Connector.open(profile, **connector_settings)
            inst._graphs = {}
            cls._instances[profile] = inst
        return inst

    def __repr__(self):
        class_name = self.__class__.__name__
        profile = self._connector.profile
        return "<%s uri=%r secure=%r user_agent=%r>" % (
            class_name, profile.uri, profile.secure, self._connector.user_agent)

    def __eq__(self, other):
        try:
            return self.uri == other.uri
        except AttributeError:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash(self._connector)

    def __getitem__(self, graph_name):
        if graph_name is None:
            graph_name = self._connector.default_graph_name()
        elif graph_name not in self._connector.graph_names():
            raise KeyError("Graph {!r} does not exist for "
                           "service {!r}".format(graph_name, self._connector.profile.uri))
        graph_class = SystemGraph if graph_name == "system" else Graph
        if graph_name not in self._graphs:
            inst = object.__new__(graph_class)
            inst.service = self
            inst.__name__ = graph_name
            inst.schema = Schema(self)
            inst.node_cache = ThreadLocalEntityCache()
            inst.relationship_cache = ThreadLocalEntityCache()
            self._graphs[graph_name] = inst
        return self._graphs[graph_name]

    def __iter__(self):
        """ Yield all named graphs.

        For Neo4j 4.0 and above, this yields the names returned by a
        SHOW DATABASES query. For earlier versions, this yields no
        entries, since the one and only graph in these versions is not
        named.
        """
        return iter(self._connector.graph_names())

    @property
    def connector(self):
        return self._connector

    @property
    def uri(self):
        """ The URI to which this `GraphService` is connected.
        """
        return self._connector.profile.uri

    @property
    def default_graph(self):
        """ The default graph exposed by this graph service.

        :rtype: :class:`.Graph`
        """
        return self[None]

    @property
    def system_graph(self):
        """ The system graph exposed by this graph service.

        :rtype: :class:`.SystemGraph`
        """
        return self["system"]

    def keys(self):
        return list(self)

    @property
    def kernel_version(self):
        """ Return the version of Neo4j.
        """
        components = self.default_graph.call("dbms.components").data()
        kernel_component = [component for component in components
                            if component["name"] == "Neo4j Kernel"][0]
        version_string = kernel_component["versions"][0]
        return Version.parse(version_string).major_minor_patch

    @property
    def product(self):
        """ Return the product name.
        """
        record = next(self.default_graph.call("dbms.components"))
        return "%s %s (%s)" % (record[0], " ".join(record[1]), record[2].title())

    @property
    def config(self):
        """ Return a dictionary of the configuration parameters used to
        configure Neo4j.
        """
        return {record["name"]: record["value"]
                for record in self.default_graph.call("dbms.listConfig")}


class Graph(object):
    """ The `Graph` class represents the graph data storage space within
    a Neo4j graph database. Connection details are provided using URIs
    and/or individual settings.

    The `name` argument allows selection of a graph database by name.
    When working with Neo4j 4.0 and above, this can be any name defined
    in the system catalogue, a full list of which can be obtained
    through the Cypher ``SHOW DATABASES`` command. Passing `None` here
    will select the default database, as defined on the server. For
    earlier versions of Neo4j, the `name` must be set to `None`.

    The `system graph`, which is available in all 4.x+ product editions,
    can also be accessed via the :class:`.SystemGraph` class.

    Supported URI schemes are:

    - ``bolt`` - Bolt (unsecured)
    - ``bolt+s`` - Bolt (secured with full certificate checks)
    - ``bolt+ssc`` - Bolt (secured with no certificate checks)
    - ``http`` - HTTP (unsecured)
    - ``https`` - HTTP (secured with full certificate checks)
    - ``http+s`` - HTTP (secured with full certificate checks)
    - ``http+ssc`` - HTTP (secured with no certificate checks)

    The full set of supported `settings` are:

    ===================  ========================================================  ==============  =========================
    Keyword              Description                                               Type            Default
    ===================  ========================================================  ==============  =========================
    ``scheme``           Use a specific URI scheme                                 str             ``'bolt'``
    ``secure``           Use a secure connection (TLS)                             bool            ``False``
    ``verify``           Verify the server certificate (if secure)                 bool            ``True``
    ``host``             Database server host name                                 str             ``'localhost'``
    ``port``             Database server port                                      int             ``7687``
    ``address``          Colon-separated host and port string                      str             ``'localhost:7687'``
    ``user``             User to authenticate as                                   str             ``'neo4j'``
    ``password``         Password to use for authentication                        str             ``'password'``
    ``auth``             A 2-tuple of (user, password)                             tuple           ``('neo4j', 'password')``
    ``user_agent``       User agent to send for all connections                    str             `(depends on URI scheme)`
    ``max_connections``  The maximum number of simultaneous connections permitted  int             40
    ===================  ========================================================  ==============  =========================

    Each setting can be provided as a keyword argument or as part of
    an URI. Therefore, the three examples below are all equivalent::

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

    #: The :class:`.GraphService` to which this :class:`.Graph` belongs.
    service = None

    #: The :class:`.Schema` resource for this :class:`.Graph`.
    schema = None

    def __new__(cls, uri=None, name=None, **settings):
        gs = GraphService(uri, **settings)
        return gs[name]

    def __repr__(self):
        if self.name is None:
            return "%s(%r)" % (self.__class__.__name__, self.service.uri)
        else:
            return "%s(%r, name=%r)" % (self.__class__.__name__, self.service.uri, self.name)

    def __eq__(self, other):
        try:
            return self.service == other.service and self.__name__ == other.__name__
        except (AttributeError, TypeError):
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __len__(self):
        return len(self.relationships)

    def __bool__(self):
        return True

    __nonzero__ = __bool__

    def auto(self, readonly=False, after=None, metadata=None, timeout=None):
        """ Begin a new auto-commit :class:`.GraphTransaction`.

        :param readonly:
        :param after:
        :param metadata:
        :param timeout:
        """
        return GraphTransaction(self, True, readonly, after, metadata, timeout)

    def begin(self, autocommit=False, readonly=False,
              after=None, metadata=None, timeout=None):
        """ Begin a new :class:`.GraphTransaction`.

        :param autocommit: if :py:const:`True`, the transaction will
                         automatically commit after the first operation
        :param readonly:
        :param after:
        :param metadata:
        :param timeout:
        """
        if autocommit:
            warn("Graph.begin(autocommit=True) is deprecated, "
                 "use Graph.auto() instead", category=DeprecationWarning, stacklevel=2)
        return GraphTransaction(self, autocommit, readonly, after, metadata, timeout)

    def call(self, procedure, *args):
        """ Call a procedure by name.

        For example:
            >>> from py2neo import Graph
            >>> g = Graph()
            >>> g.call("dbms.components")
             name         | versions  | edition
            --------------|-----------|-----------
             Neo4j Kernel | ['4.0.2'] | community

        :param procedure: fully qualified procedure name
        :param args: positional arguments to pass to the procedure
        :returns: :class:`.Cursor` object wrapping the result
        """
        procedure_name = ".".join(cypher_escape(part) for part in procedure.split("."))
        arg_list = [(str(i), arg) for i, arg in enumerate(args)]
        cypher = "CALL %s(%s)" % (procedure_name, ", ".join("$" + a[0] for a in arg_list))
        return self.run(cypher, dict(arg_list))

    def create(self, subgraph):
        """ Run a :meth:`.GraphTransaction.create` operation within a
        :class:`.GraphTransaction`.

        :param subgraph: a :class:`.Node`, :class:`.Relationship` or other
                       :class:`.Subgraph`
        """
        with self.begin() as tx:
            tx.create(subgraph)

    def delete(self, subgraph):
        """ Run a :meth:`.GraphTransaction.delete` operation within an
        `autocommit` :class:`.GraphTransaction`. To delete only the
        relationships, use the :meth:`.separate` method.

        Note that only entities which are bound to corresponding
        remote entities though the ``graph`` and ``identity``
        attributes will trigger a deletion.

        :param subgraph: a :class:`.Node`, :class:`.Relationship` or other
                       :class:`.Subgraph` object
        """
        self.auto().delete(subgraph)

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
        """ Run a :meth:`.GraphTransaction.evaluate` operation within an
        `autocommit` :class:`.GraphTransaction`.

        :param cypher: Cypher statement
        :param parameters: dictionary of parameters
        :return: first value from the first record returned or
                 :py:const:`None`.
        """
        return self.auto().evaluate(cypher, parameters, **kwparameters)

    def exists(self, subgraph):
        """ Run a :meth:`.GraphTransaction.exists` operation within an
        `autocommit` :class:`.GraphTransaction`.

        :param subgraph: a :class:`.Node`, :class:`.Relationship` or other
                       :class:`.Subgraph` object
        :return:
        """
        return self.auto(readonly=True).exists(subgraph)

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
        """ Run a :meth:`.GraphTransaction.merge` operation within an
        `autocommit` :class:`.GraphTransaction`.

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
        :meth:`.GraphTransaction.merge` method. Note that this is different
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

    def play(self, work, args=None, kwargs=None, after=None, metadata=None, timeout=None):
        """ Call a function representing a transactional unit of work.

        The function must always accept a :class:`.GraphTransaction`
        object as its first argument. Additional arguments can be
        passed though the `args` and `kwargs` arguments of this method.

        If the function has a `readonly` attribute, and this is set to
        a truthy value, then it will be executed in a read-only
        environment, if possible.

        If the function has a `timeout` attribute, and no `timeout`
        argument is passed to this method call, then the value of the
        function attribute will be used instead for setting the
        timeout.

        :param work: function containing the unit of work
        :param args: sequence of additional positional arguments to
            pass into the function
        :param kwargs: mapping of additional keyword arguments to
            pass into the function
        :param after: :class:`.Bookmark` or tuple of :class:`.Bookmark`
            objects marking the point in transactional history after
            which this unit of work should be played
        :param metadata: user metadata to attach to this transaction
        :param timeout: timeout for transaction execution
        """
        if not callable(work):
            raise TypeError("Unit of work is not callable")
        kwargs = dict(kwargs or {})
        readonly = getattr(work, "readonly", False)
        if not timeout:
            timeout = getattr(work, "timeout", None)
        tx = self.begin(readonly=readonly, after=after, metadata=metadata, timeout=timeout)
        try:
            work(tx, *args or (), **kwargs or {})
        except Exception:  # TODO: catch transient and retry, if within limit
            tx.rollback()
            raise
        else:
            return tx.commit()

    def pull(self, subgraph):
        """ Pull data to one or more entities from their remote counterparts.

        :param subgraph: the collection of nodes and relationships to pull
        """
        with self.begin(readonly=True) as tx:
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
        """ Run a :meth:`.GraphTransaction.run` operation within an
        `autocommit` :class:`.GraphTransaction`.

        :param cypher: Cypher statement
        :param parameters: dictionary of parameters
        :param kwparameters: extra keyword parameters
        :return:
        """
        return self.auto().run(cypher, parameters, **kwparameters)

    def separate(self, subgraph):
        """ Run a :meth:`.GraphTransaction.separate` operation within an
        `autocommit` :class:`.GraphTransaction`.

        Note that only relationships which are bound to corresponding
        remote relationships though the ``graph`` and ``identity``
        attributes will trigger a deletion.

        :param subgraph: a :class:`.Node`, :class:`.Relationship` or other
                       :class:`.Subgraph`
        """
        self.auto().separate(subgraph)


class SystemGraph(Graph):
    """ A subclass of :class:`.Graph` that provides access to the
    system database for the remote DBMS. This is only available in
    Neo4j 4.0 and above.
    """

    def __new__(cls, uri=None, **settings):
        return super(SystemGraph, cls).__new__(uri, name="system", **settings)

    def __repr__(self):
        return "%s(%r)" % (self.__class__.__name__, self.service.uri)


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

    def _get_indexes(self, label, unique_only=False):
        indexes = []
        result = self.graph.run("CALL db.indexes")
        for record in result:
            properties = []
            # The code branches here depending on the format of the response
            # from the `db.indexes` procedure, which has varied enormously
            # since 3.0.
            if len(record) == 10:
                if "labelsOrTypes" in result.keys():
                    # 4.0.0
                    # ['id', 'name', 'state', 'populationPercent',
                    # 'uniqueness', 'type', 'entityType', 'labelsOrTypes',
                    #  'properties', 'provider']
                    (id_, name, state, population_percent, uniqueness, type_,
                     entity_type, token_names, properties, provider) = record
                    description = None
                    # The 'type' field has randomly changed its meaning in 4.0,
                    # holding for example 'BTREE' instead of for example
                    # 'node_unique_property'. To check for uniqueness, we now
                    # need to look at the new 'uniqueness' field.
                    is_unique = uniqueness == "UNIQUE"
                else:
                    # 3.5.3
                    # ['description', 'indexName', 'tokenNames', 'properties',
                    #  'state', 'type', 'progress', 'provider', 'id',
                    #  'failureMessage']
                    (description, index_name, token_names, properties, state,
                     type_, progress, provider, id_, failure_message) = record
                    is_unique = type_ == "node_unique_property"
            elif len(record) == 7:
                # 3.4.10
                (description, lbl, properties, state,
                 type_, provider, failure_message) = record
                is_unique = type_ == "node_unique_property"
                token_names = [lbl]
            elif len(record) == 6:
                # 3.4.7
                description, lbl, properties, state, type_, provider = record
                is_unique = type_ == "node_unique_property"
                token_names = [lbl]
            elif len(record) == 3:
                # 3.0.10
                description, state, type_ = record
                is_unique = type_ == "node_unique_property"
                token_names = []
            else:
                raise RuntimeError("Unexpected response from procedure "
                                   "db.indexes (%d fields)" % len(record))
            if state not in (u"ONLINE", u"online"):
                continue
            if unique_only and not is_unique:
                continue
            if not token_names or not properties:
                if description:
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
        return [k[0] for k in self._get_indexes(label, unique_only=True)]


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
        # TODO: mappings to error subclasses:
        #     {
        #         #
        #         # ConstraintError
        #         "Neo.ClientError.Schema.ConstraintValidationFailed": ConstraintError,
        #         "Neo.ClientError.Schema.ConstraintViolation": ConstraintError,
        #         "Neo.ClientError.Statement.ConstraintVerificationFailed": ConstraintError,
        #         "Neo.ClientError.Statement.ConstraintViolation": ConstraintError,
        #         #
        #         # CypherSyntaxError
        #         "Neo.ClientError.Statement.InvalidSyntax": CypherSyntaxError,
        #         "Neo.ClientError.Statement.SyntaxError": CypherSyntaxError,
        #         #
        #         # CypherTypeError
        #         "Neo.ClientError.Procedure.TypeError": CypherTypeError,
        #         "Neo.ClientError.Statement.InvalidType": CypherTypeError,
        #         "Neo.ClientError.Statement.TypeError": CypherTypeError,
        #         #
        #         # Forbidden
        #         "Neo.ClientError.General.ForbiddenOnReadOnlyDatabase": Forbidden,
        #         "Neo.ClientError.General.ReadOnly": Forbidden,
        #         "Neo.ClientError.Schema.ForbiddenOnConstraintIndex": Forbidden,
        #         "Neo.ClientError.Schema.IndexBelongsToConstrain": Forbidden,
        #         "Neo.ClientError.Security.Forbidden": Forbidden,
        #         "Neo.ClientError.Transaction.ForbiddenDueToTransactionType": Forbidden,
        #         #
        #         # Unauthorized
        #         "Neo.ClientError.Security.AuthorizationFailed": AuthError,
        #         "Neo.ClientError.Security.Unauthorized": AuthError,
        #         #
        #     }
        raise KeyError(status)


class DatabaseError(GraphError):
    """ The database failed to service the request.
    """


class TransientError(GraphError):
    """ The database cannot service the request right now, retrying later might yield a successful outcome.
    """


class GraphTransactionError(GraphError):
    """ Raised when actions are attempted against a :class:`.GraphTransaction`
    that is no longer available for use, or a transaction is otherwise invalid.
    """


class GraphTransaction(object):
    """ A logical context for one or more graph operations.
    """

    _finished = False

    def __init__(self, graph, autocommit=False, readonly=False,
                 after=None, metadata=None, timeout=None):
        self._graph = graph
        self._autocommit = autocommit
        self._entities = deque()
        self._connector = self.graph.service.connector
        if autocommit:
            self._transaction = None
        else:
            self._transaction = self._connector.begin(self._graph.name,
                                                      readonly, after, metadata, timeout)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self.commit()
        else:
            self.rollback()

    def _assert_unfinished(self):
        if self._finished:
            raise GraphTransactionError(self)

    @property
    def graph(self):
        return self._graph

    @property
    def entities(self):
        return self._entities

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
        self._assert_unfinished()
        try:
            entities = self._entities.popleft()
        except IndexError:
            entities = {}

        try:
            hydrant = Connection.default_hydrant(self._connector.profile, self.graph)
            parameters = dict(parameters or {}, **kwparameters)
            if self._transaction:
                result = self._connector.run_in_tx(self._transaction, cypher, parameters, hydrant)
            else:
                result = self._connector.auto_run(self.graph.name, cypher, parameters, hydrant)
            return Cursor(result, hydrant, entities)
        finally:
            if not self._transaction:
                self.finish()

    def finish(self):
        self._assert_unfinished()
        self._finished = True

    def commit(self):
        """ Commit the transaction.
        """
        self._assert_unfinished()
        try:
            return self._connector.commit(self._transaction)
        except TransactionError as error:
            error.__class__ = GraphTransactionError
            raise error
        finally:
            self._finished = True

    def rollback(self):
        """ Roll back the current transaction, undoing all actions previously taken.
        """
        self._assert_unfinished()
        try:
            return self._connector.rollback(self._transaction)
        except TransactionError as error:
            error.__class__ = GraphTransactionError
            raise error
        finally:
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
        """ Determine whether one or more entities all exist within the
        graph. Note that if any nodes or relationships in *subgraph* are not
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
            try:
                merge(self, primary_label, primary_key)
            except OperationError as e0:
                e1 = GraphTransactionError("Failed to merge %r" % (subgraph,))
                e1.__cause__ = e0
                raise e1

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

    def __init__(self, result, hydrant=None, entities=None):
        self._result = result
        self._hydrant = hydrant
        self._entities = entities
        self._current = None
        self._closed = False

    def __del__(self):
        try:
            self.close()
        except OSError:
            pass

    def __repr__(self):
        return repr(self.preview(3))

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
        if not self._closed:
            self._result.buffer()   # force consumption of remaining data
            self._closed = True

    def keys(self):
        """ Return the field names for the records in the stream.
        """
        return self._result.fields()

    def summary(self):
        """ Return the result summary.
        """
        self._result.buffer()
        metadata = self._result.summary()
        return CypherSummary(**metadata)

    def plan(self):
        """ Return the plan returned with this result, if any.
        """
        self._result.buffer()
        metadata = self._result.summary()
        if "plan" in metadata:
            return CypherPlan(**metadata["plan"])
        elif "profile" in metadata:
            return CypherPlan(**metadata["profile"])
        else:
            return None

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
        contained_updates: True
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
        self._result.buffer()
        metadata = self._result.summary()
        return CypherStats(**metadata.get("stats", {}))

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
        if amount < 0:
            raise ValueError("Cursor can only move forwards")
        amount = int(amount)
        moved = 0
        v = self._result.protocol_version
        while moved != amount:
            values = self._result.fetch()
            if values is None:
                break
            else:
                keys = self._result.fields()  # TODO: don't do this for every record
                if self._hydrant:
                    values = self._hydrant.hydrate(keys, values, entities=self._entities, version=v)
                self._current = Record(zip(keys, values))
                moved += 1
        return moved

    def preview(self, limit=1):
        """ Construct a :class:`.Table` containing a preview of
        upcoming records, including no more than the given `limit`.

        :param limit: maximum number of records to include in the
            preview
        :returns: :class:`.Table` containing the previewed records
        """
        if limit < 0:
            raise ValueError("Illegal preview size")
        v = self._result.protocol_version
        records = []
        keys = self._result.fields()
        for values in self._result.peek_records(int(limit)):
            if self._hydrant:
                values = self._hydrant.hydrate(keys, values, entities=self._entities, version=v)
            records.append(values)
        return Table(records, keys)

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
            >>> g.run("MATCH (a) WHERE a.email=$x RETURN a.name", x="bob@acme.com").evaluate()
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
