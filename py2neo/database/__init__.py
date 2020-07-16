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

from time import sleep
from warnings import warn

from py2neo.caching import ThreadLocalEntityCache
from py2neo.client import Connector
from py2neo.client.config import ConnectionProfile
from py2neo.compat import xstr
from py2neo.cypher import cypher_escape, Procedures
from py2neo.matching import NodeMatcher, RelationshipMatcher
from py2neo.text import Words


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

    def __new__(cls, profile=None, **settings):
        profile = ConnectionProfile(profile, **settings)
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
            inst.schema = Schema(inst)
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
        from packaging.version import Version
        components = self.default_graph.call("dbms.components").data()
        kernel_component = [component for component in components
                            if component["name"] == "Neo4j Kernel"][0]
        version_string = kernel_component["versions"][0]
        return Version(version_string)

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

        >>> from py2neo import Graph
        >>> sales = Graph("bolt+s://g.example.com:7687", name="sales")
        >>> sales.run("MATCH (c:Customer) RETURN c.name")
         c.name
        ---------------
         John Smith
         Amy Pond
         Rory Williams

    The `system graph`, which is available in all 4.x+ product editions,
    can also be accessed via the :class:`.SystemGraph` class.

        >>> from py2neo import SystemGraph
        >>> sg = SystemGraph("bolt+s://g.example.com:7687")
        >>> sg.call("dbms.security.listUsers")
         username | roles | flags
        ----------|-------|-------
         neo4j    |  null | []

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

    Note that py2neo does not support routing with a Neo4j causal
    cluster. For this functionality, please use the official Neo4j
    Driver for Python.
    """

    #: The :class:`.GraphService` to which this :class:`.Graph` belongs.
    service = None

    #: The :class:`.Schema` resource for this :class:`.Graph`.
    schema = None

    def __new__(cls, profile=None, name=None, **settings):
        gs = GraphService(profile, **settings)
        return gs[name]

    def __init__(self, *args, **kwargs):
        self._procedures = Procedures(self)

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
        from py2neo.database.tx import Transaction
        return Transaction(self, True, readonly, after, metadata, timeout)

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
        from py2neo.database.tx import Transaction
        if autocommit:
            warn("Graph.begin(autocommit=True) is deprecated, "
                 "use Graph.auto() instead", category=DeprecationWarning, stacklevel=2)
        return Transaction(self, autocommit, readonly, after, metadata, timeout)

    @property
    def call(self):
        """ Accessor for listing and running procedures.
        """
        return self._procedures

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
        if readonly:
            # TODO: remove this warning when readonly is implemented
            warn("Acquisition of readonly connections is not yet supported; "
                 "a read-write connection will be used instead")
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

    def __new__(cls, profile=None, **settings):
        return super(SystemGraph, cls).__new__(profile, name="system", **settings)

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


# TODO: refactor out this base class
class GraphError(Exception):
    """
    """

    __cause__ = None

    http_status_code = None
    code = None
    message = None

    @classmethod
    def hydrate(cls, data):
        from py2neo.database.tx import ClientError, DatabaseError, TransientError
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
        inst.classification = classification
        inst.category = category
        inst.title = title
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
