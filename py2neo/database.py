#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2021, Nigel Small
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


"""
The ``py2neo.database`` module contains classes and functions required
to interact with a Neo4j server, including classes pertaining to the
execution of Cypher queries and transactions. For convenience, these
are also exposed through the top-level package, ``py2neo``.

The most useful of the classes provided here is the :class:`.Graph`
class which represents a Neo4j graph database instance and provides
access to a large portion of the most commonly used py2neo API.

To run a query against a local database is straightforward::

    >>> from py2neo import Graph
    >>> graph = Graph(password="password")
    >>> graph.run("UNWIND range(1, 3) AS n RETURN n, n * n as n_sq").to_table()
       n | n_sq
    -----|------
       1 |    1
       2 |    4
       3 |    9


Getting connected
=================

The :class:`.GraphService`, :class:`.Graph`, and :class:`.SystemGraph`
classes all accept an argument called `profile` plus individual keyword
`settings`. Internally, these arguments are used to construct a
:class:`.ConnectionProfile` object which holds these details.

The `profile` can either be a URI or a base :class:`.ConnectionProfile`
object. The `settings` are individual overrides for the values within
that, such as ``host`` or ``password``. This override mechanism allows
several ways of specifying the same information. For example, the three
variants below are all equivalent::

    >>> from py2neo import Graph
    >>> graph_1 = Graph()
    >>> graph_2 = Graph(host="localhost")
    >>> graph_3 = Graph("bolt://localhost:7687")

Omitting the `profile` argument completely falls back to using the
default :class:`.ConnectionProfile`. More on this, and other useful
information, can be found in the documentation for that class.

URIs
----

The general format of a URI is ``<scheme>://[<user>[:<password>]@]<host>[:<port>]``.
Supported URI schemes are:

- ``bolt`` - Bolt (unsecured)
- ``bolt+s`` - Bolt (secured with full certificate checks)
- ``bolt+ssc`` - Bolt (secured with no certificate checks)
- ``http`` - HTTP (unsecured)
- ``https`` - HTTP (secured with full certificate checks)
- ``http+s`` - HTTP (secured with full certificate checks)
- ``http+ssc`` - HTTP (secured with no certificate checks)


Note that py2neo does not support routing URIs like ``neo4j://...``
for use with Neo4j causal clusters. To enable routing, instead pass
a ``routing=True`` keyword argument to the :class:`.Graph` or
:class:`.GraphService` constructor.

Routing is only available for Bolt-enabled servers. No equivalent
currently exists for HTTP.


Individual settings
-------------------

The full set of supported `settings` are:

============  =========================================  =====  =========================
Keyword       Description                                Type   Default
============  =========================================  =====  =========================
``scheme``    Use a specific URI scheme                  str    ``'bolt'``
``secure``    Use a secure connection (TLS)              bool   ``False``
``verify``    Verify the server certificate (if secure)  bool   ``True``
``host``      Database server host name                  str    ``'localhost'``
``port``      Database server port                       int    ``7687``
``address``   Colon-separated host and port string       str    ``'localhost:7687'``
``user``      User to authenticate as                    str    ``'neo4j'``
``password``  Password to use for authentication         str    ``'password'``
``auth``      A 2-tuple of (user, password)              tuple  ``('neo4j', 'password')``
``routing``   Route connections across multiple servers  bool   ``False``
============  =========================================  =====  =========================


"""

from __future__ import absolute_import, print_function, unicode_literals

from collections import OrderedDict
from functools import reduce
from inspect import isgenerator
from io import StringIO
from operator import xor as xor_operator
from time import sleep
from warnings import warn

from py2neo.compat import (deprecated,
                           Sequence,
                           Mapping,
                           numeric_types,
                           ustr)
from py2neo.cypher import cypher_escape, cypher_repr, cypher_str
from py2neo.errors import (Neo4jError,
                           ConnectionUnavailable,
                           ConnectionBroken,
                           ConnectionLimit,
                           ServiceUnavailable,
                           WriteServiceUnavailable)
from py2neo.matching import NodeMatcher, RelationshipMatcher


class GraphService(object):
    """ The :class:`.GraphService` class is the top-level accessor for
    an entire Neo4j graph database management system (DBMS). Within the
    py2neo object hierarchy, a :class:`.GraphService` contains one or
    more :class:`.Graph` objects in which data storage and retrieval
    activity chiefly occurs.

    An explicit URI can be passed to the constructor::

        >>> from py2neo import GraphService
        >>> gs = GraphService("bolt://camelot.example.com:7687")

    Alternatively, the default value of ``bolt://localhost:7687`` is
    used::

        >>> default_gs = GraphService()
        >>> default_gs
        <GraphService uri='bolt://localhost:7687'>

    .. note::

        Some attributes of this class available in earlier versions of
        py2neo are no longer available, specifically
        ``kernel_start_time``, ``primitive_counts``,
        ``store_creation_time``, ``store_file_sizes`` and ``store_id``,
        along with the ``query_jmx`` method. This is due to a change in
        Neo4j 4.0 relating to how certain system metadata is exposed.
        Replacement functionality may be reintroduced in a future
        py2neo release.

    *Changed in 2020.0: this class was formerly known as 'Database',
    but was renamed to avoid confusion with the concept of the same
    name introduced with the multi-database feature of Neo4j 4.0.*

    .. describe:: iter(graph_service)

        Yield all named graphs.

        For Neo4j 4.0 and above, this yields the names returned by a
        ``SHOW DATABASES`` query. For earlier versions, this yields no
        entries, since the one and only graph in these versions is not
        named.

        *New in version 2020.0.*

    .. describe:: graph_service[name]

        Access a :class:`.Graph` by name.

        *New in version 2020.0.*

    """

    _connector = None

    _graphs = None

    def __init__(self, profile=None, **settings):
        from py2neo.client import Connector
        from py2neo.client.config import ConnectionProfile
        profile = ConnectionProfile(profile, **settings)
        connector_settings = {
            "user_agent": settings.get("user_agent"),
            "init_size": settings.get("init_size"),
            "max_size": settings.get("max_size"),
            "max_age": settings.get("max_age"),
            "routing": settings.get("routing"),
            "routing_refresh_ttl": settings.get("routing_refresh_ttl"),
        }
        if connector_settings["init_size"] is None and not connector_settings["routing"]:
            # Ensures credentials are checked on construction
            connector_settings["init_size"] = 1
        self._connector = Connector(profile, **connector_settings)
        self._graphs = {}

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
        if graph_name not in self._graphs:
            graph_class = SystemGraph if graph_name == "system" else Graph
            self._graphs[graph_name] = graph_class(self.profile, name=graph_name)
        return self._graphs[graph_name]

    def __iter__(self):
        return iter(self._connector.graph_names())

    @property
    def connector(self):
        """ The :class:`.Connector` providing communication for this
        graph service.

        *New in version 2020.0.*
        """
        return self._connector

    @property
    def profile(self):
        """ The :class:`.ConnectionProfile` for which this graph
        service is configured. This attribute is simply a shortcut
        for ``connector.profile``.

        *New in version 2020.0.*
        """
        return self.connector.profile

    @property
    def uri(self):
        """ The URI to which this graph service is connected. This
        attribute is simply a shortcut for ``connector.profile.uri``.
        """
        return self.profile.uri

    @property
    def default_graph(self):
        """ The default :class:`.Graph` exposed by this graph service.
        """
        return self[None]

    @property
    def system_graph(self):
        """ The :class:`.SystemGraph` exposed by this graph service.

        *New in version 2020.0.*
        """
        return self["system"]

    def keys(self):
        """ Return a list of all :class:`.Graph` names exposed by this
        graph service.

        *New in version 2020.0.*
        """
        return list(self)

    @property
    def kernel_version(self):
        """ The :class:`~packaging.version.Version` of Neo4j running.
        """
        from packaging.version import Version
        components = self.default_graph.call("dbms.components").data()
        kernel_component = [component for component in components
                            if component["name"] == "Neo4j Kernel"][0]
        version_string = kernel_component["versions"][0]
        return Version(version_string)

    @property
    def product(self):
        """ The product name.
        """
        record = next(self.default_graph.call("dbms.components"))
        return "%s %s (%s)" % (record[0], " ".join(record[1]), record[2].title())

    @property
    def config(self):
        """ A dictionary of the configuration parameters used to
        configure Neo4j.

            >>> gs.config['dbms.connectors.default_advertised_address']
            'localhost'

        """
        return {record["name"]: record["value"]
                for record in self.default_graph.call("dbms.listConfig")}


class Graph(object):
    """ The `Graph` class provides a handle to an individual named
    graph database exposed by a Neo4j graph database service.

    Connection details are provided using either a URI or a
    :class:`.ConnectionProfile`, plus individual settings, if required.

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

    In addition to the core `connection details <#getting-connected>`_
    that can be passed to the constructor, the :class:`.Graph` class
    can accept several other settings:

    ===================  ========================================================  ==============  =========================
    Keyword              Description                                               Type            Default
    ===================  ========================================================  ==============  =========================
    ``user_agent``       User agent to send for all connections                    str             `(depends on URI scheme)`
    ``max_connections``  The maximum number of simultaneous connections permitted  int             40
    ===================  ========================================================  ==============  =========================

    Once obtained, the `Graph` instance provides direct or indirect
    access to most of the functionality available within py2neo.
    """

    #: The :class:`.GraphService` to which this :class:`.Graph` belongs.
    service = None

    #: The :class:`.Schema` resource for this :class:`.Graph`.
    schema = None

    def __init__(self, profile=None, name=None, **settings):
        self.service = GraphService(profile, **settings)
        self.__name__ = name
        self.schema = Schema(self)
        self._procedures = ProcedureLibrary(self)

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

    @property
    def name(self):
        """ The name of this graph.

        *New in version 2020.0.*
        """
        return self.__name__

    # TRANSACTION MANAGEMENT #

    def auto(self, readonly=False,
             # after=None, metadata=None, timeout=None
             ):
        """ Create a new auto-commit :class:`~py2neo.database.Transaction`.

        :param readonly: if :py:const:`True`, will begin a readonly
            transaction, otherwise will begin as read-write

        *New in version 2020.0.*
        """
        return Transaction(self, autocommit=True, readonly=readonly,
                           # after, metadata, timeout
                           )

    def begin(self, readonly=False,
              # after=None, metadata=None, timeout=None
              ):
        """ Begin a new :class:`~py2neo.database.Transaction`.

        :param readonly: if :py:const:`True`, will begin a readonly
            transaction, otherwise will begin as read-write

        *Changed in version 2021.1: the 'autocommit' argument has been
        removed. Use the 'auto' method instead.*
        """
        return Transaction(self, autocommit=False, readonly=readonly,
                           # after, metadata, timeout
                           )

    def commit(self, tx):
        """ Commit a transaction.

        :returns: :class:`.TransactionSummary` object

        *New in version 2021.1.*
        """
        if tx is None:
            return
        if not isinstance(tx, Transaction):
            raise TypeError("Bad transaction %r" % tx)
        if tx.closed:
            raise TypeError("Cannot commit closed transaction")
        try:
            summary = self.service.connector.commit(tx.ref)
            return TransactionSummary(**summary)
        finally:
            tx._closed = True

    def rollback(self, tx):
        """ Rollback a transaction.

        :returns: :class:`.TransactionSummary` object

        *New in version 2021.1.*
        """
        if tx is None or tx.closed:
            return
        if not isinstance(tx, Transaction):
            raise TypeError("Bad transaction %r" % tx)
        try:
            summary = self.service.connector.rollback(tx.ref)
            return TransactionSummary(**summary)
        except (ConnectionUnavailable, ConnectionBroken):
            pass
        finally:
            tx._closed = True

    # CYPHER EXECUTION #

    def run(self, cypher, parameters=None, **kwparameters):
        """ Run a single read/write query within an auto-commit
        :class:`~py2neo.database.Transaction`.

        :param cypher: Cypher statement
        :param parameters: dictionary of parameters
        :param kwparameters: extra parameters supplied as keyword
            arguments
        :return:
        """
        return self.auto().run(cypher, parameters, **kwparameters)

    def evaluate(self, cypher, parameters=None, **kwparameters):
        """ Run a :meth:`~py2neo.database.Transaction.evaluate` operation within an
        auto-commit :class:`~py2neo.database.Transaction`.

        :param cypher: Cypher statement
        :param parameters: dictionary of parameters
        :return: first value from the first record returned or
                 :py:const:`None`.
        """
        return self.run(cypher, parameters, **kwparameters).evaluate()

    def update(self, cypher, parameters=None, timeout=None):
        """ Call a function representing a transactional unit of work.

        The function must always accept a :class:`~py2neo.database.Transaction`
        object as its first argument. Additional arguments can be
        passed though the `args` and `kwargs` arguments of this method.

        The unit of work may be called multiple times if earlier
        attempts fail due to connectivity or other transient errors.
        As such, the function should have no non-idempotent side
        effects.

        :param cypher: cypher string or transaction function containing
            a unit of work
        :param parameters: cypher parameter map or function arguments
        :param timeout:
        :raises WriteServiceUnavailable: if the update does not
            successfully complete
        """
        if callable(cypher):
            if parameters is None:
                self._update(cypher, timeout=timeout)
            elif (isinstance(parameters, tuple) and len(parameters) == 2 and
                    isinstance(parameters[0], Sequence) and isinstance(parameters[1], Mapping)):
                self._update(lambda tx: cypher(tx, *parameters[0], **parameters[1]),
                             timeout=timeout)
            elif isinstance(parameters, Sequence):
                self._update(lambda tx: cypher(tx, *parameters), timeout=timeout)
            elif isinstance(parameters, Mapping):
                self._update(lambda tx: cypher(tx, **parameters), timeout=timeout)
            else:
                raise TypeError("Unrecognised parameter type")
        else:
            self._update(lambda tx: tx.update(cypher, parameters), timeout=timeout)

    def _update(self, f, timeout=None):
        from py2neo.timing import Timer
        # TODO: logging
        n = 0
        for _ in Timer.repeat(at_least=3, timeout=timeout):
            n += 1
            tx = None
            try:
                tx = self.begin(
                                # after=after, metadata=metadata, timeout=timeout
                                )
                value = f(tx)
                if isgenerator(value):
                    _ = list(value)     # exhaust the generator
                self.commit(tx)
            except (ConnectionUnavailable, ConnectionBroken, ConnectionLimit):
                self.rollback(tx)
                continue
            except Neo4jError as error:
                self.rollback(tx)
                if error.should_invalidate_routing_table():
                    self.service.connector.invalidate_routing_table(self.name)
                if error.should_retry():
                    continue
                else:
                    raise
            except Exception:
                self.rollback(tx)
                raise
            else:
                return
        raise WriteServiceUnavailable("Failed to execute update after %r tries" % n)

    def query(self, cypher, parameters=None, timeout=None):
        """ Run a single readonly query within an auto-commit
        :class:`~py2neo.database.Transaction`.

        :param cypher: Cypher statement
        :param parameters: dictionary of parameters
        :param timeout:
        :returns:
        :raises TypeError: if the underlying connection profile does not
            support readonly transactions
        :raises ServiceUnavailable: if the query does not successfully
            complete

        *Refactored from read to query in version 2021.1*
        """
        from py2neo.timing import Timer
        # TODO: logging
        n = 0
        for _ in Timer.repeat(at_least=3, timeout=timeout):
            n += 1
            try:
                result = self.auto(readonly=True).run(cypher, parameters)
            except (ConnectionUnavailable, ConnectionBroken, ConnectionLimit):
                continue
            except Neo4jError as error:
                if error.should_invalidate_routing_table():
                    self.service.connector.invalidate_routing_table(self.name)
                if error.should_retry():
                    continue
                else:
                    raise
            else:
                return result
        raise ServiceUnavailable("Failed to execute query after %r tries" % n)

    @property
    def call(self):
        """ Accessor for listing and calling procedures.

        This property contains a :class:`.ProcedureLibrary` object tied
        to this graph, which provides links to Cypher procedures in
        the underlying implementation.

        Calling a procedure requires only the regular Python function
        call syntax::

            >>> g = Graph()
            >>> g.call.dbms.components()
             name         | versions   | edition
            --------------|------------|-----------
             Neo4j Kernel | ['3.5.12'] | community

        The object returned from the call is a
        :class:`~py2neo.database.Cursor` object, identical to
        that obtained from running a normal Cypher query, and can
        therefore be consumed in a similar way.

        Procedure names can alternatively be supplied as a string::

            >>> g.call["dbms.components"]()
             name         | versions   | edition
            --------------|------------|-----------
             Neo4j Kernel | ['3.5.12'] | community

        Using :func:`dir` or :func:`iter` on the `call` attribute will
        yield a list of available procedure names.

        *New in version 2020.0.*
        """
        return self._procedures

    def delete_all(self):
        """ Delete all nodes and relationships from this :class:`.Graph`.

        .. warning::
            This method will permanently remove **all** nodes and relationships
            from the graph and cannot be undone.
        """
        self.run("MATCH (a) DETACH DELETE a")

    @deprecated("The graph.read(...) method is deprecated, "
                "use graph.query(...) instead")
    def read(self, cypher, parameters=None, **kwparameters):
        return self.query(cypher, dict(parameters or {}, **kwparameters))

    # SUBGRAPH OPERATIONS #

    def create(self, subgraph):
        """ Run a :meth:`~py2neo.database.Transaction.create` operation within a
        :class:`~py2neo.database.Transaction`.

        :param subgraph: a :class:`.Node`, :class:`.Relationship` or other
                       :class:`.Subgraph`
        """
        self.update(lambda tx: tx.create(subgraph))

    def delete(self, subgraph):
        """ Run a :meth:`~py2neo.database.Transaction.delete` operation within an
        auto-commit :class:`~py2neo.database.Transaction`. To delete only the
        relationships, use the :meth:`.separate` method.

        Note that only entities which are bound to corresponding
        remote entities though the ``graph`` and ``identity``
        attributes will trigger a deletion.

        :param subgraph: a :class:`.Node`, :class:`.Relationship` or other
                       :class:`.Subgraph` object
        """
        self.update(lambda tx: tx.delete(subgraph))

    def exists(self, subgraph):
        """ Run a :meth:`~py2neo.database.Transaction.exists` operation within an
        auto-commit :class:`~py2neo.database.Transaction`.

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
        """ Run a :meth:`~py2neo.database.Transaction.merge` operation within an
        auto-commit :class:`~py2neo.database.Transaction`.

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
        :meth:`~py2neo.database.Transaction.merge` method. Note that this is different
        to a Cypher MERGE.

        :param subgraph: a :class:`.Node`, :class:`.Relationship` or other
                       :class:`.Subgraph` object
        :param label: label on which to match any existing nodes
        :param property_keys: property keys on which to match any existing nodes
        """
        self.update(lambda tx: tx.merge(subgraph, label, *property_keys))

    @property
    def nodes(self):
        """ A :class:`.NodeMatcher` for this graph.

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
        self.update(lambda tx: tx.pull(subgraph))

    def push(self, subgraph):
        """ Push data from one or more entities to their remote counterparts.

        :param subgraph: the collection of nodes and relationships to push
        """
        self.update(lambda tx: tx.push(subgraph))

    @property
    def relationships(self):
        """ A :class:`.RelationshipMatcher` for this graph.

        This can be used to find relationships that match given criteria
        as well as efficiently count relationships.
        """
        return RelationshipMatcher(self)

    def separate(self, subgraph):
        """ Run a :meth:`~py2neo.database.Transaction.separate`
        operation within an auto-commit :class:`~py2neo.database.Transaction`.

        Note that only relationships which are bound to corresponding
        remote relationships though the ``graph`` and ``identity``
        attributes will trigger a deletion.

        :param subgraph: a :class:`.Node`, :class:`.Relationship` or other
                       :class:`.Subgraph`
        """
        self.update(lambda tx: tx.separate(subgraph))


class SystemGraph(Graph):
    """ A subclass of :class:`.Graph` that provides access to the
    system database for the remote DBMS. This is only available in
    Neo4j 4.0 and above.

    *New in version 2020.0.*
    """

    def __init__(self, profile=None, **settings):
        settings["name"] = "system"
        super(SystemGraph, self).__init__(profile, **settings)

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


class ProcedureLibrary(object):
    """ Accessor for listing and calling procedures.

    This object is typically constructed and accessed via the
    :meth:`.Graph.call` attribute. See the documentation for that
    attribute for usage information.

    *New in version 2020.0.*
    """

    def __init__(self, graph):
        self.graph = graph

    def __getattr__(self, name):
        return Procedure(self.graph, name)

    def __getitem__(self, name):
        return Procedure(self.graph, name)

    def __dir__(self):
        return list(self)

    def __iter__(self):
        proc = Procedure(self.graph, "dbms.procedures")
        for record in proc(keys=["name"]):
            yield record[0]

    def __call__(self, procedure, *args):
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
        return Procedure(self.graph, procedure)(*args)


class Procedure(object):
    """ Represents an individual procedure.

    *New in version 2020.0.*
    """

    def __init__(self, graph, name):
        self.graph = graph
        self.name = name

    def __getattr__(self, name):
        return Procedure(self.graph, self.name + "." + name)

    def __getitem__(self, name):
        return Procedure(self.graph, self.name + "." + name)

    def __dir__(self):
        proc = Procedure(self.graph, "dbms.procedures")
        prefix = self.name + "."
        return [record[0][len(prefix):] for record in proc(keys=["name"])
                if record[0].startswith(prefix)]

    def __call__(self, *args, **kwargs):
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
        procedure_name = ".".join(cypher_escape(part) for part in self.name.split("."))
        arg_list = [(str(i), arg) for i, arg in enumerate(args)]
        cypher = "CALL %s(%s)" % (procedure_name, ", ".join("$" + a[0] for a in arg_list))
        keys = kwargs.get("keys")
        if keys:
            cypher += " YIELD %s" % ", ".join(keys)
        return self.graph.run(cypher, dict(arg_list))


class Transaction(object):
    """ Logical context for one or more graph operations.

    Transaction objects are typically constructed by the
    :meth:`.Graph.auto` and :meth:`.Graph.begin` methods.
    Likewise, the :meth:`.Graph.commit` and :meth:`.Graph.rollback`
    methods can be used to finish a transaction.
    """

    def __init__(self, graph, autocommit=False, readonly=False,
                 # after=None, metadata=None, timeout=None
                 ):
        self._graph = graph
        self._autocommit = autocommit
        self._connector = self.graph.service.connector
        if autocommit:
            self._ref = None
        else:
            self._ref = self._connector.begin(self.graph.name, readonly=readonly,
                                              # after, metadata, timeout
                                              )
        self._readonly = readonly
        self._closed = False

    @property
    def graph(self):
        """ Graph to which this transaction belongs.
        """
        return self._graph

    @property
    def ref(self):
        """ Transaction reference.
        """
        return self._ref

    @property
    def readonly(self):
        """ :py:const:`True` if this is a readonly transaction,
        :py:const:`False` otherwise.
        """
        return self._readonly

    @property
    def closed(self):
        """ :py:const:`True` if this transaction is closed,
        :py:const:`False` otherwise.
        """
        return self._closed

    def run(self, cypher, parameters=None, **kwparameters):
        """ Send a Cypher query to the server for execution and return
        a :py:class:`.Cursor` for navigating its result.

        :param cypher: Cypher query
        :param parameters: dictionary of parameters
        :returns: :py:class:`.Cursor` object
        """
        from py2neo.client import Connection
        if self.closed:
            raise TypeError("Cannot run query in closed transaction")

        try:
            hydrant = Connection.default_hydrant(self._connector.profile, self.graph)
            parameters = dict(parameters or {}, **kwparameters)
            if self.ref:
                result = self._connector.run_query(self.ref, cypher, parameters)
            else:
                result = self._connector.auto_run(self.graph.name, cypher, parameters,
                                                  readonly=self.readonly)
            return Cursor(result, hydrant)
        finally:
            if not self.ref:
                self._closed = True

    def evaluate(self, cypher, parameters=None, **kwparameters):
        """ Execute a single Cypher query and return the value from
        the first column of the first record.

        :param cypher: Cypher statement
        :param parameters: dictionary of parameters
        :returns: single return value or :const:`None`
        """
        return self.run(cypher, parameters, **kwparameters).evaluate(0)

    def update(self, cypher, parameters=None, **kwparameters):
        """ Execute a single Cypher statement and discard any result
        returned.

        :param cypher: Cypher statement
        :param parameters: dictionary of parameters
        """
        self.run(cypher, parameters, **kwparameters).close()

    @deprecated("The transaction.commit() method is deprecated, "
                "use graph.commit(transaction) instead")
    def commit(self):
        """ Commit the transaction.

        :returns: :class:`.TransactionSummary` object
        """
        return self.graph.commit(self)

    @deprecated("The transaction.rollback() method is deprecated, "
                "use graph.rollback(transaction) instead")
    def rollback(self):
        """ Roll back the current transaction, undoing all actions
        previously taken.

        :returns: :class:`.TransactionSummary` object
        """
        return self.graph.rollback(self)

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
        if self._autocommit:
            raise TypeError("Create operations are not supported inside "
                            "auto-commit transactions")
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


class TransactionSummary(object):
    """ Summary information produced as the result of a
    :class:`.Transaction` commit or rollback.
    """

    def __init__(self, bookmark=None, profile=None, time=None):
        self.bookmark = bookmark
        self.profile = profile
        self.time = time


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

    def __init__(self, result, hydrant=None):
        self._result = result
        self._fields = self._result.fields()
        self._hydrant = hydrant
        self._current = None

    def __repr__(self):
        preview = self.preview(3)
        if preview:
            return repr(preview)
        else:
            return "(No data)"

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

    @property
    def closed(self):
        return self._result.offline

    def close(self):
        """ Close this cursor and free up all associated resources.
        """
        self._result.buffer()

    def keys(self):
        """ Return the field names for the records in the stream.
        """
        return self._fields

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
        while moved != amount:
            values = self._result.fetch()
            if values is None:
                break
            if self._hydrant:
                values = self._hydrant.hydrate_list(values)
            self._current = Record(self._fields, values)
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
        records = []
        if self._fields:
            for values in self._result.peek_records(int(limit)):
                if self._hydrant:
                    values = self._hydrant.hydrate_list(values)
                records.append(values)
            return Table(records, self._fields)
        else:
            return None

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
        self._result.buffer()
        if self.forward():
            try:
                return self[field]
            except IndexError:
                return None
        else:
            return None

    def data(self, *keys):
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

        :param keys: indexes or keys of the items to include; if none
                     are provided, all values will be included
        :returns: list of dictionary of values, keyed by field name
        :raises IndexError: if an out-of-bounds index is specified
        """
        return [record.data(*keys) for record in self]

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
        :returns: `ndarray
            <https://docs.scipy.org/doc/numpy/reference/generated/numpy.ndarray.html>`__ object.
        """
        try:
            # noinspection PyPackageRequirements
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
        :returns: `Series
            <http://pandas.pydata.org/pandas-docs/stable/dsintro.html#series>`__ object.
        """
        try:
            # noinspection PyPackageRequirements
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
        :returns: `DataFrame
            <http://pandas.pydata.org/pandas-docs/stable/dsintro.html#series>`__ object.
        """
        try:
            # noinspection PyPackageRequirements
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
        :returns: `Matrix
            <http://docs.sympy.org/latest/tutorial/matrices.html>`_ object.
        """
        try:
            # noinspection PyPackageRequirements
            from sympy import MutableMatrix, ImmutableMatrix
        except ImportError:
            warn("Sympy is not installed.")
            raise
        else:
            if mutable:
                return MutableMatrix(list(map(list, self)))
            else:
                return ImmutableMatrix(list(map(list, self)))


class Record(tuple, Mapping):
    """ A :class:`.Record` object holds an ordered, keyed collection of
    values. It is in many ways similar to a :class:`namedtuple` but
    allows field access only through bracketed syntax, and provides
    more functionality. :class:`.Record` extends both :class:`tuple`
    and :class:`Mapping`.

    .. describe:: record[index]
                  record[key]

        Return the value of *record* with the specified *key* or *index*.

    .. describe:: len(record)

        Return the number of fields in *record*.

    .. describe:: dict(record)

        Return a `dict` representation of *record*.

    """

    __keys = None

    def __new__(cls, keys, values):
        inst = tuple.__new__(cls, values)
        inst.__keys = keys
        return inst

    def __repr__(self):
        return "Record({%s})" % ", ".join("%r: %r" % (field, self[i])
                                          for i, field in enumerate(self.__keys))

    def __str__(self):
        return "\t".join(map(repr, (self[i] for i, _ in enumerate(self.__keys))))

    def __eq__(self, other):
        return dict(self) == dict(other)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return reduce(xor_operator, map(hash, self.items()))

    def __getitem__(self, key):
        if isinstance(key, slice):
            keys = self.__keys[key]
            values = super(Record, self).__getitem__(key)
            return self.__class__(zip(keys, values))
        index = self.index(key)
        if 0 <= index < len(self):
            return super(Record, self).__getitem__(index)
        else:
            return None

    def __getslice__(self, start, stop):
        key = slice(start, stop)
        keys = self.__keys[key]
        values = tuple(self)[key]
        return self.__class__(zip(keys, values))

    def get(self, key, default=None):
        """ Obtain a single value from the record by index or key. If the
        specified item does not exist, the default value is returned.

        :param key: index or key
        :param default: default value to be returned if `key` does not exist
        :return: selected value
        """
        try:
            index = self.__keys.index(ustr(key))
        except ValueError:
            return default
        if 0 <= index < len(self):
            return super(Record, self).__getitem__(index)
        else:
            return default

    def index(self, key):
        """ Return the index of the given item.
        """
        from six import integer_types, string_types
        if isinstance(key, integer_types):
            if 0 <= key < len(self.__keys):
                return key
            raise IndexError(key)
        elif isinstance(key, string_types):
            try:
                return self.__keys.index(key)
            except ValueError:
                raise KeyError(key)
        else:
            raise TypeError(key)

    def keys(self):
        """ Return the keys of the record.

        :return: list of key names
        """
        return list(self.__keys)

    def values(self, *keys):
        """ Return the values of the record, optionally filtering to
        include only certain values by index or key.

        :param keys: indexes or keys of the items to include; if none
                     are provided, all values will be included
        :return: list of values
        """
        if keys:
            d = []
            for key in keys:
                try:
                    i = self.index(key)
                except KeyError:
                    d.append(None)
                else:
                    d.append(self[i])
            return d
        return list(self)

    def items(self, *keys):
        """ Return the fields of the record as a list of key and value tuples

        :param keys: indexes or keys of the items to include; if none
                     are provided, all values will be included
        :return: list of (key, value) tuples
        """
        if keys:
            d = []
            for key in keys:
                try:
                    i = self.index(key)
                except KeyError:
                    d.append((key, None))
                else:
                    d.append((self.__keys[i], self[i]))
            return d
        return list((self.__keys[i], super(Record, self).__getitem__(i)) for i in range(len(self)))

    def data(self, *keys):
        """ Return the keys and values of this record as a dictionary,
        optionally including only certain values by index or key. Keys
        provided that do not exist within the record will be included
        but with a value of :py:const:`None`; indexes provided
        that are out of bounds will trigger an :exc:`IndexError`.

        :param keys: indexes or keys of the items to include; if none
                     are provided, all values will be included
        :return: dictionary of values, keyed by field name
        :raises: :exc:`IndexError` if an out-of-bounds index is specified
        """
        if keys:
            d = {}
            for key in keys:
                try:
                    i = self.index(key)
                except KeyError:
                    d[key] = None
                else:
                    d[self.__keys[i]] = self[i]
            return d
        return dict(self)

    def to_subgraph(self):
        """ Return a :class:`.Subgraph` containing the union of all the
        graph structures within this :class:`.Record`.

        :return: :class:`.Subgraph` object
        """
        from py2neo.data import Subgraph
        s = None
        for value in self.values():
            if isinstance(value, Subgraph):
                if s is None:
                    s = value
                else:
                    s |= value
        return s


class Table(list):
    """ Immutable list of records.

    A :class:`.Table` holds a list of :class:`.Record` objects, typically received as the result of a Cypher query.
    It provides a convenient container for working with a result in its entirety and provides methods for conversion into various output formats.
    :class:`.Table` extends ``list``.

    .. describe:: repr(table)

        Return a string containing an ASCII art representation of this table.
        Internally, this method calls :meth:`.write` with `header=True`, writing the output into an ``io.StringIO`` instance.

    """

    def __init__(self, records, keys=None):
        super(Table, self).__init__(map(tuple, records))
        if keys:
            k = list(map(ustr, keys))
        else:
            try:
                k = records.keys()
            except AttributeError:
                raise ValueError("Missing keys")
        width = len(k)
        t = [set() for _ in range(width)]
        o = [False] * width
        for record in self:
            for i, value in enumerate(record):
                if value is None:
                    o[i] = True
                else:
                    t[i].add(type(value))
        f = []
        for i, _ in enumerate(k):
            f.append({
                "type": t[i].copy().pop() if len(t[i]) == 1 else tuple(t[i]),
                "numeric": all(t_ in numeric_types for t_ in t[i]),
                "optional": o[i],
            })
        self._keys = k
        self._fields = f

    def __repr__(self):
        s = StringIO()
        self.write(file=s, header=True)
        return s.getvalue()

    def _repr_html_(self):
        """ Return a string containing an HTML representation of this table.
        This method is used by Jupyter notebooks to display the table natively within a browser.
        Internally, this method calls :meth:`.write_html` with `header=True`, writing the output into an ``io.StringIO`` instance.
        """
        s = StringIO()
        self.write_html(file=s, header=True)
        return s.getvalue()

    def keys(self):
        """ Return a list of field names for this table.
        """
        return list(self._keys)

    def field(self, key):
        """ Return a dictionary of metadata for a given field.
        The metadata includes the following values:

        `type`
            Single class or tuple of classes representing the
            field values.

        `numeric`
            :const:`True` if all field values are of a numeric
            type, :const:`False` otherwise.

        `optional`
            :const:`True` if any field values are :const:`None`,
            :const:`False` otherwise.

        """
        from six import integer_types, string_types
        if isinstance(key, integer_types):
            return self._fields[key]
        elif isinstance(key, string_types):
            try:
                index = self._keys.index(key)
            except ValueError:
                raise KeyError(key)
            else:
                return self._fields[index]
        else:
            raise TypeError(key)

    def _range(self, skip, limit):
        if skip is None:
            skip = 0
        if limit is None or skip + limit > len(self):
            return range(skip, len(self))
        else:
            return range(skip, skip + limit)

    def write(self, file=None, header=None, skip=None, limit=None, auto_align=True,
              padding=1, separator=u"|", newline=u"\r\n"):
        """ Write data to a human-readable ASCII art table.

        :param file: file-like object capable of receiving output
        :param header: boolean flag for addition of column headers
        :param skip: number of records to skip before beginning output
        :param limit: maximum number of records to include in output
        :param auto_align: if :const:`True`, right-justify numeric values
        :param padding: number of spaces to include between column separator and value
        :param separator: column separator character
        :param newline: newline character sequence
        :return: the number of records included in output
        """

        space = u" " * padding
        widths = [1 if header else 0] * len(self._keys)

        def calc_widths(values, **_):
            strings = [cypher_str(value).splitlines(False) for value in values]
            for i, s in enumerate(strings):
                w = max(map(len, s)) if s else 0
                if w > widths[i]:
                    widths[i] = w

        def write_line(values, underline=u""):
            strings = [cypher_str(value).splitlines(False) for value in values]
            height = max(map(len, strings)) if strings else 1
            for y in range(height):
                line_text = u""
                underline_text = u""
                for x, _ in enumerate(values):
                    try:
                        text = strings[x][y]
                    except IndexError:
                        text = u""
                    if auto_align and self._fields[x]["numeric"]:
                        text = space + text.rjust(widths[x]) + space
                        u_text = underline * len(text)
                    else:
                        text = space + text.ljust(widths[x]) + space
                        u_text = underline * len(text)
                    if x > 0:
                        text = separator + text
                        u_text = separator + u_text
                    line_text += text
                    underline_text += u_text
                if underline:
                    line_text += newline + underline_text
                line_text += newline
                print(line_text, end=u"", file=file)

        def apply(f):
            count = 0
            for count, index in enumerate(self._range(skip, limit), start=1):
                if count == 1 and header:
                    f(self.keys(), underline=u"-")
                f(self[index])
            return count

        apply(calc_widths)
        return apply(write_line)

    def write_html(self, file=None, header=None, skip=None, limit=None, auto_align=True):
        """ Write data to an HTML table.

        :param file: file-like object capable of receiving output
        :param header: boolean flag for addition of column headers
        :param skip: number of records to skip before beginning output
        :param limit: maximum number of records to include in output
        :param auto_align: if :const:`True`, right-justify numeric values
        :return: the number of records included in output
        """

        def html_escape(s):
            return (s.replace(u"&", u"&amp;")
                     .replace(u"<", u"&lt;")
                     .replace(u">", u"&gt;")
                     .replace(u'"', u"&quot;")
                     .replace(u"'", u"&#039;"))

        def write_tr(values, tag):
            print(u"<tr>", end="", file=file)
            for i, value in enumerate(values):
                if tag == "th":
                    template = u'<{}>{}</{}>'
                elif auto_align and self._fields[i]["numeric"]:
                    template = u'<{} style="text-align:right">{}</{}>'
                else:
                    template = u'<{} style="text-align:left">{}</{}>'
                print(template.format(tag, html_escape(cypher_str(value)), tag), end="", file=file)
            print(u"</tr>", end="", file=file)

        count = 0
        print(u"<table>", end="", file=file)
        for count, index in enumerate(self._range(skip, limit), start=1):
            if count == 1 and header:
                write_tr(self.keys(), u"th")
            write_tr(self[index], u"td")
        print(u"</table>", end="", file=file)
        return count

    def write_separated_values(self, separator, file=None, header=None, skip=None, limit=None,
                               newline=u"\r\n", quote=u"\""):
        """ Write data to a delimiter-separated file.

        :param separator: field separator character
        :param file: file-like object capable of receiving output
        :param header: boolean flag or string style tag, such as 'i' or 'cyan',
            for addition of column headers
        :param skip: number of records to skip before beginning output
        :param limit: maximum number of records to include in output
        :param newline: newline character sequence
        :param quote: quote character
        :return: the number of records included in output
        """
        from pansi import ansi
        from six import string_types

        escaped_quote = quote + quote
        quotable = separator + newline + quote

        def header_row(names):
            if isinstance(header, string_types):
                if hasattr(ansi, header):
                    template = "{%s}{}{_}" % header
                else:
                    t = [tag for tag in dir(ansi) if
                         not tag.startswith("_") and isinstance(getattr(ansi, tag), str)]
                    raise ValueError("Unknown style tag %r\n"
                                     "Available tags are: %s" % (header, ", ".join(map(repr, t))))
            else:
                template = "{}"
            for name in names:
                yield template.format(name, **ansi)

        def data_row(values):
            for value in values:
                if value is None:
                    yield ""
                    continue
                if isinstance(value, string_types):
                    value = ustr(value)
                    if any(ch in value for ch in quotable):
                        value = quote + value.replace(quote, escaped_quote) + quote
                else:
                    value = cypher_repr(value)
                yield value

        count = 0
        for count, index in enumerate(self._range(skip, limit), start=1):
            if count == 1 and header:
                print(*header_row(self.keys()), sep=separator, end=newline, file=file)
            print(*data_row(self[index]), sep=separator, end=newline, file=file)
        return count

    def write_csv(self, file=None, header=None, skip=None, limit=None):
        """ Write the data as RFC4180-compatible comma-separated values.
        This is a customised call to :meth:`.write_separated_values`.
        """
        return self.write_separated_values(u",", file, header, skip, limit)

    def write_tsv(self, file=None, header=None, skip=None, limit=None):
        """ Write the data as tab-separated values.
        This is a customised call to :meth:`.write_separated_values`.
        """
        return self.write_separated_values(u"\t", file, header, skip, limit)


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
        from english.casing import Words
        return Words(key).snake()

    @classmethod
    def _clean_keys(cls, data):
        return OrderedDict(sorted((cls._clean_key(key), value)
                                  for key, value in dict(data).items()))

    def __init__(self, **kwargs):
        data = self._clean_keys(kwargs)
        if "root" in data:
            data = self._clean_keys(data["root"])
        self.operator_type = data.pop("operator_type", None)
        self.identifiers = data.pop("identifiers", [])
        self.children = [CypherPlan(**self._clean_keys(child))
                         for child in data.pop("children", [])]
        try:
            args = data.pop("args")
        except KeyError:
            self.args = data
        else:
            self.args = self._clean_keys(args)

    def __repr__(self):
        return ("%s(operator_type=%r, identifiers=%r, children=%r, args=%r)" %
                (self.__class__.__name__, self.operator_type,
                 self.identifiers, self.children, self.args))

    def __getitem__(self, key):
        return getattr(self, key)

    def __len__(self):
        return len(self.keys())

    def __iter__(self):
        return iter(self.keys())

    def keys(self):
        return ["operator_type", "identifiers", "children", "args"]
