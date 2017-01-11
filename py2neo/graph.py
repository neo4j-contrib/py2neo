#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2017, Nigel Small
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

import webbrowser
from collections import deque, OrderedDict
from email.utils import parsedate_tz, mktime_tz
from sys import stdout

from neo4j.v1 import GraphDatabase

from py2neo.compat import Mapping, string, ustr
from py2neo.cypher import cypher_escape
from py2neo.http import OK, NO_CONTENT, NOT_FOUND, register_http_driver, Remote, remote
from py2neo.meta import BOLT_USER_AGENT, HTTP_USER_AGENT
from py2neo.packstream import PackStreamValueSystem
from py2neo.selection import NodeSelector
from py2neo.status import *
from py2neo.types import cast_node, Subgraph, Node, Relationship
from py2neo.util import is_collection, version_tuple


register_http_driver()


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
    """ Accessor for the entire database management system belonging to
    a Neo4j server installation. This corresponds to the root URI in
    the HTTP API.

    An explicit URI can be passed to the constructor::

        >>> from py2neo import GraphService
        >>> gs = GraphService("http://myserver:7474/")

    Alternatively, the default value of ``http://localhost:7474/`` is
    used::

        >>> default_gs = GraphService()
        >>> default_gs
        <GraphService uri='http://localhost:7474/'>

    """

    __instances = {}

    _http_driver = None
    _bolt_driver = None
    _jmx_remote = None
    _graphs = None

    def __new__(cls, *uris, **settings):
        from py2neo.addressing import register_graph_service, get_graph_service_auth
        address = register_graph_service(*uris, **settings)
        try:
            inst = cls.__instances[address]
        except KeyError:
            http_uri = address.http_uri
            bolt_uri = address.bolt_uri
            inst = super(GraphService, cls).__new__(cls)
            inst._uris = uris
            inst._settings = settings
            inst.address = address
            auth = get_graph_service_auth(address)
            inst.__remote__ = Remote(http_uri["/"])
            auth_token = auth.token if auth else None
            inst._http_driver = GraphDatabase.driver(http_uri["/"], auth=auth_token, encrypted=address.secure, user_agent=HTTP_USER_AGENT)
            if bolt_uri:
                inst._bolt_driver = GraphDatabase.driver(bolt_uri["/"], auth=auth_token, encrypted=address.secure, user_agent=BOLT_USER_AGENT)
            inst._jmx_remote = Remote(http_uri["/db/manage/server/jmx/domain/org.neo4j"])
            inst._graphs = {}
            cls.__instances[address] = inst
        return inst

    def __del__(self):
        if self._http_driver:
            self._http_driver.close()
        if self._bolt_driver:
            self._bolt_driver.close()

    def __repr__(self):
        return "<%s uri=%r>" % (self.__class__.__name__, remote(self).uri)

    def __eq__(self, other):
        try:
            return remote(self) == remote(other)
        except AttributeError:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash(remote(self).uri)

    def __contains__(self, database):
        return database in self._graphs

    def __getitem__(self, database):
        return self._graphs[database]

    def __setitem__(self, database, graph):
        self._graphs[database] = graph

    def __iter__(self):
        yield "data"

    @property
    def driver(self):
        return self._bolt_driver or self._http_driver

    @property
    def http_driver(self):
        return self._http_driver

    @property
    def bolt_driver(self):
        return self._bolt_driver

    @property
    def graph(self):
        """ The default graph database exposed by this database management system.

        :rtype: :class:`.Graph`
        """
        return self["data"]

    def keys(self):
        return list(self)

    def _bean_dict(self, name):
        info = remote(self).get_json("db/manage/server/jmx/domain/org.neo4j")
        raw_config = [b for b in info["beans"] if b["name"].endswith("name=%s" % name)][0]
        d = {}
        for attribute in raw_config["attributes"]:
            name = attribute["name"]
            value = attribute.get("value")
            if value == "true":
                d[name] = True
            elif value == "false":
                d[name] = False
            else:
                try:
                    d[name] = int(value)
                except (TypeError, ValueError):
                    d[name] = value
        return d

    @property
    def database_name(self):
        """ Return the name of the active Neo4j database.
        """
        info = self._bean_dict("Kernel")
        return info.get("DatabaseName")

    @property
    def kernel_start_time(self):
        """ Return the time from which this Neo4j instance was in operational mode.
        """
        info = self._bean_dict("Kernel")
        return mktime_tz(parsedate_tz(info["KernelStartTime"]))

    @property
    def kernel_version(self):
        """ Return the version of Neo4j.
        """
        info = self._bean_dict("Kernel")
        version_string = info["KernelVersion"].partition("version:")[-1].partition(",")[0].strip()
        return version_tuple(version_string)

    @property
    def store_creation_time(self):
        """ Return the time when this Neo4j graph store was created.
        """
        info = self._bean_dict("Kernel")
        return mktime_tz(parsedate_tz(info["StoreCreationDate"]))

    @property
    def store_directory(self):
        """ Return the location of the Neo4j store.
        """
        info = self._bean_dict("Kernel")
        return info.get("StoreDirectory")

    @property
    def store_id(self):
        """ Return an identifier that, together with store creation time,
        uniquely identifies this Neo4j graph store.
        """
        info = self._bean_dict("Kernel")
        return info["StoreId"]

    @property
    def primitive_counts(self):
        """ Return a dictionary of estimates of the numbers of different
        kinds of Neo4j primitives.
        """
        return self._bean_dict("Primitive count")

    @property
    def store_file_sizes(self):
        """ Return a dictionary of file sizes for each file in the Neo4j
        graph store.
        """
        return self._bean_dict("Store file sizes")

    @property
    def config(self):
        """ Return a dictionary of the configuration parameters used to
        configure Neo4j.
        """
        return self._bean_dict("Configuration")

    @property
    def supports_auth(self):
        """ Returns :py:const:`True` if auth is supported by this
        version of Neo4j, :py:const:`False` otherwise.
        """
        return self.kernel_version >= (2, 2)

    @property
    def supports_bolt(self):
        """ Returns :py:const:`True` if Bolt is supported by this
        version of Neo4j, :py:const:`False` otherwise.
        """
        return self.kernel_version >= (3,)

    @property
    def supports_detach_delete(self):
        """ Returns :py:const:`True` if Cypher DETACH DELETE is
        supported by this version of Neo4j, :py:const:`False`
        otherwise.
        """
        return self.kernel_version >= (2, 3,)


class Graph(object):
    """ The `Graph` class represents a Neo4j graph database. Connection
    details are provided using URIs and/or individual settings. For any
    given `Graph`, the following protocol combinations are supported:

    - HTTP
    - HTTPS
    - Bolt + HTTP
    - Bolt/TLS + HTTPS

    Note that either HTTP or HTTPS must be enabled to allow for
    discovery and for some legacy features to be supported.

    The full set of `settings` supported are:

    ==============  =============================================  ==============  =============
    Keyword         Description                                    Type(s)         Default
    ==============  =============================================  ==============  =============
    ``bolt``        Use Bolt* protocol (`None` means autodetect)   bool, ``None``  ``None``
    ``secure``      Use a secure connection (Bolt/TLS + HTTPS)     bool            ``False``
    ``host``        Database server host name                      str             ``'localhost'``
    ``http_port``   Port for HTTP traffic                          int             ``7474``
    ``https_port``  Port for HTTPS traffic                         int             ``7473``
    ``bolt_port``   Port for Bolt traffic                          int             ``7687``
    ``user``        User to authenticate as                        str             ``'neo4j'``
    ``password``    Password to use for authentication             str             `no default`
    ==============  =============================================  ==============  =============

    *\* The new Bolt binary protocol is the successor to HTTP and available in Neo4j 3.0 and above.*

    Each setting can be provided as a keyword argument or as part of
    an ``http:``, ``https:`` or ``bolt:`` URI. Therefore, the examples
    below are equivalent::

        >>> from py2neo import Graph
        >>> graph_1 = Graph()
        >>> graph_2 = Graph(host="localhost")
        >>> graph_3 = Graph("http://localhost:7474/db/data/")

    Once obtained, the `Graph` instance provides direct or indirect
    access to most of the functionality available within py2neo. If
    Bolt is available (Neo4j 3.0 and above) and Bolt auto-detection
    is enabled, this will be used for Cypher queries instead of HTTP.
    """

    _graph_service = None

    _schema = None

    def __new__(cls, *uris, **settings):
        database = settings.pop("database", "data")
        graph_service = GraphService(*uris, **settings)
        address = graph_service.address
        if database in graph_service:
            inst = graph_service[database]
        else:
            inst = super(Graph, cls).__new__(cls)
            inst.address = address
            inst.__remote__ = Remote(address.http_uri["/db/%s/" % database])
            inst.transaction_uri = address.http_uri["/db/%s/transaction" % database]
            inst.node_selector = NodeSelector(inst)
            inst._graph_service = graph_service
            graph_service[database] = inst
        return inst

    def __repr__(self):
        return "<Graph uri=%r>" % remote(self).uri

    def __hash__(self):
        return hash(remote(self).uri)

    def __order__(self):
        return self.evaluate("MATCH (n) RETURN count(n)")

    def __size__(self):
        return self.evaluate("MATCH ()-[r]->() RETURN count(r)")

    def __len__(self):
        return self.__size__()

    def __bool__(self):
        return True

    def __nonzero__(self):
        return True

    def __contains__(self, entity):
        remote_entity = remote(entity)
        return remote_entity and remote_entity.uri.startswith(remote(self).uri)

    def begin(self, autocommit=False):
        """ Begin a new :class:`.Transaction`.

        :param autocommit: if :py:const:`True`, the transaction will
                         automatically commit after the first operation
        """
        return Transaction(self, autocommit)

    def create(self, subgraph):
        """ Run a :meth:`.Transaction.create` operation within an
        `autocommit` :class:`.Transaction`.

        :param subgraph: a :class:`.Node`, :class:`.Relationship` or other
                       :class:`.Subgraph`
        """
        self.begin(autocommit=True).create(subgraph)

    def data(self, statement, parameters=None, **kwparameters):
        """ Run a :meth:`.Transaction.run` operation within an
        `autocommit` :class:`.Transaction` and extract the data
        as a list of dictionaries.

        For example::

            >>> from py2neo import Graph
            >>> graph = Graph(password="excalibur")
            >>> graph.data("MATCH (a:Person) RETURN a.name, a.born LIMIT 4")
            [{'a.born': 1964, 'a.name': 'Keanu Reeves'},
             {'a.born': 1967, 'a.name': 'Carrie-Anne Moss'},
             {'a.born': 1961, 'a.name': 'Laurence Fishburne'},
             {'a.born': 1960, 'a.name': 'Hugo Weaving'}]

        The extracted data can then be easily passed into an external data handler such as a
        `pandas.DataFrame <http://pandas.pydata.org/pandas-docs/stable/dsintro.html#dataframe>`_
        for subsequent processing::

            >>> from pandas import DataFrame
            >>> DataFrame(graph.data("MATCH (a:Person) RETURN a.name, a.born LIMIT 4"))
               a.born              a.name
            0    1964        Keanu Reeves
            1    1967    Carrie-Anne Moss
            2    1961  Laurence Fishburne
            3    1960        Hugo Weaving

        .. seealso:: :meth:`.Cursor.data`

        :param statement: Cypher statement
        :param parameters: dictionary of parameters
        :param kwparameters: additional keyword parameters
        :return: the full query result
        :rtype: `list` of `dict`
        """
        return self.begin(autocommit=True).run(statement, parameters, **kwparameters).data()

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
        if self.graph_service.supports_detach_delete:
            self.run("MATCH (a) DETACH DELETE a")
        else:
            self.run("MATCH (a) OPTIONAL MATCH (a)-[r]->() DELETE r, a")
        Node.cache.clear()
        Relationship.cache.clear()

    def evaluate(self, statement, parameters=None, **kwparameters):
        """ Run a :meth:`.Transaction.evaluate` operation within an
        `autocommit` :class:`.Transaction`.

        :param statement: Cypher statement
        :param parameters: dictionary of parameters
        :return: first value from the first record returned or
                 :py:const:`None`.
        """
        return self.begin(autocommit=True).evaluate(statement, parameters, **kwparameters)

    def exists(self, subgraph):
        """ Run a :meth:`.Transaction.exists` operation within an
        `autocommit` :class:`.Transaction`.

        :param subgraph: a :class:`.Node`, :class:`.Relationship` or other
                       :class:`.Subgraph` object
        :return:
        """
        return self.begin(autocommit=True).exists(subgraph)

    @property
    def graph_service(self):
        """ The database management system to which this :class:`.Graph`
        instance belongs.
        """
        return self._graph_service

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
            start_node = cast_node(start_node)
            if not remote(start_node):
                raise TypeError("Nodes for relationship match end points must be bound")
            parameters = {"1": remote(start_node)._id}
            returns.append("b")
        elif start_node is None:
            clauses.append("MATCH (b) WHERE id(b) = {2}")
            end_node = cast_node(end_node)
            if not remote(end_node):
                raise TypeError("Nodes for relationship match end points must be bound")
            parameters = {"2": remote(end_node)._id}
            returns.append("a")
        else:
            clauses.append("MATCH (a) WHERE id(a) = {1} MATCH (b) WHERE id(b) = {2}")
            start_node = cast_node(start_node)
            end_node = cast_node(end_node)
            if not remote(start_node) or not remote(end_node):
                raise TypeError("Nodes for relationship match end points must be bound")
            parameters = {"1": remote(start_node)._id, "2": remote(end_node)._id}
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
        self.begin(autocommit=True).merge(subgraph, label, *property_keys)

    def node(self, id_):
        """ Fetch a node by ID. This method creates an object representing the
        remote node with the ID specified but fetches no data from the server.
        For this reason, there is no guarantee that the entity returned
        actually exists.

        :param id_:
        """
        entity_uri = "%snode/%s" % (remote(self).uri, id_)
        try:
            return Node.cache[entity_uri]
        except KeyError:
            node = self.node_selector.select().where("id(_) = %d" % id_).first()
            if node is None:
                raise IndexError("Node %d not found" % id_)
            else:
                return node

    @property
    def node_labels(self):
        """ The set of node labels currently defined within the graph.
        """
        return frozenset(remote(self).get_json("labels"))

    def open_browser(self):
        """ Open a page in the default system web browser pointing at
        the Neo4j browser application for this graph.
        """
        webbrowser.open(remote(self.graph_service).uri)

    def pull(self, subgraph):
        """ Pull data to one or more entities from their remote counterparts.

        :param subgraph: the collection of nodes and relationships to pull
        """
        self.begin(autocommit=True).pull(subgraph)

    def push(self, subgraph):
        """ Push data from one or more entities to their remote counterparts.

        :param subgraph: the collection of nodes and relationships to push
        """
        self.begin(autocommit=True).push(subgraph)

    def relationship(self, id_):
        """ Fetch a relationship by ID.

        :param id_:
        """
        entity_uri = "%srelationship/%s" % (remote(self).uri, id_)
        try:
            return Relationship.cache[entity_uri]
        except KeyError:
            relationship = self.evaluate("MATCH ()-[r]->() WHERE id(r)={x} RETURN r", x=id_)
            if relationship is None:
                raise IndexError("Relationship %d not found" % id_)
            else:
                return relationship

    @property
    def relationship_types(self):
        """ The set of relationship types currently defined within the graph.
        """
        return frozenset(remote(self).get_json("relationship/types"))

    def run(self, statement, parameters=None, **kwparameters):
        """ Run a :meth:`.Transaction.run` operation within an
        `autocommit` :class:`.Transaction`.

        :param statement: Cypher statement
        :param parameters: dictionary of parameters
        :return:
        """
        return self.begin(autocommit=True).run(statement, parameters, **kwparameters)

    def separate(self, subgraph):
        """ Run a :meth:`.Transaction.separate` operation within an
        `autocommit` :class:`.Transaction`.

        :param subgraph: a :class:`.Node`, :class:`.Relationship` or other
                       :class:`.Subgraph`
        """
        self.begin(autocommit=True).separate(subgraph)

    @property
    def schema(self):
        """ The schema resource for this graph.

        :rtype: :class:`Schema`
        """
        if self._schema is None:
            self._schema = Schema(self, "schema")
        return self._schema


class Schema(object):
    """ The schema resource attached to a `Graph` instance.
    """

    def __init__(self, graph, ref):
        self.remote_graph = remote(graph)
        self._index_ref = ref + "/index/{label}"
        self._index_key_ref = ref + "/index/{label}/{property_key}"
        self._uniqueness_constraint_ref = ref + "/constraint/{label}/uniqueness"
        self._uniqueness_constraint_key_ref = ref + "/constraint/{label}/uniqueness/{property_key}"

    def create_index(self, label, property_key):
        """ Create a schema index for a label and property
        key combination.
        """
        ref = self._index_ref.format(label=label)
        self.remote_graph.post(ref, {"property_keys": [property_key]}, expected=(OK,)).close()

    def create_uniqueness_constraint(self, label, property_key):
        """ Create a uniqueness constraint for a label.
        """
        ref = self._uniqueness_constraint_ref.format(label=label)
        self.remote_graph.post(ref, {"property_keys": [property_key]}, expected=(OK,)).close()

    def drop_index(self, label, property_key):
        """ Remove label index for a given property key.
        """
        ref = self._index_key_ref.format(label=label, property_key=property_key)
        rs = self.remote_graph.delete(ref, expected=(NO_CONTENT, NOT_FOUND))
        if rs.status == NOT_FOUND:
            raise GraphError("No such schema index (label=%r, key=%r)" % (label, property_key))

    def drop_uniqueness_constraint(self, label, property_key):
        """ Remove the uniqueness constraint for a given property key.
        """
        ref = self._uniqueness_constraint_key_ref.format(label=label, property_key=property_key)
        rs = self.remote_graph.delete(ref, expected=(NO_CONTENT, NOT_FOUND))
        if rs.status == NOT_FOUND:
            raise GraphError("No such unique constraint (label=%r, key=%r)" % (label, property_key))

    def get_indexes(self, label):
        """ Fetch a list of indexed property keys for a label.
        """
        ref = self._index_ref.format(label=label)
        return [indexed["property_keys"][0] for indexed in self.remote_graph.get_json(ref)]

    def get_uniqueness_constraints(self, label):
        """ Fetch a list of unique constraints for a label.
        """
        ref = self._uniqueness_constraint_ref.format(label=label)
        return [unique["property_keys"][0] for unique in self.remote_graph.get_json(ref)]


class Result(object):
    """ Wraps a BoltStatementResult
    """

    def __init__(self, graph, entities, result):
        from py2neo.http import HTTPStatementResult
        from neo4j.v1 import BoltStatementResult
        self.result = result
        self.result.error_class = GraphError.hydrate
        # TODO: un-yuk this
        if isinstance(result, HTTPStatementResult):
            self.result.value_system.entities = entities
        elif isinstance(result, BoltStatementResult):
            self.result.value_system = PackStreamValueSystem(graph, result.keys(), entities)
        else:
            raise RuntimeError("Unexpected statement result class %r" % result.__class__.__name__)
        self.result.zipper = Record
        self.result_iterator = iter(self.result)

    @property
    def loaded(self):
        return not self.result.online()

    def keys(self):
        """ Return the keys for the whole data set.
        """
        return self.result.keys()

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
        self.driver = driver = self.graph.graph_service.driver
        self.session = driver.session()
        self.sources = []
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

    def run(self, statement, parameters=None, **kwparameters):
        """ Send a Cypher statement to the server for execution and return
        a :py:class:`.Cursor` for navigating its result.

        :param statement: Cypher statement
        :param parameters: dictionary of parameters
        :returns: :py:class:`.Cursor` object
        """
        self._assert_unfinished()
        try:
            entities = self.entities.popleft()
        except IndexError:
            entities = {}

        if self.transaction:
            result = self.transaction.run(statement, parameters, **kwparameters)
        else:
            result = self.session.run(statement, parameters, **kwparameters)
        source = Result(self.graph, entities, result)
        self.sources.append(source)
        if not self.transaction:
            self.finish()
        return Cursor(source)

    def process(self):
        """ Send all pending statements to the server for processing.
        """
        if self.transaction:
            self.transaction.sync()
        else:
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

    def evaluate(self, statement, parameters=None, **kwparameters):
        """ Execute a single Cypher statement and return the value from
        the first column of the first record.

        :param statement: Cypher statement
        :param parameters: dictionary of parameters
        :returns: single return value or :const:`None`
        """
        return self.run(statement, parameters, **kwparameters).evaluate(0)

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

    def __init__(self, source):
        self._source = source
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
        self._source = None
        self._current = None

    def keys(self):
        """ Return the field names for the records in the stream.
        """
        return self._source.keys()

    def stats(self):
        """ Return the query statistics.
        """
        s = dict.fromkeys(update_stats_keys, 0)
        s.update(self._source.stats())
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
        fetch = self._source.fetch
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
        dictionaries. This method generates a self-contained set of
        result data using only Python-native data types.

        ::

            >>> from py2neo import Graph
            >>> graph = Graph(password="excalibur")
            >>> graph.run("MATCH (a:Person) RETURN a.name, a.born LIMIT 4").data()
            [{'a.born': 1964, 'a.name': 'Keanu Reeves'},
             {'a.born': 1967, 'a.name': 'Carrie-Anne Moss'},
             {'a.born': 1961, 'a.name': 'Laurence Fishburne'},
             {'a.born': 1960, 'a.name': 'Hugo Weaving'}]

        The extracted data can then be easily passed into an external data handler such as a
        `pandas.DataFrame <http://pandas.pydata.org/pandas-docs/stable/dsintro.html#dataframe>`_
        for subsequent processing::

            >>> from pandas import DataFrame
            >>> DataFrame(graph.run("MATCH (a:Person) RETURN a.name, a.born LIMIT 4").data())
               a.born              a.name
            0    1964        Keanu Reeves
            1    1967    Carrie-Anne Moss
            2    1961  Laurence Fishburne
            3    1960        Hugo Weaving

        Similarly, to output the result data as a JSON-formatted string::

            >>> import json
            >>> json.dumps(graph.run("UNWIND range(1, 3) AS n RETURN n").data())
            '[{"n": 1}, {"n": 2}, {"n": 3}]'

        :return: the full query result
        :rtype: `list` of `dict`
        """
        return [record.data() for record in self]

    def dump(self, out=stdout):
        """ Consume all records from this cursor and write in tabular
        form to the console.

        :param out: the channel to which output should be dumped
        """
        records = list(self)
        keys = self.keys()
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
            out.write(u"".join(templates[i].format(ustr(value)) for i, value in enumerate(record)))
            out.write(u"\n")


class Record(tuple, Mapping):
    """ A :class:`.Record` holds a collection of result values that are
    both indexed by position and keyed by name. A `Record` instance can
    therefore be seen as a combination of a `tuple` and a `Mapping`.
    """

    def __new__(cls, keys, values):
        if len(keys) == len(values):
            return super(Record, cls).__new__(cls, values)
        else:
            raise ValueError("Keys and values must be of equal length")

    def __init__(self, keys, values):
        self.__keys = tuple(keys)
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

    def items(self):
        return list(zip(self.__keys, self))

    def data(self):
        return dict(self)

    def subgraph(self):
        nodes = []
        relationships = []
        for value in self:
            if hasattr(value, "nodes"):
                nodes.extend(value.nodes())
            if hasattr(value, "relationships"):
                relationships.extend(value.relationships())
        if nodes:
            return Subgraph(nodes, relationships)
        else:
            return None
