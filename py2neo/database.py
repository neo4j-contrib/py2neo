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


from collections import deque, OrderedDict
from email.utils import parsedate_tz, mktime_tz
import logging
from sys import stdout
from warnings import warn
import webbrowser

from py2neo import PRODUCT
from py2neo.compat import integer, ustr, string
from py2neo.types import Node, Relationship, Path, cast_node, Subgraph, \
    cypher_escape, walk, size, Walkable, cypher_repr
from py2neo.env import NEO4J_AUTH, NEO4J_URI
from py2neo.http import authenticate, Resource, ResourceTemplate
from py2neo.packages.httpstream import Response as HTTPResponse
from py2neo.packages.httpstream.numbers import NOT_FOUND
from py2neo.packages.httpstream.packages.urimagic import URI
from py2neo.packages.neo4j.v1 import GraphDatabase
from py2neo.packages.neo4j.v1.connection import Response, RUN, PULL_ALL
from py2neo.packages.neo4j.v1.typesystem import \
    Node as BoltNode, Relationship as BoltRelationship, Path as BoltPath, hydrated as bolt_hydrate
from py2neo.status import CypherError, Finished, GraphError
from py2neo.util import deprecated, is_collection, version_tuple


log = logging.getLogger("py2neo.cypher")


def presubstitute(statement, parameters):
    more = True
    presub_parameters = []
    while more:
        before, opener, key = statement.partition(u"{%")
        if opener:
            key, closer, after = key.partition(u"%}")
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


def normalise_request(statement, parameters, **kwparameters):
    from py2neo.types import Entity

    s = ustr(statement)
    p = {}

    def add_parameters(params):
        if params:
            for k, v in dict(params).items():
                if isinstance(v, tuple):
                    v = list(v)
                elif isinstance(v, Entity):
                    if v.remote:
                        v = v.remote._id
                    else:
                        raise TypeError("Cannot pass an unbound entity as a parameter")
                p[k] = v

    add_parameters(dict(parameters or {}, **kwparameters))
    return presubstitute(s, p)


def cypher_request(statement, parameters, **kwparameters):
    s, p = normalise_request(statement, parameters, **kwparameters)

    # OrderedDict is used here to avoid statement/parameters ordering bug
    return OrderedDict([
        ("statement", s),
        ("parameters", p),
        ("resultDataContents", ["REST"]),
    ])


class DBMS(object):
    """ Accessor for the entire database management system belonging to
    a Neo4j server installation. This corresponds to the ``/`` URI in
    the HTTP API.

    An explicit URI can be passed to the constructor::

        >>> from py2neo import DBMS
        >>> my_dbms = DBMS("http://myserver:7474/")

    Alternatively, the default value of ``http://localhost:7474/`` is
    used::

        >>> default_dbms = DBMS()
        >>> default_dbms
        <DBMS uri='http://localhost:7474/'>

    """

    __instances = {}
    __graph = None

    def __new__(cls, uri=None):
        if uri is None:
            uri = NEO4J_URI
        uri = ustr(uri)
        if not uri.endswith("/"):
            uri += "/"
        try:
            inst = cls.__instances[uri]
        except KeyError:
            if NEO4J_AUTH:
                user_name, password = NEO4J_AUTH.partition(":")[0::2]
                authenticate(URI(uri).host_port, user_name, password)
            inst = super(DBMS, cls).__new__(cls)
            inst.remote = Resource(uri)
            inst.__graph = None
            cls.__instances[uri] = inst
        return inst

    def __repr__(self):
        return "<DBMS uri=%r>" % self.remote.uri.string

    def __eq__(self, other):
        try:
            return self.remote.uri == other.remote.uri
        except AttributeError:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash(self.remote.uri)

    @property
    def graph(self):
        """ The graph database exposed by this database management system.

        :rtype: :class:`.Graph`
        """
        if self.__graph is None:
            # The graph URI used to be determined via
            # discovery but another HTTP call sometimes
            # caused problems in the middle of other
            # operations (such as hydration) when using
            # concurrent code. Therefore, the URI is now
            # constructed manually.
            self.__graph = Graph(self.remote.uri.string + "db/data/")
        return self.__graph

    def raw_system_info(self):
        """ Obtain a dictionary of system information.
        """
        return Resource(self.remote.uri.string + "db/manage/server/jmx/domain/org.neo4j").get().content

    def _bean_dict(self, name):
        info = self.raw_system_info()
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

    def kernel_start_time(self):
        """ Return the time from which this Neo4j instance was in operational mode.
        """
        info = self._bean_dict("Kernel")
        return mktime_tz(parsedate_tz(info["KernelStartTime"]))

    def kernel_version(self):
        """ Return the version of Neo4j.
        """
        info = self._bean_dict("Kernel")
        return version_tuple(info["KernelVersion"].partition("version:")[-1].partition(",")[0].strip())

    def store_creation_time(self):
        """ Return the time when this Neo4j graph store was created.
        """
        info = self._bean_dict("Kernel")
        return mktime_tz(parsedate_tz(info["StoreCreationDate"]))

    def store_directory(self):
        """ Return the location where the Neo4j store is located.
        """
        info = self._bean_dict("Kernel")
        return info["StoreDirectory"]

    def store_id(self):
        """ Return an identifier that, together with store creation time,
        uniquely identifies this Neo4j graph store.
        """
        info = self._bean_dict("Kernel")
        return info["StoreId"]

    def primitive_counts(self):
        """ Return a dictionary of estimates of the numbers of different
        kinds of Neo4j primitives.
        """
        return self._bean_dict("Primitive count")

    def store_file_sizes(self):
        """ Return a dictionary of information about the sizes of the
        different parts of the Neo4j graph store.
        """
        return self._bean_dict("Store file sizes")

    def config(self):
        """ Return a dictionary of the configuration parameters used to
        configure Neo4j.
        """
        return self._bean_dict("Configuration")

    def supports_auth(self):
        """ Returns :py:const:`True` if auth is supported by this
        version of Neo4j, :py:const:`False` otherwise.
        """
        return self.kernel_version() >= (2, 2)

    def supports_bolt(self):
        """ Returns :py:const:`True` if Bolt is supported by this
        version of Neo4j, :py:const:`False` otherwise.
        """
        return self.kernel_version() >= (3,)


class Graph(object):
    """ The `Graph` class represents a Neo4j graph database. To construct,
    one or more URIs can be supplied. These can be ``http`` or ``bolt`` URIs::

        >>> from py2neo import Graph
        >>> graph = Graph("http://myserver:7474/db/data/", "bolt://myserver:7687")

    If no URIs are specified, a default value is taken from the ``NEO4J_URI``
    environment variable. If this is not set, a default of
    ``http://localhost:7474/db/data/`` is assumed. Therefore, the simplest way
    to connect to a running service is to use::

        >>> graph = Graph()

    Even if no ``bolt`` URI is specified, one will be derived based on the ``http``
    URI, assuming the server supports the Bolt protocol. Where Bolt is available,
    it will automatically be used for all Cypher queries.

    If the database server requires authorisation, the credentials can also
    be specified within the URI::

        >>> secure_graph = Graph("http://arthur:excalibur@camelot:1138/db/data/")

    Once obtained, the `Graph` instance provides direct or indirect access
    to most of the functionality available within py2neo.
    """

    __instances = {}

    __schema = None
    __node_labels = None
    __relationship_types = None

    remote = None
    driver = None

    def __new__(cls, *uris):
        # Gather all URIs
        uri_dict = {"http": [], "bolt": []}
        for uri in uris:
            uri = URI(uri)
            uri_dict.setdefault(uri.scheme, []).append(uri.string)
        # Ensure there is at least one HTTP URI available
        if not uri_dict["http"]:
            uri_dict["http"].append(DBMS().graph.remote.uri.string)
        http_uri = uri_dict["http"][0]
        # Add a trailing slash if required
        if not http_uri.endswith("/"):
            http_uri += "/"
        # Construct a new instance
        key = (cls, http_uri)
        try:
            inst = cls.__instances[key]
        except KeyError:
            inst = super(Graph, cls).__new__(cls)
            inst.uris = uri_dict
            inst.remote = Resource(http_uri)
            inst.transaction_uri = Resource(http_uri + "transaction").uri.string
            if inst.dbms.supports_bolt():
                if not uri_dict["bolt"]:
                    uri_dict["bolt"].append("bolt://%s" % inst.remote.uri.host)
                bolt_uri = URI(uri_dict["bolt"][0])
                inst.driver = GraphDatabase.driver(bolt_uri.string, user_agent="/".join(PRODUCT))
            cls.__instances[key] = inst
        return inst

    def __repr__(self):
        return "<Graph uri=%r>" % self.remote.uri.string

    def __hash__(self):
        return hash(self.remote.uri)

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
        return entity.remote and entity.remote.uri.string.startswith(self.remote.uri.string)

    def begin(self, autocommit=False):
        """ Begin a new transaction.

        :arg autocommit:
        """
        if self.driver:
            return BoltTransaction(self, autocommit)
        else:
            return HTTPTransaction(self, autocommit)

    def create(self, subgraph):
        """ Create remote nodes and relationships that correspond to those in a
        local subgraph.

        :arg subgraph: a :class:`.Node`, :class:`.Relationship` or other
                       :class:`.Subgraph`

        .. seealso::
            :meth:`Transaction.create`
        """
        self.begin(autocommit=True).create(subgraph)

    def create_unique(self, walkable):
        """ Create unique remote nodes and relationships that correspond to those
        in a local walkable object.

        :arg walkable: a :class:`.Node`, :class:`.Relationship` or other
                       :class:`.Walkable` object

        .. seealso::
            :meth:`Transaction.create_unique`
        """
        self.begin(autocommit=True).create_unique(walkable)

    @property
    def dbms(self):
        """ The database management system to which this graph belongs.
        """
        return self.remote.dbms

    def degree(self, subgraph):
        """ Return the total number of relationships attached to all nodes in
        a subgraph.

        :arg subgraph: a :class:`.Node`, :class:`.Relationship` or other
                       :class:`.Subgraph` object

        .. seealso::
            :meth:`Transaction.degree`
        """
        return self.begin(autocommit=True).degree(subgraph)

    def delete(self, subgraph):
        """ Delete the remote nodes and relationships that correspond to
        those in a local subgraph.

        :arg subgraph: a :class:`.Node`, :class:`.Relationship` or other
                       :class:`.Subgraph` object

        .. seealso::
            :meth:`Transaction.delete`
        """
        self.begin(autocommit=True).delete(subgraph)

    def delete_all(self):
        """ Delete all nodes and relationships from the graph.

        .. warning::
            This method will permanently remove **all** nodes and relationships
            from the graph and cannot be undone.
        """
        self.run("MATCH (a) OPTIONAL MATCH (a)-[r]->() DELETE r, a")

    def evaluate(self, statement, parameters=None, **kwparameters):
        """ Execute a single Cypher statement and return the value from
        the first column of the first record.

        :arg statement: Cypher statement
        :arg parameters: dictionary of parameters
        :return: single return value or :const:`None`.

        .. seealso::
            :meth:`Transaction.evaluate`
        """
        return self.begin(autocommit=True).evaluate(statement, parameters, **kwparameters)

    def exists(self, subgraph):
        """ Determine whether one or more graph entities all exist within the
        database.

        :arg subgraph: a :class:`.Node`, :class:`.Relationship` or other
                       :class:`.Subgraph` object

        .. seealso::
            :meth:`Transaction.exists`
        """
        return self.begin(autocommit=True).exists(subgraph)

    def find(self, label, property_key=None, property_value=None, limit=None):
        """ Iterate through a set of labelled nodes, optionally filtering
        by property key and value
        """
        if not label:
            raise ValueError("Empty label")
        if property_key is None:
            statement = "MATCH (n:%s) RETURN n,labels(n)" % cypher_escape(label)
            parameters = {}
        else:
            statement = "MATCH (n:%s {%s:{V}}) RETURN n,labels(n)" % (
                cypher_escape(label), cypher_escape(property_key))
            parameters = {"V": property_value}
        if limit:
            statement += " LIMIT %s" % limit
        cursor = self.run(statement, parameters)
        while cursor.forward():
            a = cursor[0]
            a.update_labels(cursor[1])
            yield a
        cursor.close()

    def find_one(self, label, property_key=None, property_value=None):
        """ Find a single node by label and optional property. This method is
        intended to be used with a unique constraint and does not fail if more
        than one matching node is found.
        """
        for node in self.find(label, property_key, property_value, limit=1):
            return node

    def hydrate(self, data, inst=None):
        """ Hydrate a dictionary of data to produce a :class:`.Node`,
        :class:`.Relationship` or other graph object instance. The
        data structure and values expected are those produced by the
        `REST API <http://neo4j.com/docs/stable/rest-api.html>`__.

        :arg data: dictionary of data to hydrate

        """
        if isinstance(data, dict):
            if "errors" in data and data["errors"]:
                from py2neo.status import CypherError
                for error in data["errors"]:
                    raise CypherError.hydrate(error)
            elif "self" in data:
                if "type" in data:
                    return Relationship.hydrate(data, inst)
                else:
                    return Node.hydrate(data, inst)
            elif "nodes" in data and "relationships" in data:
                if "directions" not in data:
                    directions = []
                    relationships = self.evaluate(
                        "MATCH ()-[r]->() WHERE id(r) IN {x} RETURN collect(r)",
                        x=[int(uri.rpartition("/")[-1]) for uri in data["relationships"]])
                    node_uris = data["nodes"]
                    for i, relationship in enumerate(relationships):
                        if relationship.start_node().remote.uri == node_uris[i]:
                            directions.append("->")
                        else:
                            directions.append("<-")
                    data["directions"] = directions
                return Path.hydrate(data)
            elif "results" in data:
                return self.hydrate(data["results"][0])
            elif "columns" in data and "data" in data:
                cursor = Cursor(self, hydrate=True)
                HTTPTransaction.fill_cursor(cursor, data)
                return cursor
            elif "neo4j_version" in data:
                return self
            else:
                warn("Map literals returned over the Neo4j REST interface are ambiguous "
                     "and may be hydrated as graph objects")
                return data
        elif is_collection(data):
            return type(data)(map(self.hydrate, data))
        else:
            return data

    def match(self, start_node=None, rel_type=None, end_node=None, bidirectional=False, limit=None):
        """ Return an iterator for all relationships matching the
        specified criteria.

        For example, to find all of Alice's friends::

            for rel in graph.match(start_node=alice, rel_type="FRIEND"):
                print(rel.end_node.properties["name"])

        :arg start_node: :attr:`~py2neo.Node.identity()` start :class:`~py2neo.Node` to match or
                           :const:`None` if any
        :arg rel_type: type of relationships to match or :const:`None` if any
        :arg end_node: :attr:`~py2neo.Node.identity()` end :class:`~py2neo.Node` to match or
                         :const:`None` if any
        :arg bidirectional: :const:`True` if reversed relationships should also be included
        :arg limit: maximum number of relationships to match or :const:`None` if no limit
        :return: matching relationships
        :rtype: generator
        """
        if start_node is None and end_node is None:
            statement = "MATCH (a)"
            parameters = {}
        elif end_node is None:
            statement = "MATCH (a) WHERE id(a)={A}"
            start_node = cast_node(start_node)
            if not start_node.remote:
                raise TypeError("Nodes for relationship match end points must be bound")
            parameters = {"A": start_node}
        elif start_node is None:
            statement = "MATCH (b) WHERE id(b)={B}"
            end_node = cast_node(end_node)
            if not end_node.remote:
                raise TypeError("Nodes for relationship match end points must be bound")
            parameters = {"B": end_node}
        else:
            statement = "MATCH (a) WHERE id(a)={A} MATCH (b) WHERE id(b)={B}"
            start_node = cast_node(start_node)
            end_node = cast_node(end_node)
            if not start_node.remote or not end_node.remote:
                raise TypeError("Nodes for relationship match end points must be bound")
            parameters = {"A": start_node, "B": end_node}
        if rel_type is None:
            rel_clause = ""
        elif is_collection(rel_type):
            rel_clause = ":" + "|:".join("`{0}`".format(_) for _ in rel_type)
        else:
            rel_clause = ":`{0}`".format(rel_type)
        if bidirectional:
            statement += " MATCH (a)-[r" + rel_clause + "]-(b) RETURN r"
        else:
            statement += " MATCH (a)-[r" + rel_clause + "]->(b) RETURN r"
        if limit is not None:
            statement += " LIMIT {0}".format(int(limit))
        cursor = self.run(statement, parameters)
        while cursor.forward():
            yield cursor["r"]

    def match_one(self, start_node=None, rel_type=None, end_node=None, bidirectional=False):
        """ Return a single relationship matching the
        specified criteria. See :meth:`~py2neo.Graph.match` for
        argument details.
        """
        rels = list(self.match(start_node, rel_type, end_node,
                               bidirectional, 1))
        if rels:
            return rels[0]
        else:
            return None

    def merge(self, walkable, label=None, *property_keys):
        """ Merge remote nodes and relationships that correspond to those in
        a local walkable. Optionally perform the merge based on a specific
        label or set of property keys.

        :arg walkable: a :class:`.Node`, :class:`.Relationship` or other
                       :class:`.Walkable` object
        :arg label: label on which to match any existing nodes
        :arg property_keys: property keys on which to match any existing nodes

        .. seealso::
            :meth:`Transaction.merge`
        """
        self.begin(autocommit=True).merge(walkable, label, *property_keys)

    @property
    @deprecated("Graph.neo4j_version is deprecated, use DBMS.kernel_version() instead")
    def neo4j_version(self):
        return version_tuple(self.remote.metadata["neo4j_version"])

    def node(self, id_):
        """ Fetch a node by ID. This method creates an object representing the
        remote node with the ID specified but fetches no data from the server.
        For this reason, there is no guarantee that the entity returned
        actually exists.
        """
        resource = self.remote.resolve("node/%s" % id_)
        uri_string = resource.uri.string
        try:
            return Node.cache[uri_string]
        except KeyError:
            node = self.evaluate("MATCH (a) WHERE id(a)={x} RETURN a", x=id_)
            if node is None:
                raise IndexError("Node %d not found" % id_)
            else:
                return node

    @property
    def node_labels(self):
        """ The set of node labels currently defined within the graph.
        """
        if self.__node_labels is None:
            self.__node_labels = Resource(self.remote.uri.string + "labels")
        return frozenset(self.__node_labels.get().content)

    def open_browser(self):
        """ Open a page in the default system web browser pointing at
        the Neo4j browser application for this graph.
        """
        webbrowser.open(self.dbms.remote.uri.string)

    def pull(self, *entities):
        """ Pull data to one or more entities from their remote counterparts.
        """
        if not entities:
            return
        nodes = {}
        relationships = []
        for entity in entities:
            for node in entity.nodes():
                nodes[node] = None
            relationships.extend(entity.relationships())
        tx = self.begin()
        for node in nodes:
            tx.entities.append({"a": node})
            cursor = tx.run("MATCH (a) WHERE id(a)={x} RETURN a, labels(a)", x=node)
            nodes[node] = cursor
        for relationship in relationships:
            tx.entities.append({"r": relationship})
            tx.run("MATCH ()-[r]->() WHERE id(r)={x} RETURN r", x=relationship)
        tx.commit()
        for node, cursor in nodes.items():
            labels = node._labels
            labels.clear()
            labels.update(cursor.evaluate(1))

    def push(self, *entities):
        """ Push data from one or more entities to their remote counterparts.
        """
        batch = []
        i = 0
        for entity in entities:
            for node in entity.nodes():
                if not node.remote:
                    continue
                batch.append({"id": i, "method": "PUT",
                              "to": "%s/properties" % node.remote.ref,
                              "body": dict(node)})
                i += 1
                batch.append({"id": i, "method": "PUT",
                              "to": "%s/labels" % node.remote.ref,
                              "body": list(node.labels())})
                i += 1
            for relationship in entity.relationships():
                if not relationship.remote:
                    continue
                batch.append({"id": i, "method": "PUT",
                              "to": "%s/properties" % relationship.remote.ref,
                              "body": dict(relationship)})
                i += 1
        self.remote.resolve("batch").post(batch)

    def relationship(self, id_):
        """ Fetch a relationship by ID.
        """
        resource = self.remote.resolve("relationship/" + str(id_))
        uri_string = resource.uri.string
        try:
            return Relationship.cache[uri_string]
        except KeyError:
            relationship = self.evaluate("MATCH ()-[r]->() WHERE id(r)={x} "
                                                "RETURN r", x=id_)
            if relationship is None:
                raise IndexError("Relationship %d not found" % id_)
            else:
                return relationship

    @property
    def relationship_types(self):
        """ The set of relationship types currently defined within the graph.
        """
        if self.__relationship_types is None:
            self.__relationship_types = Resource(self.remote.uri.string + "relationship/types")
        return frozenset(self.__relationship_types.get().content)

    def run(self, statement, parameters=None, **kwparameters):
        """ Execute a single Cypher statement.

        :arg statement: Cypher statement
        :arg parameters: dictionary of parameters

        .. seealso::
            :meth:`Transaction.run`
        """
        return self.begin(autocommit=True).run(statement, parameters, **kwparameters)

    def separate(self, subgraph):
        """ Delete the remote relationships that correspond to those
        in a local subgraph.

        :arg subgraph: a :class:`.Node`, :class:`.Relationship` or other
                       :class:`.Subgraph`

        .. seealso::
            :meth:`Transaction.separate`
        """
        self.begin(autocommit=True).separate(subgraph)

    @property
    def schema(self):
        """ The schema resource for this graph.

        :rtype: :class:`Schema`
        """
        if self.__schema is None:
            self.__schema = Schema(self.remote.uri.string + "schema")
        return self.__schema


class Schema(object):
    """ The schema resource attached to a `Graph` instance.
    """

    def __init__(self, uri):
        self._index_template = ResourceTemplate(uri + "/index/{label}")
        self._index_key_template = ResourceTemplate(uri + "/index/{label}/{property_key}")
        self._uniqueness_constraint_template = \
            ResourceTemplate(uri + "/constraint/{label}/uniqueness")
        self._uniqueness_constraint_key_template = \
            ResourceTemplate(uri + "/constraint/{label}/uniqueness/{property_key}")

    def create_index(self, label, property_key):
        """ Create a schema index for a label and property
        key combination.
        """
        self._index_template.expand(label=label).post({"property_keys": [property_key]})

    def create_uniqueness_constraint(self, label, property_key):
        """ Create a uniqueness constraint for a label.
        """
        self._uniqueness_constraint_template.expand(label=label).post(
            {"property_keys": [property_key]})

    def drop_index(self, label, property_key):
        """ Remove label index for a given property key.
        """
        try:
            self._index_key_template.expand(label=label, property_key=property_key).delete()
        except GraphError as error:
            cause = error.__cause__
            if isinstance(cause, HTTPResponse):
                if cause.status_code == NOT_FOUND:
                    raise GraphError("No such schema index (label=%r, key=%r)" % (
                        label, property_key))
            raise

    def drop_uniqueness_constraint(self, label, property_key):
        """ Remove the uniqueness constraint for a given property key.
        """
        try:
            self._uniqueness_constraint_key_template.expand(
                label=label, property_key=property_key).delete()
        except GraphError as error:
            cause = error.__cause__
            if isinstance(cause, HTTPResponse):
                if cause.status_code == NOT_FOUND:
                    raise GraphError("No such unique constraint (label=%r, key=%r)" % (
                        label, property_key))
            raise

    def get_indexes(self, label):
        """ Fetch a list of indexed property keys for a label.
        """
        return [
            indexed["property_keys"][0]
            for indexed in self._index_template.expand(label=label).get().content
        ]

    def get_uniqueness_constraints(self, label):
        """ Fetch a list of unique constraints for a label.
        """

        return [
            unique["property_keys"][0]
            for unique in self._uniqueness_constraint_template.expand(label=label).get().content
        ]


class Transaction(object):
    """ A transaction is a transient resource that allows multiple Cypher
    statements to be executed within a single server transaction.
    """

    def __init__(self, graph, autocommit=False):
        log.info("begin")
        self.graph = graph
        self.autocommit = autocommit
        self._finished = False
        self.entities = deque()

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

    def finished(self):
        """ Indicates whether or not this transaction has been completed
        or is still open.
        """
        return self._finished

    def run(self, statement, parameters=None, **kwparameters):
        """ Add a statement to the current queue of statements to be
        executed.

        :arg statement: Cypher statement
        :arg parameters: dictionary of parameters
        """
        raise NotImplementedError("%s.run" % self.__class__.__name__)

    @deprecated("Transaction.append(...) is deprecated, use Transaction.run(...) instead")
    def append(self, statement, parameters=None, **kwparameters):
        return self.run(statement, parameters, **kwparameters)

    def post(self, commit=False, hydrate=False):
        """ Submit the outstanding queue of actions to the current transaction.

        :arg commit: flag indicating whether or not to commit this transaction
        :arg hydrate: flag indicating whether or not to hydrate the values returned
        """
        raise NotImplementedError("%s.post" % self.__class__.__name__)

    def process(self):
        """ Send all pending statements to the server for execution, leaving
        the transaction open for further statements.
        """
        self.post(hydrate=True)

    def finish(self):
        self._assert_unfinished()
        self._finished = True

    def commit(self):
        """ Send all pending statements to the server for execution and commit
        the transaction.
        """
        self.post(commit=True, hydrate=True)

    def rollback(self):
        """ Rollback the current transaction, undoing all actions taken so far.
        """
        raise NotImplementedError("%s.rollback" % self.__class__.__name__)

    def evaluate(self, statement, parameters=None, **kwparameters):
        """ Execute a single Cypher statement and return the value from
        the first column of the first record.

        :arg statement: Cypher statement
        :arg parameters: dictionary of parameters
        :return: single return value or :const:`None`
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

        :arg subgraph: a :class:`.Node`, :class:`.Relationship` or other
                       :class:`.Subgraph`
        """
        try:
            nodes = list(subgraph.nodes())
            relationships = list(subgraph.relationships())
        except AttributeError:
            raise TypeError("Object %r is not graphy" % subgraph)
        reads = []
        writes = []
        parameters = {}
        returns = {}
        for i, node in enumerate(nodes):
            node_id = "a%d" % i
            param_id = "x%d" % i
            if node.remote:
                reads.append("MATCH (%s) WHERE id(%s)={%s}" % (node_id, node_id, param_id))
                parameters[param_id] = node.remote._id
            else:
                label_string = "".join(":" + cypher_escape(label)
                                       for label in sorted(node.labels()))
                writes.append("CREATE (%s%s {%s})" % (node_id, label_string, param_id))
                parameters[param_id] = dict(node)
                node._set_remote_pending(self)
            returns[node_id] = node
        for i, relationship in enumerate(relationships):
            if not relationship.remote:
                rel_id = "r%d" % i
                start_node_id = "a%d" % nodes.index(relationship.start_node())
                end_node_id = "a%d" % nodes.index(relationship.end_node())
                type_string = cypher_escape(relationship.type())
                param_id = "y%d" % i
                writes.append("CREATE UNIQUE (%s)-[%s:%s]->(%s) SET %s={%s}" %
                              (start_node_id, rel_id, type_string, end_node_id, rel_id, param_id))
                parameters[param_id] = dict(relationship)
                returns[rel_id] = relationship
                relationship._set_remote_pending(self)
        statement = "\n".join(reads + writes + ["RETURN %s LIMIT 1" % ", ".join(returns)])
        self.entities.append(returns)
        self.run(statement, parameters)

    def create_unique(self, walkable):
        """ Create unique remote nodes and relationships that correspond to those
        in a local walkable object. This method is similar to :meth:`create` but
        uses a Cypher `CREATE UNIQUE <http://docs.neo4j.org/chunked/stable/query-create-unique.html>`_
        clause to ensure that only relationships that do not already exist are created.

        :arg walkable: a :class:`.Node`, :class:`.Relationship` or other
                       :class:`.Walkable` object
        """
        if not isinstance(walkable, Walkable):
            raise ValueError("Object %r is not walkable" % walkable)
        if not any(node.remote for node in walkable.nodes()):
            raise ValueError("At least one node must be bound")
        matches = []
        pattern = []
        writes = []
        parameters = {}
        returns = {}
        node = None
        for i, entity in enumerate(walk(walkable)):
            if i % 2 == 0:
                # node
                node_id = "a%d" % i
                param_id = "x%d" % i
                if entity.remote:
                    matches.append("MATCH (%s) "
                                   "WHERE id(%s)={%s}" % (node_id, node_id, param_id))
                    pattern.append("(%s)" % node_id)
                    parameters[param_id] = entity.remote._id
                else:
                    label_string = "".join(":" + cypher_escape(label)
                                           for label in sorted(entity.labels()))
                    pattern.append("(%s%s {%s})" % (node_id, label_string, param_id))
                    parameters[param_id] = dict(entity)
                    entity._set_remote_pending(self)
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
                if not entity.remote:
                    entity._set_remote_pending(self)
                returns[rel_id] = entity
        statement = "\n".join(matches + ["CREATE UNIQUE %s" % "".join(pattern)] + writes +
                              ["RETURN %s LIMIT 1" % ", ".join(returns)])
        self.entities.append(returns)
        self.run(statement, parameters)

    def degree(self, subgraph):
        """ Return the total number of relationships attached to all nodes in
        a subgraph.

        :arg subgraph: a :class:`.Node`, :class:`.Relationship` or other
                       :class:`.Subgraph`
        :return: the total number of distinct relationships
        """
        try:
            nodes = list(subgraph.nodes())
        except AttributeError:
            raise TypeError("Object %r is not graphy" % subgraph)
        node_ids = []
        for i, node in enumerate(nodes):
            remote = node.remote
            if remote:
                node_ids.append(remote._id)
        statement = "OPTIONAL MATCH (a)-[r]-() WHERE id(a) IN {x} RETURN count(DISTINCT r)"
        parameters = {"x": node_ids}
        return self.evaluate(statement, parameters)

    def delete(self, subgraph):
        """ Delete the remote nodes and relationships that correspond to
        those in a local subgraph.

        :arg subgraph: a :class:`.Node`, :class:`.Relationship` or other
                       :class:`.Subgraph`
        """
        try:
            nodes = list(subgraph.nodes())
            relationships = list(subgraph.relationships())
        except AttributeError:
            raise TypeError("Object %r is not graphy" % subgraph)
        matches = []
        deletes = []
        parameters = {}
        for i, relationship in enumerate(relationships):
            if relationship.remote:
                rel_id = "r%d" % i
                param_id = "y%d" % i
                matches.append("MATCH ()-[%s]->() "
                               "WHERE id(%s)={%s}" % (rel_id, rel_id, param_id))
                deletes.append("DELETE %s" % rel_id)
                parameters[param_id] = relationship.remote._id
                relationship._del_remote()
        for i, node in enumerate(nodes):
            if node.remote:
                node_id = "a%d" % i
                param_id = "x%d" % i
                matches.append("MATCH (%s) "
                               "WHERE id(%s)={%s}" % (node_id, node_id, param_id))
                deletes.append("DELETE %s" % node_id)
                parameters[param_id] = node.remote._id
                node._del_remote()
        statement = "\n".join(matches + deletes)
        self.run(statement, parameters)

    def exists(self, subgraph):
        """ Determine whether one or more graph entities all exist within the
        database. Note that if any nodes or relationships in *subgraph* are not
        bound to remote counterparts, this method will return ``False``.

        :arg subgraph: a :class:`.Node`, :class:`.Relationship` or other
                       :class:`.Subgraph`
        :return: ``True`` if all entities exist remotely, ``False`` otherwise
        """
        try:
            nodes = list(subgraph.nodes())
            relationships = list(subgraph.relationships())
        except AttributeError:
            raise TypeError("Object %r is not graphy" % subgraph)
        node_ids = set()
        relationship_ids = set()
        for i, node in enumerate(nodes):
            remote = node.remote
            if remote:
                node_ids.add(remote._id)
            else:
                return False
        for i, relationship in enumerate(relationships):
            remote = relationship.remote
            if remote:
                relationship_ids.add(remote._id)
            else:
                return False
        statement = ("OPTIONAL MATCH (a) WHERE id(a) IN {x} "
                     "OPTIONAL MATCH ()-[r]->() WHERE id(r) IN {y} "
                     "RETURN count(DISTINCT a) + count(DISTINCT r)")
        parameters = {"x": list(node_ids), "y": list(relationship_ids)}
        return self.evaluate(statement, parameters) == len(node_ids) + len(relationship_ids)

    def merge(self, walkable, label=None, *property_keys):
        """ Merge remote nodes and relationships that correspond to those in
        a local walkable object. Optionally perform the merge based on a specific
        label or set of property keys.

        :arg walkable: a :class:`.Node`, :class:`.Relationship` or other
                       :class:`.Walkable` object
        :arg label: label on which to match any existing nodes
        :arg property_keys: property keys on which to match any existing nodes
        """
        if not isinstance(walkable, Walkable):
            raise ValueError("Object %r is not walkable" % walkable)
        if size(walkable) == 0 and walkable.start_node().remote:
            return  # single, bound node - nothing to do
        matches = []
        pattern = []
        writes = []
        parameters = {}
        returns = {}
        node = None
        for i, entity in enumerate(walk(walkable)):
            if i % 2 == 0:
                # node
                node_id = "a%d" % i
                param_id = "x%d" % i
                if entity.remote:
                    matches.append("MATCH (%s) "
                                   "WHERE id(%s)={%s}" % (node_id, node_id, param_id))
                    pattern.append("(%s)" % node_id)
                    parameters[param_id] = entity.remote._id
                else:
                    if label is None:
                        label_string = "".join(":" + cypher_escape(label)
                                               for label in sorted(entity.labels()))
                    elif entity.labels():
                        label_string = ":" + cypher_escape(label)
                        writes.append("SET %s%s" % (
                            node_id, "".join(":" + cypher_escape(label)
                                             for label in sorted(entity.labels()))))
                    else:
                        label_string = ""
                    if property_keys:
                        property_map_string = cypher_repr({k: v for k, v in dict(entity).items()
                                                           if k in property_keys})
                        writes.append("SET %s={%s}" % (node_id, param_id))
                        parameters[param_id] = dict(entity)
                    else:
                        property_map_string = cypher_repr(dict(entity))
                    pattern.append("(%s%s %s)" % (node_id, label_string, property_map_string))
                    entity._set_remote_pending(self)
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
                if not entity.remote:
                    entity._set_remote_pending(self)
                returns[rel_id] = entity
        statement = "\n".join(matches + ["MERGE %s" % "".join(pattern)] + writes +
                              ["RETURN %s LIMIT 1" % ", ".join(returns)])
        self.entities.append(returns)
        self.run(statement, parameters)

    def separate(self, subgraph):
        """ Delete the remote relationships that correspond to those in a local
        subgraph. This leaves any nodes in *subgraph* untouched.

        :arg subgraph: a :class:`.Node`, :class:`.Relationship` or other
                       :class:`.Subgraph`
        """
        try:
            relationships = list(subgraph.relationships())
        except AttributeError:
            raise TypeError("Object %r is not graphy" % subgraph)
        matches = []
        deletes = []
        parameters = {}
        for i, relationship in enumerate(relationships):
            if relationship.remote:
                rel_id = "r%d" % i
                param_id = "y%d" % i
                matches.append("MATCH ()-[%s]->() "
                               "WHERE id(%s)={%s}" % (rel_id, rel_id, param_id))
                deletes.append("DELETE %s" % rel_id)
                parameters[param_id] = relationship.remote._id
                relationship._del_remote()
        statement = "\n".join(matches + deletes)
        self.run(statement, parameters)


class HTTPTransaction(Transaction):
    """ A transaction is a transient resource that allows multiple Cypher
    statements to be executed within a single server transaction.
    """

    def __init__(self, graph, autocommit=False):
        Transaction.__init__(self, graph, autocommit)
        self.statements = []
        self.cursors = []
        uri = graph.transaction_uri
        self._begin = Resource(uri)
        self._begin_commit = Resource(uri + "/commit")
        self._execute = None
        self._commit = None

    def run(self, statement, parameters=None, **kwparameters):
        self._assert_unfinished()
        self.statements.append(cypher_request(statement, parameters, **kwparameters))
        cursor = Cursor(self.graph, self, hydrate=True)
        self.cursors.append(cursor)
        if self.autocommit:
            self.commit()
        return cursor

    def post(self, commit=False, hydrate=False):
        self._assert_unfinished()
        if commit:
            log.info("commit")
            resource = self._commit or self._begin_commit
            self.finish()
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
            raise CypherError.hydrate(raw_error)
        for raw_result in raw["results"]:
            cursor = self.cursors.pop(0)
            cursor.hydrate = hydrate
            self.fill_cursor(cursor, raw_result)

    def rollback(self):
        self._assert_unfinished()
        log.info("rollback")
        try:
            if self._execute:
                self._execute.delete()
        finally:
            self.finish()

    @staticmethod
    def fill_cursor(cursor, raw):
        try:
            entities = cursor.transaction.entities.popleft()
        except (AttributeError, IndexError):
            entities = {}
        cursor._keys = keys = tuple(raw["columns"])
        if cursor.hydrate:
            hydrate = cursor.graph.hydrate
            records = []
            for record in raw["data"]:
                values = []
                for i, value in enumerate(record["rest"]):
                    key = keys[i]
                    cached = entities.get(key)
                    values.append(hydrate(value, inst=cached))
                records.append(Record(keys, values))
            cursor._records = records
        else:
            cursor._records = [values["rest"] for values in raw["data"]]
        cursor.filled = True


class BoltTransaction(Transaction):

    def __init__(self, graph, autocommit=False):
        Transaction.__init__(self, graph, autocommit)
        self.driver = driver = self.graph.driver
        self.session = driver.session()
        self.cursors = []
        if not self.autocommit:
            self.run("BEGIN")

    def run(self, statement, parameters=None, **kwparameters):
        self._assert_unfinished()
        connection = self.session.connection
        cursor = Cursor(self.graph, self, hydrate=True)
        try:
            entities = self.entities.popleft()
        except IndexError:
            entities = {}

        def on_header(metadata):
            """ Called on receipt of the result header.
            """
            cursor._keys = metadata["fields"]

        def on_record(values):
            """ Called on receipt of each result record.
            """
            keys = cursor._keys
            hydrated_values = []
            for i, value in enumerate(values):
                key = keys[i]
                cached = entities.get(key)
                v = self.rehydrate(bolt_hydrate(value), inst=cached)
                hydrated_values.append(v)
            cursor._records.append(Record(keys, hydrated_values))

        def on_footer(metadata):
            """ Called on receipt of the result footer.
            """
            cursor.filled = True
            #cursor.summary = ResultSummary(self.statement, self.parameters, **metadata)

        def on_failure(metadata):
            """ Called on execution failure.
            """
            raise CypherError.hydrate(metadata)

        run_response = Response(connection)
        run_response.on_success = on_header
        run_response.on_failure = on_failure

        pull_all_response = Response(connection)
        pull_all_response.on_record = on_record
        pull_all_response.on_success = on_footer
        pull_all_response.on_failure = on_failure

        s, p = normalise_request(statement, parameters, **kwparameters)
        connection.append(RUN, (s, p), run_response)
        connection.append(PULL_ALL, (), pull_all_response)
        self.cursors.append(cursor)
        if self.autocommit:
            self.finish()
        return cursor

    def rehydrate(self, obj, inst=None):
        from py2neo.types import Node, Relationship, Path
        if isinstance(obj, BoltNode):
            return Node.hydrate({
                "self": "%snode/%d" % (self.graph.remote.uri.string, obj.identity),
                "metadata": {"labels": list(obj.labels)},
                "data": obj.properties,
            }, inst)
        elif isinstance(obj, BoltRelationship):
            graph_uri = self.graph.remote.uri.string
            return Relationship.hydrate({
                "self": "%srelationship/%d" % (graph_uri, obj.identity),
                "start": "%snode/%d" % (graph_uri, obj.start),
                "end": "%snode/%d" % (graph_uri, obj.end),
                "type": obj.type,
                "data": obj.properties,
            }, inst)
        elif isinstance(obj, BoltPath):
            graph_uri = self.graph.remote.uri.string
            return Path.hydrate({
                "nodes": ["%snode/%d" % (graph_uri, n.identity) for n in obj.nodes],
                "relationships": ["%srelationship/%d" % (graph_uri, r.identity)
                                  for r in obj.relationships],
                "directions": ["->" if r.start == obj.nodes[i].identity else "<-"
                               for i, r in enumerate(obj.relationships)],
            })
        elif isinstance(obj, list):
            return list(map(self.rehydrate, obj))
        elif isinstance(obj, dict):
            return {key: self.rehydrate(value) for key, value in obj.items()}
        else:
            return obj

    def _sync(self):
        connection = self.session.connection
        connection.send()
        fetch = connection.fetch
        while self.cursors:
            cursor = self.cursors.pop(0)
            while not cursor.filled:
                fetch()

    def post(self, commit=False, hydrate=False):
        self._assert_unfinished()
        if commit:
            log.info("commit")
            self.run("COMMIT")
            self.finish()
        else:
            log.info("process")
            self._sync()

    def finish(self):
        self._sync()
        Transaction.finish(self)
        self.session.close()
        self.session = None

    def rollback(self):
        self._assert_unfinished()
        log.info("rollback")
        try:
            self.run("ROLLBACK")
            self._sync()
        finally:
            self.finish()


class Cursor(object):
    """ A navigable reader for the stream of records made available from running
    a Cypher statement.
    """

    def __init__(self, graph, transaction=None, hydrate=False):
        assert transaction is None or isinstance(transaction, Transaction)
        self.graph = graph
        self.transaction = transaction
        self._keys = None
        self._records = []
        self._position = 0
        self.filled = False
        self.hydrate = hydrate

    def __repr__(self):
        return "<Cursor position=%r keys=%r>" % (self._position, self.keys())

    def __len__(self):
        record = self.current
        if record is None:
            raise TypeError("No current record")
        else:
            return len(record)

    def __getitem__(self, item):
        record = self.current
        if record is None:
            raise TypeError("No current record")
        else:
            return record[item]

    def __iter__(self):
        record = self.current
        if record is None:
            raise TypeError("No current record")
        else:
            return iter(record)

    def keys(self):
        """ Return the keys for the currently selected record.
        """
        return self._keys

    @property
    def position(self):
        """ Return the current cursor position. Position zero indicates
        that no record is currently selected, position one is that of
        the first record available, and so on.
        """
        return self._position

    def forward(self, amount=1):
        """ Attempt to move the cursor one position forward (or by
        another amount if explicitly specified). The cursor will move
        position by up to, but never more than, the amount specified.
        If not enough scope for movement remains, only that remainder
        will be consumed. The total amount moved is returned.

        :param amount: the amount by which to move the cursor
        :return: the amount that the cursor was able to move
        """
        if not self.filled:
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

    @property
    def current(self):
        """ Return the current record.

        :param keys:
        :return:
        """
        if self._position == 0:
            return None
        else:
            return self._records[self._position - 1]

    def next(self):
        """ Fetch and return the next record, if available.

        :param keys:
        :return:
        """
        if self.forward():
            return self.current
        else:
            return None

    def stream(self):
        """ Consume and yield all remaining records.

        :param keys:
        :return:
        """
        while self.forward():
            yield self.current

    def evaluate(self, key=0):
        """ Select the next available record and return the value from
        its first field (or another field if explicitly specified).

        :param key:
        :return:
        """
        if self.forward():
            return self.current[key]
        else:
            return None

    def close(self):
        """ Close this cursor and free up all associated resources.
        """
        self._records.clear()

    def dump(self, out=None):
        """ Consume all records from this cursor and write in tabular
        form to the console.

        :param out: the channel to which output should be dumped
        """
        if out is None:
            out = stdout
        records = list(self.stream())
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
