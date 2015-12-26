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


from io import StringIO
from itertools import chain
from warnings import warn
import webbrowser

from py2neo.compat import integer, string, ustr, xstr, ReprIO
from py2neo.env import NEO4J_AUTH, NEO4J_URI
from py2neo.http import authenticate, Resource
from py2neo.packages.httpstream.packages.urimagic import URI
from py2neo.data import PropertyContainer, coerce_property
from py2neo.util import is_collection, round_robin, version_tuple, \
    ThreadLocalWeakValueDictionary, deprecated, relationship_case


class DBMS(object):
    """ Wrapper for the base REST resource exposed by a running Neo4j
    server, corresponding to the ``/`` URI. If no URI is supplied to
    the constructor, a value is taken from the ``NEO4J_URI`` environment
    variable (if set) otherwise a default of ``http://localhost:7474/``
    is used.
    """

    __instances = {}

    __authentication = None
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
            inst.__resource = Resource(uri)
            inst.__graph = None
            cls.__instances[uri] = inst
        return inst

    def __repr__(self):
        return "<DBMS uri=%r>" % self.uri.string

    def __eq__(self, other):
        try:
            return self.uri == other.uri
        except AttributeError:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash(self.uri)

    @property
    def graph(self):
        """ The graph exposed by this service.

        :rtype: :class:`.Graph`
        """
        if self.__graph is None:
            # The graph URI used to be determined via
            # discovery but another HTTP call sometimes
            # caused problems in the middle of other
            # operations (such as hydration) when using
            # concurrent code. Therefore, the URI is now
            # constructed manually.
            self.__graph = Graph(self.uri.string + "db/data/")
        return self.__graph

    @property
    def resource(self):
        """ The contained resource object for this instance.

        :rtype: :class:`py2neo.Resource`
        """
        return self.__resource

    @property
    def uri(self):
        """ The full URI of the contained resource.
        """
        return self.resource.uri


class Graph(object):
    """ The `Graph` class provides a wrapper around the
    `REST API <http://docs.neo4j.org/chunked/stable/rest-api.html>`_ exposed
    by a running Neo4j database server and is identified by the base URI of
    the graph database. If no URI is specified, a default value is taken from
    the ``NEO4J_URI`` environment variable. If this is not set, a default of
    `http://localhost:7474/db/data/` is assumed. Therefore, the simplest way
    to connect to a running service is to use::

        >>> from py2neo import Graph
        >>> graph = Graph()

    An explicitly specified graph database URI can also be passed to the
    constructor as a string::

        >>> other_graph = Graph("http://camelot:1138/db/data/")

    If the database server requires authorisation, the credentials can also
    be specified within the URI::

        >>> secure_graph = Graph("http://arthur:excalibur@camelot:1138/db/data/")

    Once obtained, the `Graph` instance provides direct or indirect access
    to most of the functionality available within py2neo.

    """

    __instances = {}

    __cypher = None
    __schema = None
    __node_labels = None
    __relationship_types = None

    def __new__(cls, uri=None):
        if uri is None:
            uri = DBMS().graph.uri.string
        if not uri.endswith("/"):
            uri += "/"
        key = (cls, uri)
        try:
            inst = cls.__instances[key]
        except KeyError:
            inst = super(Graph, cls).__new__(cls)
            inst.resource = Resource(uri)
            cls.__instances[key] = inst
        return inst

    def __repr__(self):
        return "<Graph uri=%r>" % self.uri.string

    def __hash__(self):
        return hash(self.uri)

    def __len__(self):
        return self.size()

    def __bool__(self):
        return True

    def __nonzero__(self):
        return True

    def __contains__(self, entity):
        return entity.resource and entity.resource.uri.string.startswith(self.resource.uri.string)

    @property
    def cypher(self):
        """ The Cypher execution resource for this graph providing access to
        all Cypher functionality for the underlying database, both simple
        and transactional.

        ::

            >>> from py2neo import Graph
            >>> graph = Graph()
            >>> graph.cypher.run("CREATE (a:Person {name:{N}})", {"N": "Alice"})

        :rtype: :class:`py2neo.cypher.CypherEngine`

        """
        if self.__cypher is None:
            from py2neo.cypher import CypherEngine
            metadata = self.resource.metadata
            self.__cypher = CypherEngine(metadata.get("transaction"))
        return self.__cypher

    def create(self, g):
        """ Create one or more remote nodes, relationships or paths in a
        single transaction. The entity values provided must be either
        existing entity objects (such as nodes or relationships) or values
        that can be cast to them.

        For example, to create a remote node from a local :class:`Node` object::

            from py2neo import Graph, Node
            graph = Graph()
            alice = Node("Person", name="Alice")
            graph.create(alice)

        Then, create a second node and a relationship connecting both nodes::

            german, speaks = graph.create({"name": "German"}, (alice, "SPEAKS", 0))

        This second example shows how :class:`dict` and :class:`tuple` objects
        can also be used to create nodes and relationships respectively. The
        zero value in the relationship tuple references the zeroth item created
        within that transaction, i.e. the "German" node.

        .. note::
            If an object is passed to this method that is already bound to
            a remote entity, that argument will be ignored and nothing will
            be created.

        :arg entities: One or more existing graph entities or values that
                       can be cast to entities.
        :return: A tuple of all entities created (or ignored) of the same
                 length and order as the arguments passed in.

        """
        # TODO update examples in docstring
        self.cypher.create(g)

    def create_unique(self, t):
        """ Create one or more unique paths or relationships in a single
        transaction. This is similar to :meth:`create` but uses a Cypher
        `CREATE UNIQUE <http://docs.neo4j.org/chunked/stable/query-create-unique.html>`_
        clause to ensure that only relationships that do not already exist are created.
        """
        # TODO update examples in docstring
        self.cypher.create_unique(t)

    @property
    def dbms(self):
        return self.resource.dbms

    def delete(self, g):
        """ Delete one or more nodes, relationships and/or paths.
        """
        self.cypher.delete(g)

    def delete_all(self):
        """ Delete all nodes and relationships from the graph.

        .. warning::
            This method will permanently remove **all** nodes and relationships
            from the graph and cannot be undone.
        """
        self.cypher.run("MATCH (a) OPTIONAL MATCH (a)-[r]->() DELETE r, a")

    def detach(self, g):
        """ Delete one or more relationships.
        """
        self.cypher.detach(g)

    def exists(self, g):
        """ Determine whether a number of graph entities all exist within the database.
        """
        tx = self.cypher.begin()
        cursors = []
        try:
            nodes = g.nodes()
            relationships = g.relationships()
        except AttributeError:
            raise TypeError("Object %r is not graphy" % g)
        else:
            for a in nodes:
                if a.resource:
                    cursors.append(tx.run("MATCH (a) WHERE id(a)={x} "
                                          "RETURN count(a)", x=a))
                else:
                    return False
            for r in relationships:
                if r.resource:
                    cursors.append(tx.run("MATCH ()-[r]->() WHERE id(r)={x} "
                                          "RETURN count(r)", x=r))
                else:
                    return False
        count = len(tx.statements)
        tx.commit()
        if count == 0:
            return None
        else:
            return sum(cursor.evaluate() for cursor in cursors) == count

    def find(self, label, property_key=None, property_value=None, limit=None):
        """ Iterate through a set of labelled nodes, optionally filtering
        by property key and value
        """
        if not label:
            raise ValueError("Empty label")
        from py2neo.cypher import cypher_escape
        if property_key is None:
            statement = "MATCH (n:%s) RETURN n,labels(n)" % cypher_escape(label)
            parameters = {}
        else:
            statement = "MATCH (n:%s {%s:{V}}) RETURN n,labels(n)" % (
                cypher_escape(label), cypher_escape(property_key))
            parameters = {"V": property_value}
        if limit:
            statement += " LIMIT %s" % limit
        cursor = self.cypher.run(statement, parameters)
        while cursor.move():
            a = cursor[0]
            a.labels().update(cursor[1])
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
                    relationships = self.cypher.evaluate(
                        "MATCH ()-[r]->() WHERE id(r) IN {x} RETURN collect(r)",
                        x=[int(uri.rpartition("/")[-1]) for uri in data["relationships"]])
                    node_uris = data["nodes"]
                    for i, relationship in enumerate(relationships):
                        if relationship.start_node().resource.uri == node_uris[i]:
                            directions.append("->")
                        else:
                            directions.append("<-")
                    data["directions"] = directions
                return Path.hydrate(data)
            elif "results" in data:
                return self.hydrate(data["results"][0])
            elif "columns" in data and "data" in data:
                from py2neo.cypher import Cursor
                result = Cursor(self, hydrate=True)
                result._process(data)
                return result
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
            if not start_node.resource:
                raise TypeError("Nodes for relationship match end points must be bound")
            parameters = {"A": start_node}
        elif start_node is None:
            statement = "MATCH (b) WHERE id(b)={B}"
            end_node = cast_node(end_node)
            if not end_node.resource:
                raise TypeError("Nodes for relationship match end points must be bound")
            parameters = {"B": end_node}
        else:
            statement = "MATCH (a) WHERE id(a)={A} MATCH (b) WHERE id(b)={B}"
            start_node = cast_node(start_node)
            end_node = cast_node(end_node)
            if not start_node.resource or not end_node.resource:
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
        cursor = self.cypher.run(statement, parameters)
        while cursor.move():
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

    def merge(self, label, property_key=None, property_value=None, limit=None):
        """ Match or create a node by label and optional property and return
        all matching nodes.
        """
        if not label:
            raise ValueError("Empty label")
        from py2neo.cypher import cypher_escape
        if property_key is None:
            statement = "MERGE (n:%s) RETURN n,labels(n)" % cypher_escape(label)
            parameters = {}
        elif not isinstance(property_key, string):
            raise TypeError("Property key must be textual")
        elif property_value is None:
            raise ValueError("Both key and value must be specified for a property")
        else:
            statement = "MERGE (n:%s {%s:{V}}) RETURN n,labels(n)" % (
                cypher_escape(label), cypher_escape(property_key))
            parameters = {"V": coerce_property(property_value)}
        if limit:
            statement += " LIMIT %s" % limit
        cursor = self.cypher.post(statement, parameters)
        for record in cursor.collect():
            dehydrated = record[0]
            dehydrated.setdefault("metadata", {})["labels"] = record[1]
            yield self.hydrate(dehydrated)

    def merge_one(self, label, property_key=None, property_value=None):
        """ Match or create a node by label and optional property and return a
        single matching node. This method is intended to be used with a unique
        constraint and does not fail if more than one matching node is found.

            >>> graph = Graph()
            >>> person = graph.merge_one("Person", "email", "bob@example.com")

        """
        for node in self.merge(label, property_key, property_value, limit=1):
            return node

    @property
    def neo4j_version(self):
        """ The database software version as a 4-tuple of (``int``, ``int``,
        ``int``, ``str``).
        """
        return version_tuple(self.resource.metadata["neo4j_version"])

    def node(self, id_):
        """ Fetch a node by ID. This method creates an object representing the
        remote node with the ID specified but fetches no data from the server.
        For this reason, there is no guarantee that the entity returned
        actually exists.
        """
        resource = self.resource.resolve("node/%s" % id_)
        uri_string = resource.uri.string
        try:
            return Node.cache[uri_string]
        except KeyError:
            node = self.cypher.evaluate("MATCH (a) WHERE id(a)={x} "
                                        "RETURN a", x=id_)
            if node is None:
                raise IndexError("Node %d not found" % id_)
            else:
                return node

    @property
    def node_labels(self):
        """ The set of node labels currently defined within the graph.
        """
        if self.__node_labels is None:
            self.__node_labels = Resource(self.uri.string + "labels")
        return frozenset(self.__node_labels.get().content)

    def open_browser(self):
        """ Open a page in the default system web browser pointing at
        the Neo4j browser application for this graph.
        """
        webbrowser.open(self.dbms.resource.uri.string)

    def order(self):
        """ The number of nodes in this graph.
        """
        statement = "MATCH (n) RETURN count(n)"
        return self.cypher.evaluate(statement)

    def pull(self, *entities):
        """ Pull data to one or more entities from their remote counterparts.
        """
        if not entities:
            return
        nodes = {}
        relationships = set()
        for entity in entities:
            for node in entity.nodes():
                nodes[node] = None
            relationships.update(entity.relationships())
        tx = self.cypher.begin()
        for node in nodes:
            cursor = tx.run("MATCH (a) WHERE id(a)={x} "
                            "RETURN a, labels(a)", x=node)
            cursor.cache["a"] = node
            nodes[node] = cursor
        for relationship in relationships:
            cursor = tx.run("MATCH ()-[r]->() WHERE id(r)={x} "
                            "RETURN r", x=relationship)
            cursor.cache["r"] = relationship
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
                if not node.resource:
                    continue
                batch.append({"id": i, "method": "PUT",
                              "to": "%s/properties" % node.resource.ref,
                              "body": dict(node)})
                i += 1
                batch.append({"id": i, "method": "PUT",
                              "to": "%s/labels" % node.resource.ref,
                              "body": list(node.labels())})
                i += 1
            for relationship in entity.relationships():
                if not relationship.resource:
                    continue
                batch.append({"id": i, "method": "PUT",
                              "to": "%s/properties" % relationship.resource.ref,
                              "body": dict(relationship)})
                i += 1
        self.resource.resolve("batch").post(batch)

    def relationship(self, id_):
        """ Fetch a relationship by ID.
        """
        resource = self.resource.resolve("relationship/" + str(id_))
        uri_string = resource.uri.string
        try:
            return Relationship.cache[uri_string]
        except KeyError:
            relationship = self.cypher.evaluate("MATCH ()-[r]->() WHERE id(r)={x} "
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
            self.__relationship_types = Resource(self.uri.string + "relationship/types")
        return frozenset(self.__relationship_types.get().content)

    @property
    def schema(self):
        """ The schema resource for this graph.

        :rtype: :class:`SchemaResource <py2neo.schema.SchemaResource>`
        """
        if self.__schema is None:
            from py2neo.schema import SchemaResource
            self.__schema = SchemaResource(self.uri.string + "schema")
        return self.__schema

    def size(self):
        """ The number of relationships in this graph.
        """
        statement = "MATCH ()-[r]->() RETURN count(r)"
        return self.cypher.evaluate(statement)

    def supports_auth(self):
        """ Returns :py:`True` if auth is supported by this version of Neo4j.
        """
        return self.neo4j_version >= (2, 2)

    @property
    def uri(self):
        return self.resource.uri


class EntityResource(Resource):

    def __init__(self, uri, metadata=None):
        Resource.__init__(self, uri, metadata)
        self.ref = self.uri.string[len(self.graph.uri.string):]
        self._id = int(self.ref.rpartition("/")[2])


class Entity(object):
    """ Base class for objects that can be optionally bound to a remote resource. This
    class is essentially a container for a :class:`.Resource` instance.
    """

    _resource = None
    _resource_pending_tx = None

    def _set_resource_pending(self, tx):
        self._resource_pending_tx = tx

    def _set_resource(self, uri, metadata=None):
        self._resource = EntityResource(uri, metadata)
        self._resource_pending_tx = None

    def _del_resource(self):
        self._resource = None
        self._resource_pending_tx = None

    @property
    def resource(self):
        """ Remote resource with which this entity is associated.
        """
        if self._resource_pending_tx:
            self._resource_pending_tx.process()
            self._resource_pending_tx = None
        return self._resource


class Subgraph(object):
    """ Arbitrary, unordered collection of nodes and relationships.
    """

    def __init__(self, nodes=None, relationships=None):
        self._nodes = frozenset(nodes or frozenset())
        self._relationships = frozenset(relationships or frozenset())
        self._nodes |= frozenset(chain(*(r.nodes() for r in self._relationships)))

    def __repr__(self):
        from py2neo.cypher import CypherWriter
        r = ReprIO()
        writer = CypherWriter(r)
        writer.write_subgraph(self)
        return r.getvalue()

    def __eq__(self, other):
        try:
            return self.nodes() == other.nodes() and self.relationships() == other.relationships()
        except AttributeError:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        value = 0
        for entity in self._nodes:
            value ^= hash(entity)
        for entity in self._relationships:
            value ^= hash(entity)
        return value

    def __len__(self):
        return self.size()

    def __iter__(self):
        return iter(self._relationships)

    def __bool__(self):
        return bool(self._relationships)

    def __nonzero__(self):
        return bool(self._relationships)

    def __or__(self, other):
        nodes = Subgraph.nodes
        relationships = Subgraph.relationships
        return Subgraph(nodes(self) | nodes(other), relationships(self) | relationships(other))

    def __and__(self, other):
        nodes = Subgraph.nodes
        relationships = Subgraph.relationships
        return Subgraph(nodes(self) & nodes(other), relationships(self) & relationships(other))

    def __sub__(self, other):
        nodes = Subgraph.nodes
        relationships = Subgraph.relationships
        r = relationships(self) - relationships(other)
        n = (nodes(self) - nodes(other)) | set().union(*(nodes(rel) for rel in r))
        return Subgraph(n, r)

    def __xor__(self, other):
        nodes = Subgraph.nodes
        relationships = Subgraph.relationships
        r = relationships(self) ^ relationships(other)
        n = (nodes(self) ^ nodes(other)) | set().union(*(nodes(rel) for rel in r))
        return Subgraph(n, r)

    def nodes(self):
        """ Set of all nodes in this subgraph.
        """
        return self._nodes

    def relationships(self):
        """ Set of all relationships in this subgraph.
        """
        return self._relationships

    def order(self):
        """ Total number of unique nodes in this set.
        """
        return len(self._nodes)

    def size(self):
        """ Total number of unique relationships in this set.
        """
        return len(self._relationships)

    def labels(self):
        """ Set of all node labels used in this subgraph.
        """
        return frozenset(chain(*(node.labels() for node in self._nodes)))

    def types(self):
        """ Set of all relationship types used in this subgraph.
        """
        return frozenset(rel.type() for rel in self._relationships)

    def keys(self):
        """ Set of all property keys used in this subgraph.
        """
        return (frozenset(chain(*(node.keys() for node in self._nodes))) |
                frozenset(chain(*(rel.keys() for rel in self._relationships))))


class TraversableSubgraph(Subgraph):
    """ A graph with traversal information.
    """

    def __init__(self, head, *tail):
        sequence = (head,) + tail
        self._node_sequence = sequence[0::2]
        self._relationship_sequence = sequence[1::2]
        Subgraph.__init__(self, self._node_sequence, self._relationship_sequence)
        self._sequence = sequence

    def __repr__(self):
        from py2neo.cypher import CypherWriter
        r = ReprIO()
        writer = CypherWriter(r)
        writer.write_traversable_subgraph(self)
        return r.getvalue()

    def __eq__(self, other):
        try:
            other_traversal = tuple(other.traverse())
        except AttributeError:
            return False
        else:
            return tuple(self.traverse()) == other_traversal

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        value = 0
        for item in self._sequence:
            value ^= hash(item)
        return value

    def __len__(self):
        return (len(self._sequence) - 1) // 2

    def __getitem__(self, index):
        if isinstance(index, slice):
            start, stop = index.start, index.stop
            if start is not None:
                if start < 0:
                    start += len(self)
                start *= 2
            if stop is not None:
                if stop < 0:
                    stop += len(self)
                stop = 2 * stop + 1
            return TraversableSubgraph(*self._sequence[start:stop])
        elif index < 0:
            return self._sequence[2 * index]
        else:
            return self._sequence[2 * index + 1]

    def __iter__(self):
        for relationship in self._relationship_sequence:
            yield relationship

    def __add__(self, other):
        if other is None:
            return self
        return TraversableSubgraph(*traverse(self, other))

    def start_node(self):
        """ The first node in a traversal of this subgraph.
        """
        return self._node_sequence[0]

    def end_node(self):
        """ The last node in a traversal of this subgraph.
        """
        return self._node_sequence[-1]

    def length(self):
        """ The total number of relationships traversed.
        """
        return len(self._relationship_sequence)

    def traverse(self):
        """ Traverse all nodes and relationships in order.
        """
        return iter(self._sequence)

    def nodes(self):
        """ Set of all nodes in this subgraph.
        """
        return self._node_sequence

    def relationships(self):
        """ Set of all relationships in this subgraph.
        """
        return self._relationship_sequence


class Node(PropertyContainer, TraversableSubgraph, Entity):
    """ A node is a fundamental unit of data storage within a property
    graph that may optionally be connected, via relationships, to
    other nodes.

    All positional arguments passed to the constructor are interpreted
    as labels and all keyword arguments as properties::

        >>> from py2neo import Node
        >>> a = Node("Person", name="Alice")

    """

    cache = ThreadLocalWeakValueDictionary()

    @classmethod
    def hydrate(cls, data, inst=None):
        self = data["self"]
        if inst is None:
            new_inst = cls()
            new_inst.__stale.update({"labels", "properties"})
            inst = cls.cache.setdefault(self, new_inst)
            # The check below is a workaround for http://bugs.python.org/issue19542
            # See also: https://github.com/nigelsmall/py2neo/issues/391
            if inst is None:
                inst = cls.cache[self] = new_inst
        cls.cache[self] = inst
        inst._set_resource(self, data)
        if "data" in data:
            inst.__stale.discard("properties")
            inst.clear()
            inst.update(data["data"])
        if "metadata" in data:
            inst.__stale.discard("labels")
            metadata = data["metadata"]
            labels = inst.labels()
            labels.clear()
            labels.update(metadata["labels"])
        return inst

    def __init__(self, *labels, **properties):
        self._labels = set(labels)
        PropertyContainer.__init__(self, **properties)
        TraversableSubgraph.__init__(self, self)
        self.__stale = set()

    def __repr__(self):
        from py2neo.cypher import CypherWriter
        r = ReprIO()
        writer = CypherWriter(r)
        writer.write_node(self)
        return r.getvalue()

    def __eq__(self, other):
        if other is None:
            return False
        other = cast_node(other)
        if self.resource and other.resource:
            return self.resource == other.resource
        else:
            return self is other

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        if self.resource:
            return hash(self.resource.uri)
        else:
            return hash(id(self))

    def __getitem__(self, item):
        if self.resource and "properties" in self.__stale:
            self.resource.graph.pull(self)
        return PropertyContainer.__getitem__(self, item)

    def degree(self):
        """ The number of relationships attached to this node.
        """
        remote = self.resource
        if remote is None:
            raise TypeError("Cannot determine degree of node not "
                            "bound to a remote resource")
        return remote.graph.cypher.evaluate("MATCH (a)-[r]-() WHERE id(a)={n} "
                                            "RETURN count(r)", n=self)

    @deprecated("Node.exists() is deprecated, use graph.exists(node) instead")
    def exists(self):
        return self.resource.graph.exists(self)

    def labels(self):
        """ The set of labels attached to this node.
        """
        if self.resource and "labels" in self.__stale:
            self.resource.graph.pull(self)
        return self._labels

    @deprecated("Node.match() is deprecated, use graph.match(node, ...) instead")
    def match(self, rel_type=None, other_node=None, limit=None):
        return self.resource.graph.match(self, rel_type, other_node, True, limit)

    @deprecated("Node.match_incoming() is deprecated, use graph.match(node, ...) instead")
    def match_incoming(self, rel_type=None, start_node=None, limit=None):
        return self.resource.graph.match(start_node, rel_type, self, False, limit)

    @deprecated("Node.match_outgoing() is deprecated, use graph.match(node, ...) instead")
    def match_outgoing(self, rel_type=None, end_node=None, limit=None):
        return self.resource.graph.match(self, rel_type, end_node, False, limit)

    @property
    @deprecated("Node.properties is deprecated, use dict(node) instead")
    def properties(self):
        if self.resource and "properties" in self.__stale:
            self.resource.graph.pull(self)
        return dict(self)

    @deprecated("Node.pull() is deprecated, use graph.pull(node) instead")
    def pull(self):
        self.resource.graph.pull(self)

    @deprecated("Node.push() is deprecated, use graph.push(node) instead")
    def push(self):
        self.resource.graph.push(self)

    def _del_resource(self):
        try:
            del self.cache[self.resource.uri]
        except KeyError:
            pass
        Entity._del_resource(self)


class NodeProxy(object):
    """ Base class for objects that can be used in place of a node.
    """
    pass


class Relationship(PropertyContainer, TraversableSubgraph, Entity):
    """ A graph relationship that may optionally be bound to a remote counterpart
    in a Neo4j database. Relationships require a triple of start node, relationship
    type and end node and may also optionally be given one or more properties::

        >>> from py2neo import Node, Relationship
        >>> alice = Node("Person", name="Alice")
        >>> bob = Node("Person", name="Bob")
        >>> alice_knows_bob = Relationship(alice, "KNOWS", bob, since=1999)



        >>> a = PropertyNode(name="Alice")
        >>> b = PropertyNode(name="Bob")

        >>> Relationship(a)
        ({name:'Alice'})-[:TO]->({name:'Alice'})
        >>> Relationship(a, b)
        ({name:'Alice'})-[:TO]->({name:'Bob'})
        >>> Relationship(a, "KNOWS", b)
        ({name:'Alice'})-[:KNOWS]->({name:'Bob'})

        >>> class WorksWith(Relationship): pass
        >>> WorksWith(a, b)
        ({name:'Alice'})-[:WORKS_WITH]->({name:'Bob'})

    :param nodes:
    :param properties:
    :return:
    """

    cache = ThreadLocalWeakValueDictionary()

    @classmethod
    def default_type(cls):
        if cls is Relationship:
            return None
        elif issubclass(cls, Relationship):
            return ustr(relationship_case(cls.__name__))
        else:
            raise TypeError("Class %s is not a relationship subclass" % cls.__name__)

    @classmethod
    def hydrate(cls, data, inst=None):
        self = data["self"]
        start = data["start"]
        end = data["end"]
        if inst is None:
            new_inst = cls(Node.hydrate({"self": start}),
                           data.get("type"),
                           Node.hydrate({"self": end}),
                           **data.get("data", {}))
            inst = cls.cache.setdefault(self, new_inst)
            # The check below is a workaround for http://bugs.python.org/issue19542
            # See also: https://github.com/nigelsmall/py2neo/issues/391
            if inst is None:
                inst = cls.cache[self] = new_inst
        else:
            Node.hydrate({"self": start}, inst.start_node())
            Node.hydrate({"self": end}, inst.end_node())
            inst._type = data.get("type")
            if "data" in data:
                inst.clear()
                inst.update(data["data"])
            else:
                inst.__stale.add("properties")
        cls.cache[self] = inst
        inst._set_resource(self, data)
        return inst

    def __init__(self, *nodes, **properties):
        n = []
        p = {}
        for value in nodes:
            if isinstance(value, string):
                n.append(value)
            elif isinstance(value, tuple) and len(value) == 2 and isinstance(value[0], string):
                t, props = value
                n.append(t)
                p.update(props)
            else:
                n.append(cast_node(value))
        p.update(properties)

        num_args = len(n)
        if num_args == 0:
            raise TypeError("Relationships must specify at least one endpoint")
        elif num_args == 1:
            # Relationship(a)
            self._type = self.default_type()
            n = (n[0], n[0])
        elif num_args == 2:
            if n[1] is None or isinstance(n[1], string):
                # Relationship(a, "TO")
                self._type = n[1]
                n = (n[0], n[0])
            else:
                # Relationship(a, b)
                self._type = self.default_type()
                n = (n[0], n[1])
        elif num_args == 3:
            # Relationship(a, "TO", b)
            self._type = n[1]
            n = (n[0], n[2])
        else:
            raise TypeError("Hyperedges not supported")
        PropertyContainer.__init__(self, **p)
        TraversableSubgraph.__init__(self, n[0], self, n[1])

        self.__stale = set()

    def __repr__(self):
        from py2neo.cypher import CypherWriter
        r = ReprIO()
        writer = CypherWriter(r)
        writer.write_relationship(self)
        return r.getvalue()

    def __eq__(self, other):
        if other is None:
            return False
        try:
            other = cast_relationship(other)
        except TypeError:
            return False
        else:
            if self.resource and other.resource:
                return self.resource == other.resource
            try:
                return (self.nodes() == other.nodes() and other.size() == 1 and
                        self.type() == other.type() and dict(self) == dict(other))
            except AttributeError:
                return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        if self.resource:
            return hash(self.resource.uri)
        else:
            return hash(self.nodes()) ^ hash(self.type())

    @deprecated("Relationship.exists() is deprecated, "
                "use graph.exists(relationship) instead")
    def exists(self):
        return self.resource.graph.exists(self)

    @property
    @deprecated("Relationship.properties is deprecated, use dict(relationship) instead")
    def properties(self):
        if self.resource and "properties" in self.__stale:
            self.resource.graph.pull(self)
        return dict(self)

    @deprecated("Relationship.pull() is deprecated, use graph.pull(relationship) instead")
    def pull(self):
        self.resource.graph.pull(self)

    @deprecated("Relationship.push() is deprecated, use graph.push(relationship) instead")
    def push(self):
        self.resource.graph.push(self)

    def type(self):
        """ The type of this relationship.
        """
        if self.resource and self._type is None:
            self.resource.graph.pull(self)
        return self._type

    def _del_resource(self):
        """ Detach this relationship and its start and end
        nodes from any remote counterparts.
        """
        try:
            del self.cache[self.resource.uri]
        except KeyError:
            pass
        Entity._del_resource(self)


class Path(TraversableSubgraph):
    """ A sequence of nodes connected by relationships that may
    optionally be bound to remote counterparts in a Neo4j database.

        >>> from py2neo import Node, Path
        >>> alice, bob, carol = Node(name="Alice"), Node(name="Bob"), Node(name="Carol")
        >>> abc = Path(alice, "KNOWS", bob, Relationship(carol, "KNOWS", bob), carol)
        >>> abc
        <Path order=3 size=2>
        >>> abc.nodes
        (<Node labels=set() properties={'name': 'Alice'}>,
         <Node labels=set() properties={'name': 'Bob'}>,
         <Node labels=set() properties={'name': 'Carol'}>)
        >>> abc.relationships
        (<Relationship type='KNOWS' properties={}>,
         <Relationship type='KNOWS' properties={}>)
        >>> dave, eve = Node(name="Dave"), Node(name="Eve")
        >>> de = Path(dave, "KNOWS", eve)
        >>> de
        <Path order=2 size=1>
        >>> abcde = Path(abc, "KNOWS", de)
        >>> abcde
        <Path order=5 size=4>
        >>> for relationship in abcde.relationships():
        ...     print(relationship)
        ({name:"Alice"})-[:KNOWS]->({name:"Bob"})
        ({name:"Carol"})-[:KNOWS]->({name:"Bob"})
        ({name:"Carol"})-[:KNOWS]->({name:"Dave"})
        ({name:"Dave"})-[:KNOWS]->({name:"Eve"})

    """
    @classmethod
    def hydrate(cls, data):
        node_uris = data["nodes"]
        relationship_uris = data["relationships"]
        offsets = [(0, 1) if direction == "->" else (1, 0) for direction in data["directions"]]
        nodes = [Node.hydrate({"self": uri}) for uri in node_uris]
        relationships = [Relationship.hydrate({"self": uri,
                                               "start": node_uris[i + offsets[i][0]],
                                               "end": node_uris[i + offsets[i][1]]})
                         for i, uri in enumerate(relationship_uris)]
        inst = Path(*round_robin(nodes, relationships))
        inst.__metadata = data
        return inst

    def __init__(self, *entities):
        entities = list(entities)
        for i, entity in enumerate(entities):
            if entity is None:
                entities[i] = Node()
            elif isinstance(entity, dict):
                entities[i] = Node(**entity)
        for i, entity in enumerate(entities):
            try:
                start_node = entities[i - 1].end_node()
                end_node = entities[i + 1].start_node()
            except (IndexError, AttributeError):
                pass
            else:
                if isinstance(entity, string):
                    entities[i] = Relationship(start_node, entity, end_node)
                elif isinstance(entity, tuple) and len(entity) == 2:
                    t, properties = entity
                    entities[i] = Relationship(start_node, t, end_node, **properties)
        TraversableSubgraph.__init__(self, *tuple(traverse(*entities)))

    def __str__(self):
        return xstr(self.__unicode__())

    def __unicode__(self):
        from py2neo.cypher import CypherWriter
        s = StringIO()
        writer = CypherWriter(s)
        writer.write_traversable_subgraph(self)
        return s.getvalue()


def traverse(*traversables):
    if not traversables:
        return
    traversable = traversables[0]
    try:
        entities = traversable.traverse()
    except AttributeError:
        raise TypeError("Object %r is not traversable" % traversable)
    for entity in entities:
        yield entity
    end_node = traversable.end_node()
    for traversable in traversables[1:]:
        try:
            if end_node == traversable.start_node():
                entities = traversable.traverse()
                end_node = traversable.end_node()
            elif end_node == traversable.end_node():
                entities = reversed(list(traversable.traverse()))
                end_node = traversable.start_node()
            else:
                raise ValueError("Cannot append traversable %r "
                                 "to node %r" % (traversable, end_node))
        except AttributeError:
            raise TypeError("Object %r is not traversable" % traversable)
        for i, entity in enumerate(entities):
            if i > 0:
                yield entity


def cast(obj, entities=None):
    """ Cast a general Python object to a graph-specific entity,
    such as a :class:`.Node` or a :class:`.Relationship`.
    """
    if obj is None:
        return None
    elif isinstance(obj, (Node, NodeProxy, Relationship, Path)):
        return obj
    elif isinstance(obj, dict):
        return cast_node(obj)
    elif isinstance(obj, tuple):
        return cast_relationship(obj, entities)
    else:
        raise TypeError(obj)


def cast_node(obj):
    if obj is None or isinstance(obj, (Node, NodeProxy)):
        return obj

    def apply(x):
        if isinstance(x, dict):
            inst.update(x)
        elif is_collection(x):
            for item in x:
                apply(item)
        elif isinstance(x, string):
            inst.labels().add(ustr(x))
        else:
            raise TypeError("Cannot cast %s to Node" % obj.__class__.__name__)

    inst = Node()
    apply(obj)
    return inst


def cast_relationship(obj, entities=None):

    def get_type(r):
        if isinstance(r, string):
            return r
        elif hasattr(r, "type"):
            return r.type()
        elif isinstance(r, tuple) and len(r) == 2 and isinstance(r[0], string):
            return r[0]
        else:
            raise ValueError("Cannot determine relationship type from %r" % r)

    def get_properties(r):
        if isinstance(r, string):
            return {}
        elif hasattr(r, "type") and callable(r.type):
            return dict(r)
        elif hasattr(r, "properties"):
            return r.properties
        elif isinstance(r, tuple) and len(r) == 2 and isinstance(r[0], string):
            return dict(r[1])
        else:
            raise ValueError("Cannot determine properties from %r" % r)

    if isinstance(obj, Relationship):
        return obj
    elif isinstance(obj, tuple):
        if len(obj) == 3:
            start_node, t, end_node = obj
            properties = get_properties(t)
        elif len(obj) == 4:
            start_node, t, end_node, properties = obj
            properties = dict(get_properties(t), **properties)
        else:
            raise TypeError("Cannot cast relationship from %r" % obj)
    else:
        raise TypeError("Cannot cast relationship from %r" % obj)

    if entities:
        if isinstance(start_node, integer):
            start_node = entities[start_node]
        if isinstance(end_node, integer):
            end_node = entities[end_node]
    return Relationship(start_node, get_type(t), end_node, **properties)
