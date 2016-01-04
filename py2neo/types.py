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

"""
Graph Data Types
================

Before connecting to a Neo4j server, it's useful to become familiar with
the fundamental data types of the property graph model offered by py2neo.
While the types described here are completely compatible with Neo4j, they
can also be used independently of it.


Overview
--------

The two essential building blocks of the py2neo property graph model are
:class:`.Node` and :class:`.Relationship`. Along with the container types
:class:`.Subgraph` and :class:`.Walkable`, these provide a way to construct
and work with a wide variety of Neo4j-compatible graph data. The four types
can be summarised as follows:

- :class:`.Node` - fundamental unit of data storage within a graph
- :class:`.Relationship` - typed connection between a pair of nodes
- :class:`.Subgraph` - collection of nodes and relationships
- :class:`.Walkable` - subgraph with added traversal information

The example below shows how to create a couple of nodes and a relationship
joining them. Each node has a single property, `name`, and is labelled as a
`Person`. The relationship ``ab`` describes a connection from the first node
``a`` to the second node ``b`` of type `KNOWS`.

::

    >>> from py2neo import Node, Relationship
    >>> a = Node("Person", name="Alice")
    >>> b = Node("Person", name="Bob")
    >>> ab = Relationship(a, "KNOWS", b)

Relationship types can alternatively be determined by a class that extends
the :class:`.Relationship` class. The default type of such relationships is
derived from the class name::

    >>> c = Node("Person", name="Carol")
    >>> class WorksWith(Relationship): pass
    >>> ac = WorksWith(a, c)
    >>> ac.type()
    'WORKS_WITH'

Arbitrary collections of nodes and relationships may be contained in a
:class:`.Subgraph` object. The simplest way to construct these is by
combining nodes and relationships with standard set operations. For
example::

    >>> s = ab | ac
    >>> s
    {(xyz01:Person {name:"Alice"}),
     (xyz02:Person {name:"Bob"}),
     (xyz03:Person {name:"Carol"}),
     (xyz01)-[:KNOWS]->(xyz02),
     (xyz01)-[:WORKS_WITH]->(xyz03)}
    >>> s.nodes()
    frozenset({(xyz01:Person {name:"Alice"}),
               (xyz02:Person {name:"Bob"}),
               (xyz03:Person {name:"Carol"})})
    >>> s.relationships()
    frozenset({(xyz01)-[:KNOWS]->(xyz02),
               (xyz01)-[:WORKS_WITH]->(xyz03)})

A :class:`.Walkable` is a subgraph with added traversal information.
The simplest way to construct a :class:`.Walkable` is by concatenating
other graph objects::

    >>> w = ab + Relationship(b, "LIKES", c) + ac
    >>> w
    (xyz01)-[:KNOWS]->(xyz02)-[:LIKES]->(xyz03)<-[:WORKS_WITH]-(xyz01)


Graph Arithmetic
----------------

Graph objects can be combined in a number of ways using standard
Python operators. In this context, Node and Relationship objects
are treated as simple :class:`.Subgraph` instances. The full set
of operations are detailed below.

Union
~~~~~
**Syntax**: ``x | y``

The union of `x` and `y` is a :class:`.Subgraph` containing all
nodes and relationships from `x` as well as all nodes and relationships
from `y`. Any entities common to both operands will only be included
once.

For example::

    >>> a = Node()
    >>> b = Node()
    >>> c = Node()
    >>> ab = Relationship(a, "TO", b)
    >>> ac = Relationship(a, "TO", c)
    >>> s = ab | ac
    >>> s
    {(Z0N0a), (Z0PAe), (Z0PCS), (Z0PAe)-[:TO]->(Z0PCS), (Z0PAe)-[:TO]->(Z0N0a)}
    >>> s | Relationship(b, "TO", c)
    {(Z0N0a), (Z0PAe), (Z0PCS), (Z0N0a)-[:TO]->(Z0PCS), (Z0PAe)-[:TO]->(Z0PCS), (Z0PAe)-[:TO]->(Z0N0a)}


====================  ===========  ===========
Operation             Notation     Result
====================  ===========  ===========
union                 ``s1 | s2``  A :class:`.Subgraph` containing all nodes and relationships from `s1` and `s2` combined
intersection          ``s1 & s2``  A :class:`.Subgraph` containing all nodes and relationships common to both `s1` and `s2`
difference            ``s1 - s2``  A :class:`.Subgraph` containing all nodes and relationships from `s1` excluding those that are also in `s2` (nodes in `s2` attached to relationships in `s1` will remain)
symmetric difference  ``s1 ^ s2``  A :class:`.Subgraph` containing all nodes and relationships in either `s1` or `s2` but not both (nodes attached to relationships solely in `s1` or `s2` will remain)
concatenation         ``s1 + s2``  A :class:`.Walkable` containing a :func:`.walk` of `s1` followed by a :func:`.walk` of `s2`
====================  ===========  ===========


Equality Rules
--------------

Node equality is based on identity.
This means that a node is only equal to itself and is not equal to another node with the same properties and labels.

Relationship equality is based on type and endpoints.
A relationship will therefore be considered equal to another relationship of the same type attached to the same nodes.
Properties are not considered for relationship equality.

API
---
"""

from io import StringIO
from itertools import chain
import json
from sys import stdout

from py2neo.compat import integer, string, unicode, ustr, ReprIO
from py2neo.http import Resource
from py2neo.util import is_collection, round_robin, \
    ThreadLocalWeakValueDictionary, deprecated, relationship_case, snake_case, base62


# Maximum and minimum integers supported up to Java 7.
# Java 8 also supports unsigned long which can extend
# to (2 ** 64 - 1) but Neo4j is not yet on Java 8.
JAVA_INTEGER_MIN_VALUE = -2 ** 63
JAVA_INTEGER_MAX_VALUE = 2 ** 63 - 1

entity_name_property_key = "name"


def set_entity_name_property_key(key):
    global entity_name_property_key
    entity_name_property_key = key


def coerce_atomic_property(x):
    if isinstance(x, unicode):
        return x
    elif isinstance(x, string):
        return ustr(x)
    elif isinstance(x, bool):
        return x
    elif isinstance(x, integer):
        if JAVA_INTEGER_MIN_VALUE <= x <= JAVA_INTEGER_MAX_VALUE:
            return x
        else:
            raise ValueError("Integer value out of range: %s" % x)
    elif isinstance(x, float):
        return x
    else:
        raise TypeError("Properties of type %s are not supported" % x.__class__.__name__)


def coerce_property(x):
    if isinstance(x, (tuple, list, set, frozenset)):
        collection = []
        cls = None
        for item in x:
            item = coerce_atomic_property(item)
            t = type(item)
            if cls is None:
                cls = t
            elif t != cls:
                raise TypeError("List properties must be homogenous "
                                "(found %s in list of %s)" % (t.__name__, cls.__name__))
            collection.append(item)
        return x.__class__(collection)
    else:
        return coerce_atomic_property(x)


def entity_name(entity):
    if hasattr(entity, "__name__") and entity.__name__:
        name = entity.__name__
    else:
        resource = entity.resource
        if resource:
            cls = entity.__class__
            if cls is Node:
                prefix = "a"
            else:
                prefix = cls.__name__[0].lower()
            name = "%s%d" % (prefix, resource._id)
        else:
            name = entity[entity_name_property_key]
            if isinstance(name, string):
                name = snake_case(name)
            else:
                name = "_" + base62(abs(hash(entity)))
    return name


def walk(*walkables):
    """ Traverse over the arguments supplied, yielding the entities
    from each in turn.

    :param walkables: sequence of walkable objects
    """
    if not walkables:
        return
    walkable = walkables[0]
    try:
        entities = walkable.__walk__()
    except AttributeError:
        raise TypeError("Object %r is not walkable" % walkable)
    for entity in entities:
        yield entity
    end_node = walkable.end_node()
    for walkable in walkables[1:]:
        try:
            if end_node == walkable.start_node():
                entities = walkable.__walk__()
                end_node = walkable.end_node()
            elif end_node == walkable.end_node():
                entities = reversed(list(walkable.__walk__()))
                end_node = walkable.start_node()
            else:
                raise ValueError("Cannot append walkable %r "
                                 "to node %r" % (walkable, end_node))
        except AttributeError:
            raise TypeError("Object %r is not walkable" % walkable)
        for i, entity in enumerate(entities):
            if i > 0:
                yield entity


def cast(obj, entities=None):
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


class Subgraph(object):
    """ Arbitrary, unordered collection of nodes and relationships.
    """
    def __init__(self, nodes=None, relationships=None):
        self._nodes = frozenset(nodes or frozenset())
        self._relationships = frozenset(relationships or frozenset())
        self._nodes |= frozenset(chain(*(r.nodes() for r in self._relationships)))

    def __repr__(self):
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
        """ Set of all nodes.
        """
        return self._nodes

    def relationships(self):
        """ Set of all relationships.
        """
        return self._relationships

    def order(self):
        """ Total number of unique nodes.
        """
        return len(self._nodes)

    def size(self):
        """ Total number of unique relationships.
        """
        return len(self._relationships)

    def labels(self):
        """ Set of all node labels.
        """
        return frozenset(chain(*(node.labels() for node in self._nodes)))

    def types(self):
        """ Set of all relationship types.
        """
        return frozenset(rel.type() for rel in self._relationships)

    def keys(self):
        """ Set of all property keys.
        """
        return (frozenset(chain(*(node.keys() for node in self._nodes))) |
                frozenset(chain(*(rel.keys() for rel in self._relationships))))


class Walkable(Subgraph):
    """ A subgraph with added traversal information.
    """

    def __init__(self, iterable):
        sequence = tuple(iterable)
        self._node_sequence = sequence[0::2]
        self._relationship_sequence = sequence[1::2]
        Subgraph.__init__(self, self._node_sequence, self._relationship_sequence)
        self._sequence = sequence

    def __repr__(self):
        r = ReprIO()
        writer = CypherWriter(r)
        writer.write_walkable(self)
        return r.getvalue()

    def __eq__(self, other):
        try:
            other_walk = tuple(walk(other))
        except TypeError:
            return False
        else:
            return tuple(walk(self)) == other_walk

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        value = 0
        for item in self._sequence:
            value ^= hash(item)
        return value

    def __len__(self):
        return len(self._relationship_sequence)

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
            return Walkable(self._sequence[start:stop])
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
        return Walkable(walk(self, other))

    def __walk__(self):
        """ Traverse and yield all nodes and relationships in this
        object in order.
        """
        return iter(self._sequence)

    def start_node(self):
        """ The first node encountered on a :func:`.walk` of this object.
        """
        return self._node_sequence[0]

    def end_node(self):
        """ The last node encountered on a :func:`.walk` of this object.
        """
        return self._node_sequence[-1]

    def nodes(self):
        """ The sequence of nodes over which a :func:`.walk` of this
        object will traverse.
        """
        return self._node_sequence

    def relationships(self):
        """ The sequence of relationships over which a :func:`.walk`
        of this object will traverse.
        """
        return self._relationship_sequence


class PropertyDict(dict):
    """ A dictionary that treats :const:`None` and missing values as
    semantically identical.
    """

    def __init__(self, iterable=None, **kwargs):
        dict.__init__(self)
        self.update(iterable, **kwargs)

    def __eq__(self, other):
        return dict.__eq__(self, other)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        value = 0
        for k, v in self.items():
            if isinstance(v, list):
                value ^= hash((k, tuple(v)))
            else:
                value ^= hash((k, v))
        return value

    def __getitem__(self, key):
        return dict.get(self, key)

    def __setitem__(self, key, value):
        if value is None:
            try:
                dict.__delitem__(self, key)
            except KeyError:
                pass
        else:
            dict.__setitem__(self, key, coerce_property(value))

    def setdefault(self, key, default=None):
        if key in self:
            value = self[key]
        elif default is None:
            value = None
        else:
            value = dict.setdefault(self, key, default)
        return value

    def update(self, iterable=None, **kwargs):
        for key, value in dict(iterable or {}, **kwargs).items():
            self[key] = value


class EntityResource(Resource):
    """ A web resource that represents a graph database entity.
    """

    def __init__(self, uri, metadata=None):
        Resource.__init__(self, uri, metadata)
        self.ref = self.uri.string[len(self.graph.uri.string):]
        self._id = int(self.ref.rpartition("/")[2])


class Entity(PropertyDict, Walkable):
    """ Base class for objects that can be optionally bound to a remote resource. This
    class is essentially a container for a :class:`.Resource` instance.
    """

    _resource = None
    _resource_pending_tx = None

    def __init__(self, iterable, properties):
        Walkable.__init__(self, iterable)
        PropertyDict.__init__(self, properties)

    def __bool__(self):
        return len(self) > 0

    def __nonzero__(self):
        return len(self) > 0

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


class Node(Entity):
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
        Entity.__init__(self, (self,), properties)
        self.__stale = set()

    def __repr__(self):
        r = ReprIO()
        writer = CypherWriter(r)
        writer.write_node(self)
        return r.getvalue()

    def __eq__(self, other):
        if self.__class__ is not other.__class__:
            return False
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
        return Entity.__getitem__(self, item)

    @deprecated("Node.degree() is deprecated, use graph.degree(node) instead")
    def degree(self):
        return self.resource.graph.degree(self)

    @deprecated("Node.exists() is deprecated, use graph.exists(node) instead")
    def exists(self):
        return self.resource.graph.exists(self)

    def labels(self):
        """ Set of all node labels.
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


class Relationship(Entity):
    """ A relationship represents a typed connection between a pair of nodes.

    The positional arguments passed to the constructor identify the nodes to
    relate and the type of the relationship. Keyword arguments describe the
    properties of the relationship::

        >>> from py2neo import Node, Relationship
        >>> a = Node("Person", name="Alice")
        >>> b = Node("Person", name="Bob")
        >>> a_knows_b = Relationship(a, "KNOWS", b, since=1999)

    This class may be extended to allow relationship types names to be
    derived from the class name. For example::

        >>> class WorksWith(Relationship): pass
        >>> a_works_with_b = WorksWith(a, b)
        >>> a_works_with_b.type()
        'WORKS_WITH'

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
        Entity.__init__(self, (n[0], self, n[1]), p)

        self.__stale = set()

    def __repr__(self):
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


class Path(Walkable):
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
            if isinstance(entity, Entity):
                continue
            elif entity is None:
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
        Walkable.__init__(self, walk(*entities))


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
        from py2neo.types import Node, Relationship, Path
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
            from py2neo.types import entity_name
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
