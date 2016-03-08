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
import json
from sys import stdout
from uuid import uuid4

from py2neo.compat import integer, string, unicode, ustr, ReprIO
from py2neo.http import Resource
from py2neo.util import is_collection, round_robin, \
    ThreadLocalWeakValueDictionary, deprecated, relationship_case, snake_case


# Maximum and minimum integers supported up to Java 7.
# Java 8 also supports unsigned long which can extend
# to (2 ** 64 - 1) but Neo4j is not yet on Java 8.
JAVA_INTEGER_MIN_VALUE = -2 ** 63
JAVA_INTEGER_MAX_VALUE = 2 ** 63 - 1


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


def order(subgraph):
    """ Return the number of unique nodes in a subgraph.

    :arg subgraph:
    :return:
    """
    try:
        return subgraph.__order__()
    except AttributeError:
        try:
            return len(set(subgraph.nodes()))
        except AttributeError:
            raise TypeError("Object %r is not graphy")


def size(subgraph):
    """ Return the number of unique relationships in a subgraph.

    :arg subgraph:
    :return:
    """
    try:
        return subgraph.__size__()
    except AttributeError:
        try:
            return len(set(subgraph.relationships()))
        except AttributeError:
            raise TypeError("Object %r is not graphy")


def walk(*walkables):
    """ Traverse over the arguments supplied, yielding the entities
    from each in turn.

    :arg walkables: sequence of walkable objects
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
    elif isinstance(obj, (Relatable, Relationship, Path)):
        return obj
    elif isinstance(obj, dict):
        return cast_node(obj)
    elif isinstance(obj, tuple):
        return cast_relationship(obj, entities)
    else:
        raise TypeError(obj)


def cast_node(obj):
    if obj is None or isinstance(obj, Relatable):
        return obj

    def apply(x):
        if isinstance(x, dict):
            inst.update(x)
        elif is_collection(x):
            for item in x:
                apply(item)
        elif isinstance(x, string):
            inst.add_label(ustr(x))
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
    def __init__(self, nodes, relationships):
        self._nodes = frozenset(nodes)
        self._relationships = frozenset(relationships)
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

    def __order__(self):
        """ Total number of unique nodes.
        """
        return len(self._nodes)

    def __size__(self):
        """ Total number of unique relationships.
        """
        return len(self._relationships)

    def __len__(self):
        return len(self._relationships)

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
    """ A dictionary for property values that treats :const:`None`
    and missing values as semantically identical.

    PropertyDict instances can be created and used in a similar way
    to a standard dictionary. For example::

        >>> from py2neo import PropertyDict
        >>> fruit = PropertyDict({"name": "banana", "colour": "yellow"})
        >>> fruit["name"]
        'banana'

    The key difference with a PropertyDict is in how it handles
    missing values. Instead of raising a :py:class:`KeyError`,
    attempts to access a missing value will simply return
    :py:const:`None` instead.

    These are the operations that the PropertyDict can support:

   .. describe:: len(d)

        Return the number of items in the PropertyDict `d`.

   .. describe:: d[key]

        Return the item of `d` with key `key`. Returns :py:const:`None`
        if key is not in the map.

    """

    def __init__(self, iterable=None, **kwargs):
        dict.__init__(self)
        self.update(iterable, **kwargs)

    def __eq__(self, other):
        return dict.__eq__(self, {key: value for key, value in other.items() if value is not None})

    def __ne__(self, other):
        return not self.__eq__(other)

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


class RemoteEntity(Resource):
    """ A handle to a remote entity in a graph database.
    """

    def __init__(self, uri, metadata=None):
        Resource.__init__(self, uri, metadata)
        self.ref = self.uri.string[len(self.graph.remote.uri.string):]
        self._id = int(self.ref.rpartition("/")[2])

    def __repr__(self):
        return "<%s graph=%r ref=%r>" % (self.__class__.__name__,
                                         self.graph.remote.uri.string, self.ref)


class Entity(PropertyDict, Walkable):
    """ Base class for objects that can be optionally bound to a remote resource. This
    class is essentially a container for a :class:`.Resource` instance.
    """

    _remote = None
    _remote_pending_tx = None

    def __init__(self, iterable, properties):
        Walkable.__init__(self, iterable)
        PropertyDict.__init__(self, properties)
        uuid = str(uuid4())
        while "0" <= uuid[-7] <= "9":
            uuid = str(uuid4())
        self.__uuid__ = uuid
        if "__name__" in properties:
            self.__name__ = properties["__name__"]
        elif "name" in properties:
            self.__name__ = snake_case(properties["name"])
        else:
            self.__name__ = self.__uuid__[-7:]

    def __bool__(self):
        return len(self) > 0

    def __nonzero__(self):
        return len(self) > 0

    def _set_remote_pending(self, tx):
        self._remote_pending_tx = tx

    def _set_remote(self, uri, metadata=None):
        self._remote = RemoteEntity(uri, metadata)
        self._remote_pending_tx = None

    def _del_remote(self):
        self._remote = None
        self._remote_pending_tx = None

    @property
    def remote(self):
        """ Remote resource with which this entity is associated.
        """
        if self._remote_pending_tx:
            self._remote_pending_tx.process()
            self._remote_pending_tx = None
        return self._remote


class Relatable(object):
    """ Base class for objects that can be connected with relationships.
    """
    pass


class SetView(object):

    def __init__(self, items):
        assert isinstance(items, (set, frozenset))
        self.__items = items

    def __repr__(self):
        return "%s(%r)" % (self.__class__.__name__, set(self.__items))

    def __len__(self):
        return len(self.__items)

    def __iter__(self):
        return iter(self.__items)

    def __contains__(self, item):
        return item in self.__items

    def __and__(self, other):
        return self.__items & set(other)

    def __or__(self, other):
        return self.__items | set(other)

    def __sub__(self, other):
        return self.__items - set(other)

    def __xor__(self, other):
        return self.__items ^ set(other)


class Node(Relatable, Entity):
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
        inst._set_remote(self, data)
        if "data" in data:
            inst.__stale.discard("properties")
            inst.clear()
            inst.update(data["data"])
        if "metadata" in data:
            inst.__stale.discard("labels")
            metadata = data["metadata"]
            inst.clear_labels()
            inst.update_labels(metadata["labels"])
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
        if self.remote and other.remote:
            return self.remote == other.remote
        else:
            return self is other

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        if self.remote:
            return hash(self.remote.uri)
        else:
            return hash(id(self))

    def __getitem__(self, item):
        if self.remote and "properties" in self.__stale:
            self.remote.graph.pull(self)
        return Entity.__getitem__(self, item)

    @deprecated("Node.degree() is deprecated, use graph.degree(node) instead")
    def degree(self):
        return self.remote.graph.degree(self)

    @deprecated("Node.exists() is deprecated, use graph.exists(node) instead")
    def exists(self):
        return self.remote.graph.exists(self)

    def __ensure_labels(self):
        if self.remote and "labels" in self.__stale:
            self.remote.graph.pull(self)

    def labels(self):
        """ Set of all node labels.
        """
        self.__ensure_labels()
        return SetView(self._labels)

    def has_label(self, label):
        self.__ensure_labels()
        return label in self._labels

    def add_label(self, label):
        self.__ensure_labels()
        self._labels.add(label)

    def remove_label(self, label):
        self.__ensure_labels()
        self._labels.discard(label)

    def clear_labels(self):
        self.__ensure_labels()
        self._labels.clear()

    def update_labels(self, labels):
        self.__ensure_labels()
        self._labels.update(labels)

    @deprecated("Node.match() is deprecated, use graph.match(node, ...) instead")
    def match(self, rel_type=None, other_node=None, limit=None):
        return self.remote.graph.match(self, rel_type, other_node, True, limit)

    @deprecated("Node.match_incoming() is deprecated, use graph.match(node, ...) instead")
    def match_incoming(self, rel_type=None, start_node=None, limit=None):
        return self.remote.graph.match(start_node, rel_type, self, False, limit)

    @deprecated("Node.match_outgoing() is deprecated, use graph.match(node, ...) instead")
    def match_outgoing(self, rel_type=None, end_node=None, limit=None):
        return self.remote.graph.match(self, rel_type, end_node, False, limit)

    @property
    @deprecated("Node.properties is deprecated, use dict(node) instead")
    def properties(self):
        if self.remote and "properties" in self.__stale:
            self.remote.graph.pull(self)
        return dict(self)

    @deprecated("Node.pull() is deprecated, use graph.pull(node) instead")
    def pull(self):
        self.remote.graph.pull(self)

    @deprecated("Node.push() is deprecated, use graph.push(node) instead")
    def push(self):
        self.remote.graph.push(self)

    def _del_remote(self):
        try:
            del self.cache[self.remote.uri]
        except KeyError:
            pass
        Entity._del_remote(self)


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
            return "TO"
        assert issubclass(cls, Relationship)
        return ustr(relationship_case(cls.__name__))

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
        inst._set_remote(self, data)
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
            if self.remote and other.remote:
                return self.remote == other.remote
            else:
                return (self.nodes() == other.nodes() and size(other) == 1 and
                        self.type() == other.type() and dict(self) == dict(other))

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash(self.nodes()) ^ hash(self.type())

    @deprecated("Relationship.exists() is deprecated, "
                "use graph.exists(relationship) instead")
    def exists(self):
        return self.remote.graph.exists(self)

    @property
    @deprecated("Relationship.properties is deprecated, use dict(relationship) instead")
    def properties(self):
        if self.remote and "properties" in self.__stale:
            self.remote.graph.pull(self)
        return dict(self)

    @deprecated("Relationship.pull() is deprecated, use graph.pull(relationship) instead")
    def pull(self):
        self.remote.graph.pull(self)

    @deprecated("Relationship.push() is deprecated, use graph.push(relationship) instead")
    def push(self):
        self.remote.graph.push(self)

    def type(self):
        """ The type of this relationship.
        """
        if self.remote and self._type is None:
            self.remote.graph.pull(self)
        return self._type

    def _del_remote(self):
        """ Detach this relationship and its start and end
        nodes from any remote counterparts.
        """
        try:
            del self.cache[self.remote.uri]
        except KeyError:
            pass
        Entity._del_remote(self)


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
        
        :arg obj: 
        """
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
        
        :arg value: 
        """
        self.file.write(ustr(json.dumps(value, ensure_ascii=False)))

    def write_identifier(self, identifier):
        """ Write an identifier.
        
        :arg identifier: 
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
        
        :arg collection: 
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
        
        :arg text: 
        """
        self.file.write(ustr(text))

    def write_map(self, mapping, private=False):
        """ Write a map.
        
        :arg mapping: 
        :arg private: 
        """
        self.file.write(u"{")
        link = u""
        for key, value in sorted(dict(mapping).items()):
            if key.startswith("_") and not private:
                continue
            self.file.write(link)
            self.write_identifier(key)
            self.file.write(self.key_value_separator)
            self.write(value)
            link = self.sequence_separator
        self.file.write(u"}")

    def write_node(self, node, name=None, full=True):
        """ Write a node.
        
        :arg node: 
        :arg name: 
        :arg full: 
        """
        self.file.write(u"(")
        if name is None:
            name = node.__name__
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
        
        :arg relationship:
        :arg name:
        """
        self.write_node(relationship.start_node(), full=False)
        self.file.write(u"-")
        self.write_relationship_detail(relationship, name)
        self.file.write(u"->")
        self.write_node(relationship.end_node(), full=False)

    def write_relationship_detail(self, relationship, name=None):
        """ Write a relationship (excluding nodes).
        
        :arg relationship:
        :arg name:
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
        
        :arg subgraph: 
        """
        self.write_literal("({")
        for i, node in enumerate(subgraph.nodes()):
            if i > 0:
                self.write_literal(", ")
            self.write_node(node)
        self.write_literal("}, {")
        for i, relationship in enumerate(subgraph.relationships()):
            if i > 0:
                self.write_literal(", ")
            self.write_relationship(relationship)
        self.write_literal("})")

    def write_walkable(self, walkable):
        """ Write a walkable.
        
        :arg walkable: 
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

    :arg identifier: 
    """
    s = StringIO()
    writer = CypherWriter(s)
    writer.write_identifier(identifier)
    return s.getvalue()


def cypher_repr(obj):
    """ Generate the Cypher representation of an object.
    
    :arg obj: 
    """
    s = StringIO()
    writer = CypherWriter(s)
    writer.write(obj)
    return s.getvalue()
