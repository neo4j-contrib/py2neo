#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2014, Nigel Small
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


from itertools import chain
from re import compile as re_compile

from .compat import integer, string, unicode, ustr


__all__ = ["Graph", "TraversableGraph", "Node", "Relationship", "Path",
           "Record", "RecordList"]

# Maximum and minimum integers supported up to Java 7.
# Java 8 also supports unsigned long which can extend
# to (2 ** 64 - 1) but Neo4j is not yet on Java 8.
JAVA_INTEGER_MIN_VALUE = -2 ** 63
JAVA_INTEGER_MAX_VALUE = 2 ** 63 - 1

# Word separation patterns for re-casing strings.
# Taken from:
#   http://stackoverflow.com/questions/1175208/elegant-python-function-to-convert-camelcase-to-camel-case
WORD_FIRST = re_compile("(.)([A-Z][a-z]+)")
WORD_ALL = re_compile("([a-z0-9])([A-Z])")


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


def relationship_case(s):
    s1 = WORD_FIRST.sub(r"\1_\2", s)
    return WORD_ALL.sub(r"\1_\2", s1).upper()


class PropertySet(dict):
    """ A dictionary subclass that equates None with a non-existent key.
    """

    def __init__(self, iterable=None, **kwargs):
        dict.__init__(self)
        self.update(iterable, **kwargs)

    def __eq__(self, other):
        if not isinstance(other, PropertySet):
            other = PropertySet(other)
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


class PropertyContainer(object):
    """ Base class for objects that contain a set of properties,
    """

    def __init__(self, **properties):
        self._properties = PropertySet(properties)

    def __bool__(self):
        return bool(self._properties)

    def __nonzero__(self):
        return bool(self._properties)

    def __len__(self):
        return len(self._properties)

    def __contains__(self, key):
        return key in self._properties

    def __getitem__(self, key):
        return self._properties.__getitem__(key)

    def __setitem__(self, key, value):
        self._properties.__setitem__(key, value)

    def __delitem__(self, key):
        self._properties.__delitem__(key)

    def __iter__(self):
        return iter(self._properties)

    def clear(self):
        self._properties.clear()

    def get(self, key, default=None):
        return self._properties.get(key, default)

    def keys(self):
        return self._properties.keys()

    def setdefault(self, key, default=None):
        return self._properties.setdefault(key, default)

    def update(self, iterable=None, **kwargs):
        self._properties.update(iterable, **kwargs)

    def values(self):
        return self._properties.values()


class Graph(object):
    """ Arbitrary, unordered collection of nodes and relationships.
    """

    def __init__(self, nodes=None, relationships=None):
        self._nodes = frozenset(nodes or frozenset())
        self._relationships = frozenset(relationships or frozenset())
        self._nodes |= frozenset(chain(*(r.nodes() for r in self._relationships)))

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
        nodes = Graph.nodes
        relationships = Graph.relationships
        return Graph(nodes(self) | nodes(other), relationships(self) | relationships(other))

    def __and__(self, other):
        nodes = Graph.nodes
        relationships = Graph.relationships
        return Graph(nodes(self) & nodes(other), relationships(self) & relationships(other))

    def __sub__(self, other):
        nodes = Graph.nodes
        relationships = Graph.relationships
        r = relationships(self) - relationships(other)
        n = (nodes(self) - nodes(other)) | set().union(*(nodes(rel) for rel in r))
        return Graph(n, r)

    def __xor__(self, other):
        nodes = Graph.nodes
        relationships = Graph.relationships
        r = relationships(self) ^ relationships(other)
        n = (nodes(self) ^ nodes(other)) | set().union(*(nodes(rel) for rel in r))
        return Graph(n, r)

    def nodes(self):
        return self._nodes

    def relationships(self):
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
        return frozenset(chain(*(node.labels() for node in self._nodes)))

    def types(self):
        return frozenset(rel.type() for rel in self._relationships)

    def keys(self):
        return (frozenset(chain(*(node.keys() for node in self._nodes))) |
                frozenset(chain(*(rel.keys() for rel in self._relationships))))


class TraversableGraph(Graph):
    """ A graph with traversal information.
    """

    def __init__(self, head, *tail):
        sequence = (head,) + tail
        self._node_sequence = sequence[0::2]
        self._relationship_sequence = sequence[1::2]
        Graph.__init__(self, self._node_sequence, self._relationship_sequence)
        self._sequence = sequence

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
            return TraversableGraph(*self._sequence[start:stop])
        else:
            return self._sequence[2 * index + 1]

    def __iter__(self):
        for relationship in self._relationship_sequence:
            yield relationship

    def __add__(self, other):
        if other is None:
            return self
        if not isinstance(other, TraversableGraph):
            raise TypeError(other)
        if self.end_node() == other.start_node():
            # Try a direct append
            seq_1 = self._sequence[:-1]
            seq_2 = other.traverse()
        elif other.length() <= 1 and self.end_node() == other.end_node():
            # Try reversing other
            seq_1 = self._sequence[:-1]
            seq_2 = reversed(list(other.traverse()))
        elif self.length() <= 1 and self.start_node() == other.start_node():
            # Try reversing self
            seq_1 = reversed(self._sequence[1:])
            seq_2 = other.traverse()
        elif self.length() <= 1 and other.length() <= 1 and self.start_node() == other.end_node():
            # Try reversing both
            seq_1 = reversed(self._sequence[1:])
            seq_2 = reversed(list(other.traverse()))
        else:
            raise ValueError("Cannot concatenate traversable objects with no common "
                             "endpoints: %r, %r" % (self, other))
        return TraversableGraph(*chain(seq_1, seq_2))

    def start_node(self):
        return self._node_sequence[0]

    def end_node(self):
        return self._node_sequence[-1]

    def length(self):
        return len(self._relationship_sequence)

    def traverse(self):
        return iter(self._sequence)

    def nodes(self):
        return self._node_sequence

    def relationships(self):
        return self._relationship_sequence


def traverse(*traversables):
    if not traversables:
        return
    traversable = traversables[0]
    for entity in traversable.traverse():
        yield entity
    end_node = traversable.end_node()
    for traversable in traversables[1:]:
        if end_node == traversable.start_node():
            entities = traversable.traverse()
            end_node = traversable.end_node()
        elif end_node == traversable.end_node():
            entities = reversed(list(traversable.traverse()))
            end_node = traversable.start_node()
        else:
            raise ValueError("Cannot append traversable %r "
                             "to node %r" % (traversable, end_node))
        for i, entity in enumerate(entities):
            if i > 0:
                yield entity


class Entity(PropertyContainer, TraversableGraph):

    def __init__(self, *sequence, **properties):
        PropertyContainer.__init__(self, **properties)
        TraversableGraph.__init__(self, *sequence)


class Node(Entity):
    """ A graph vertex with support for labels and properties.
    """

    def __init__(self, *labels, **properties):
        self._labels = set(labels)
        Entity.__init__(self, self, **properties)

    def __repr__(self):
        return "(%s {...})" % "".join(":" + label for label in self._labels)

    def __eq__(self, other):
        return self is other

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash(id(self))

    def labels(self):
        return self._labels

    def add_label(self, label):
        self._labels.add(label)

    def clear_labels(self):
        self._labels.clear()

    def discard_label(self, label):
        self._labels.discard(label)

    def has_label(self, label):
        return label in self._labels

    def remove_label(self, label):
        self._labels.remove(label)

    def update_labels(self, labels):
        self._labels.update(labels)


class Relationship(Entity):
    """ A typed edge between two graph nodes with support for properties.
    """

    @classmethod
    def default_type(cls):
        if cls is Relationship:
            return None
        else:
            return ustr(relationship_case(cls.__name__))

    def __init__(self, *nodes, **properties):
        """

            >>> a = Node(name="Alice")
            >>> b = Node(name="Bob")

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
        num_args = len(nodes)
        if num_args == 0:
            raise TypeError("Relationships must specify at least one endpoint")
        elif num_args == 1:
            # Relationship(a)
            self._type = self.default_type()
            nodes = (nodes[0], nodes[0])
        elif num_args == 2:
            if nodes[1] is None or isinstance(nodes[1], string):
                # Relationship(a, "TO")
                self._type = nodes[1]
                nodes = (nodes[0], nodes[0])
            else:
                # Relationship(a, b)
                self._type = self.default_type()
                nodes = (nodes[0], nodes[1])
        elif num_args == 3:
            # Relationship(a, "TO", b)
            self._type = nodes[1]
            nodes = (nodes[0], nodes[2])
        else:
            raise TypeError("Hyperedges not supported")
        Entity.__init__(self, nodes[0], self, nodes[1], **properties)

    def __repr__(self):
        if self._type is not None:
            value = "-[:%s]->" % self._type
        else:
            value = "->"
        if self.start_node() is not None:
            value = repr(self.start_node()) + value
        if self.end_node() is not None:
            value += repr(self.end_node())
        return value

    def __eq__(self, other):
        try:
            return (self.nodes() == other.nodes() and other.size() == 1 and
                    self.type() == other.type() and dict(self) == dict(other))
        except AttributeError:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash(self.nodes()) ^ hash(self.type())

    def type(self):
        return self._type


class Path(TraversableGraph):

    def __init__(self, head, *tail):
        TraversableGraph.__init__(self, *traverse(head, *tail))

    def __repr__(self):
        return "<Path length=%r>" % self.length()


class Record(tuple, Graph):

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
        Graph.__init__(self, nodes, relationships)
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


class RecordList(Graph):

    def __init__(self, records):
        keys = set()
        nodes = []
        relationships = []
        for record in records:
            keys.update(record.keys())
            nodes.extend(record.nodes())
            relationships.extend(record.relationships())
        Graph.__init__(self, nodes, relationships)
        self.__keys = frozenset(keys)
        self.__records = list(records)

    def __len__(self):
        return len(self.__records)

    def __getitem__(self, item):
        if isinstance(item, slice):
            return self.__class__(self.__records[item.start:item.stop])
        else:
            return self.__records[item]

    def __iter__(self):
        return iter(self.__records)

    def keys(self):
        return self.__keys
