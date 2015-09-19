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


from itertools import chain

from py2neo.compat import integer, string, unicode, ustr


__all__ = ["Graph", "TraversableGraph", "Node", "Relationship", "Path"]

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
    else:
        raise TypeError("Properties of type %s are not supported" % x.__class__.__name__)


def coerce_property(x):
    if isinstance(x, (tuple, list, set, frozenset)):
        collection = []
        cls = None
        for item in x:
            item = coerce_atomic_property(item)
            if cls is None:
                cls = type(item)
            elif type(item) != cls:
                raise ValueError("List properties must be homogenous")
            collection.append(item)
        return x.__class__(collection)
    else:
        return coerce_atomic_property(x)


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
        self.properties = PropertySet(properties)

    def __len__(self):
        return len(self.properties)

    def __contains__(self, key):
        return key in self.properties

    def __getitem__(self, key):
        return self.properties.__getitem__(key)

    def __setitem__(self, key, value):
        self.properties.__setitem__(key, value)

    def __delitem__(self, key):
        self.properties.__delitem__(key)

    def __iter__(self):
        return iter(self.properties)

    @property
    def property_keys(self):
        return frozenset(self.properties.keys())


class Graph(object):
    """ Arbitrary, unordered collection of nodes and relationships.
    """

    def __init__(self, nodes, relationships):
        self.nodes = frozenset(nodes)
        self.relationships = frozenset(relationships)

    def __eq__(self, other):
        try:
            return self.nodes == other.nodes and self.relationships == other.relationships
        except AttributeError:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        value = 0
        for entity in self.nodes:
            value ^= hash(entity)
        for entity in self.relationships:
            value ^= hash(entity)
        return value

    def __len__(self):
        return self.size

    def __iter__(self):
        return iter(self.relationships)

    def __bool__(self):
        return bool(self.relationships)

    def __nonzero__(self):
        return bool(self.relationships)

    def __or__(self, other):
        return Graph(self.nodes | other.nodes, self.relationships | other.relationships)

    def __and__(self, other):
        return Graph(self.nodes & other.nodes, self.relationships & other.relationships)

    def __sub__(self, other):
        relationships = self.relationships - other.relationships
        nodes = (self.nodes - other.nodes) | set().union(*(rel.nodes for rel in relationships))
        return Graph(nodes, relationships)

    def __xor__(self, other):
        relationships = self.relationships ^ other.relationships
        nodes = (self.nodes ^ other.nodes) | set().union(*(rel.nodes for rel in relationships))
        return Graph(nodes, relationships)

    @property
    def order(self):
        """ Total number of unique nodes in this set.
        """
        return len(self.nodes)

    @property
    def size(self):
        """ Total number of unique relationships in this set.
        """
        return len(self.relationships)

    @property
    def labels(self):
        return frozenset(chain(*(node.labels for node in self.nodes)))

    @property
    def types(self):
        return frozenset(rel.type for rel in self.relationships)

    @property
    def property_keys(self):
        return (frozenset(chain(*(node.properties.keys() for node in self.nodes))) |
                frozenset(chain(*(rel.properties.keys() for rel in self.relationships))))


class TraversableGraph(Graph):
    """ A graph with traversal information.
    """

    def __init__(self, head, *tail):
        sequence = (head,) + tail
        Graph.__init__(self, sequence[0::2], sequence[1::2])
        self.__sequence = sequence

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
        for item in self.__sequence:
            value ^= hash(item)
        return value

    def __len__(self):
        return (len(self.__sequence) - 1) // 2

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
            return TraversableGraph(*self.__sequence[start:stop])
        else:
            return self.__sequence[2 * index + 1]

    def __iter__(self):
        for relationship in self.__sequence[1::2]:
            yield relationship

    def __add__(self, other):
        assert isinstance(other, TraversableGraph)
        if self.end_node == other.start_node:
            seq_1 = self.__sequence[:-1]
            seq_2 = other.traverse()
        elif self.length <= 1 and self.start_node is other.start_node:
            seq_1 = reversed(self.__sequence[1:])
            seq_2 = other.traverse()
        elif other.length <= 1 and self.end_node is other.end_node:
            seq_1 = self.__sequence[:-1]
            seq_2 = reversed(list(other.traverse()))
        elif self.length <= 1 and other.length <= 1 and self.start_node is other.end_node:
            seq_1 = reversed(self.__sequence[1:])
            seq_2 = reversed(list(other.traverse()))
        else:
            raise ValueError("Cannot concatenate walkable objects with no common endpoints")
        return TraversableGraph(*chain(seq_1, seq_2))

    @property
    def start_node(self):
        return self.__sequence[0]

    @property
    def end_node(self):
        return self.__sequence[-1]

    @property
    def length(self):
        return (len(self.__sequence) - 1) // 2

    def traverse(self):
        return iter(self.__sequence)


class Entity(PropertyContainer, TraversableGraph):

    def __init__(self, *sequence, **properties):
        PropertyContainer.__init__(self, **properties)
        TraversableGraph.__init__(self, *sequence)


class Node(Entity):
    """ A graph vertex with support for labels and properties.
    """

    def __init__(self, *labels, **properties):
        Entity.__init__(self, self, **properties)
        self.__labels = set(labels)

    def __repr__(self):
        return "(%s)" % "".join(":" + label for label in self.labels)

    def __eq__(self, other):
        try:
            return (other.order == 1 and other.size == 0 and
                    self.labels == other.labels and self.properties == other.properties)
        except AttributeError:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash(id(self))

    @property
    def labels(self):
        return self.__labels


class Relationship(Entity):
    """ A typed edge between two graph nodes with support for properties.
    """

    def __init__(self, *args, **properties):
        num_args = len(args)
        if num_args == 0:
            nodes = (None, None)
            self.type = None
        elif num_args == 1:
            nodes = (None, None)
            self.type = args[0]
        elif num_args == 2:
            nodes = args
            self.type = None
        elif num_args == 3:
            nodes = (args[0], args[2])
            self.type = args[1]
        else:
            raise TypeError("Hyperedges not supported")
        Entity.__init__(self, nodes[0], self, nodes[1], **properties)

    def __repr__(self):
        if self.type is not None:
            value = "-[:%s]->" % self.type
        else:
            value = "->"
        if self.start_node is not None:
            value = repr(self.start_node) + value
        if self.end_node is not None:
            value += repr(self.end_node)
        return value

    def __eq__(self, other):
        try:
            return (self.order == other.order and other.size == 1 and
                    self.start_node is other.start_node and self.end_node is other.end_node and
                    self.type == other.type and self.properties == other.properties)
        except AttributeError:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash(self.nodes) ^ hash(self.type)


class Path(TraversableGraph):

    def __new__(cls, head, *tail):
        path = TraversableGraph(*head.traverse())
        for item in tail:
            path += item
        path.__class__ = cls
        return path

    def __repr__(self):
        return "<Path length=%r>" % self.length
