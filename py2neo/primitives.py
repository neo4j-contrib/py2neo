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


from datetime import date, time, datetime
from decimal import Decimal

from py2neo.compat import integer, string
from py2neo.util import ustr


__all__ = ["GraphView", "Node", "Relationship", "Path"]


# Maximum and minimum integers supported up to Java 7.
# Java 8 also supports unsigned long which can extend
# to (2 ** 64 - 1) but Neo4j is not yet on Java 8.
JAVA_INTEGER_MIN_VALUE = -2 ** 63
JAVA_INTEGER_MAX_VALUE = 2 ** 63 - 1


def cast_property(value):
    """ Cast the supplied property value to something supported by
    Neo4j, raising an error if this is not possible.
    """
    if isinstance(value, (bool, float)):
        pass
    elif isinstance(value, integer):
        if JAVA_INTEGER_MIN_VALUE <= value <= JAVA_INTEGER_MAX_VALUE:
            pass
        else:
            raise ValueError("Integer value out of range: %s" % value)
    elif isinstance(value, string):
        value = ustr(value)
    elif isinstance(value, (frozenset, list, set, tuple)):
        # check each item and all same type
        list_value = []
        list_type = None
        for item in value:
            item = cast_property(item)
            if list_type is None:
                list_type = type(item)
                if list_type is list:
                    raise ValueError("Lists cannot contain nested collections")
            elif not isinstance(item, list_type):
                raise TypeError("List property items must be of similar types")
            list_value.append(item)
        value = list_value
    elif isinstance(value, (datetime, date, time)):
        value = value.isoformat()
    elif isinstance(value, Decimal):
        # We'll lose some precision here but Neo4j can't
        # handle decimals anyway.
        value = float(value)
    elif isinstance(value, complex):
        value = [value.real, value.imag]
    else:
        raise TypeError("Invalid property type: %s" % type(value))
    return value


class PropertySet(dict):
    """ A dict subclass that equates None with a non-existent key.
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
        for key, value in self.items():
            if isinstance(value, list):
                value ^= hash((key, tuple(value)))
            else:
                value ^= hash((key, value))
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
            dict.__setitem__(self, key, cast_property(value))

    def replace(self, iterable=None, **kwargs):
        self.clear()
        self.update(iterable, **kwargs)

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

    def __eq__(self, other):
        return self is other

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash(id(self))

    def __contains__(self, key):
        return key in self.properties

    def __getitem__(self, key):
        return self.properties.__getitem__(key)

    def __setitem__(self, key, value):
        self.properties.__setitem__(key, value)

    def __delitem__(self, key):
        self.properties.__delitem__(key)

    def __iter__(self):
        raise TypeError("%r object is not iterable" % self.__class__.__name__)


class EntityCollectionView(object):

    def __init__(self, collection):
        self._entities = collection

    def __eq__(self, other):
        return frozenset(self) == frozenset(other)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __len__(self):
        return len(self._entities)

    def __getitem__(self, index):
        return self._entities[index]

    def __iter__(self):
        return iter(self._entities)

    def __contains__(self, item):
        return item in self._entities

    def __or__(self, other):
        return EntityCollectionView(frozenset(self).union(other))

    def __and__(self, other):
        return EntityCollectionView(frozenset(self).intersection(other))

    def __sub__(self, other):
        return EntityCollectionView(frozenset(self).difference(other))

    def __xor__(self, other):
        return EntityCollectionView(frozenset(self).symmetric_difference(other))


class GraphView(object):

    def __init__(self, nodes, relationships):
        self.nodes = EntityCollectionView(nodes)
        self.relationships = EntityCollectionView(relationships)

    def __repr__(self):
        return "<%s order=%s size=%s>" % (self.__class__.__name__, self.order, self.size)

    def __eq__(self, other):
        try:
            return (set(self.nodes) == set(other.nodes) and
                    set(self.relationships) == set(other.relationships))
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
        return GraphView(self.nodes | other.nodes, self.relationships | other.relationships)

    def __and__(self, other):
        return GraphView(self.nodes & other.nodes, self.relationships & other.relationships)

    def __sub__(self, other):
        relationships = self.relationships - other.relationships
        nodes = (self.nodes - other.nodes) | set().union(*(rel.nodes for rel in relationships))
        return GraphView(nodes, relationships)

    def __xor__(self, other):
        relationships = self.relationships ^ other.relationships
        nodes = (self.nodes ^ other.nodes) | set().union(*(rel.nodes for rel in relationships))
        return GraphView(nodes, relationships)

    @property
    def order(self):
        return len(set(self.nodes))

    @property
    def size(self):
        return len(set(self.relationships))

    @property
    def property_keys(self):
        keys = set()
        for entity in self.nodes:
            keys |= set(entity.properties.keys())
        for entity in self.relationships:
            keys |= set(entity.properties.keys())
        return frozenset(keys)

    @property
    def labels(self):
        return frozenset().union(*(node.labels for node in self.nodes))

    @property
    def types(self):
        return frozenset(relationship.type for relationship in self.relationships)


class Identifiable(object):

    def __init__(self, identity=None):
        self.identity = identity


class DirectedGraphView(GraphView):

    def __init__(self, nodes, relationships, directions):
        GraphView.__init__(self, nodes, relationships)
        self.__directions = tuple(directions)

    def __eq__(self, other):
        try:
            return (tuple(self.nodes) == tuple(other.nodes) and
                    tuple(self.relationships) == tuple(other.relationships) and
                    tuple(self.directions) == tuple(other.directions))
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
        for direction in self.directions:
            value ^= hash(direction)
        return value

    def __len__(self):
        return self.length

    def __add__(self, other):
        return Path(self, other)

    @property
    def length(self):
        return len(self.relationships)

    @property
    def start_node(self):
        return self.nodes[0]

    @property
    def end_node(self):
        return self.nodes[-1]

    @property
    def directions(self):
        return self.__directions


class Node(Identifiable, PropertyContainer, DirectedGraphView):

    def __init__(self, *labels, **properties):
        Identifiable.__init__(self)
        PropertyContainer.__init__(self, **properties)
        DirectedGraphView.__init__(self, (self,), (), ())
        self.__labels = set(labels)

    def __repr__(self):
        return "<Node identity=%r labels=%r properties=%r>" % \
               (self.identity, set(self.labels), self.properties)

    def __eq__(self, other):
        try:
            other_nodes = tuple(other.nodes)
        except AttributeError:
            return False
        else:
            return len(other_nodes) == 1 and self is other_nodes[0]

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash(id(self))

    def __len__(self):
        return 0

    def __iter__(self):
        return iter([])

    @property
    def labels(self):
        return self.__labels


class Relationship(Identifiable, PropertyContainer, DirectedGraphView):

    def __init__(self, *args, **properties):
        Identifiable.__init__(self)
        PropertyContainer.__init__(self, **properties)
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
        else:
            nodes = (args[0],) + args[2:]
            self.type = args[1]
        DirectedGraphView.__init__(self, nodes, (self,), (0,))

    def __repr__(self):
        return "<Relationship identity=%r nodes=%r type=%r properties=%r>" % \
               (self.identity, tuple(self.nodes), self.type, self.properties)

    def __eq__(self, other):
        try:
            return (tuple(self.nodes) == tuple(other.nodes) and
                    (self,) == tuple(other.relationships))
        except AttributeError:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash(id(self))

    def __len__(self):
        return 1

    def __iter__(self):
        yield self


class Path(DirectedGraphView):

    def __init__(self, base, *sequence):
        # TODO: if base has size=1,direction=0 try forward and backward
        nodes = list(base.nodes)
        relationships = list(base.relationships)
        directions = list(base.directions)
        for item in sequence:
            if item is None:
                continue
            elif item.length == 0:
                last_node = nodes[-1]
                if item.nodes[0] != last_node:
                    raise ValueError("Non-continuous")
            else:
                for i, relationship in enumerate(item):
                    last_node = nodes[-1]
                    direction = item.directions[i]
                    if direction >= 0 and last_node == relationship.start_node:
                        next_node = relationship.end_node
                        directions.append(1)
                    elif direction <= 0 and last_node == relationship.end_node:
                        next_node = relationship.start_node
                        directions.append(-1)
                    else:
                        raise ValueError("Non-continuous")
                    nodes.append(next_node)
                    relationships.append(relationship)
        DirectedGraphView.__init__(self, nodes, relationships, directions)
