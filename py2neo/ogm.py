#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2018, Nigel Small
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


from py2neo.cypher.writing import cypher_escape
from py2neo.data import Node, PropertyDict
from py2neo.internal.util import metaclass, label_case, relationship_case
from py2neo.matching import NodeMatch, NodeMatcher


OUTGOING = 1
UNDIRECTED = 0
INCOMING = -1


class Property(object):
    """ A property definition for a :class:`.GraphObject`.
    """

    def __init__(self, key=None):
        self.key = key

    def __get__(self, instance, owner):
        return instance.__ogm__.node[self.key]

    def __set__(self, instance, value):
        instance.__ogm__.node[self.key] = value


class Label(object):
    """ Describe a node label for a :class:`.GraphObject`.
    """

    def __init__(self, name=None):
        self.name = name

    def __get__(self, instance, owner):
        return instance.__ogm__.node.has_label(self.name)

    def __set__(self, instance, value):
        if value:
            instance.__ogm__.node.add_label(self.name)
        else:
            instance.__ogm__.node.remove_label(self.name)


class Related(object):
    """ Describe a set of related objects for a :class:`.GraphObject`.

    :param related_class: class of object to which these relationships connect
    :param relationship_type: underlying relationship type for these relationships
    """

    direction = UNDIRECTED

    def __init__(self, related_class, relationship_type=None):
        self.related_class = related_class
        self.relationship_type = relationship_type

    def resolve_related_class(self, instance):
        if not isinstance(self.related_class, type):
            module_name, _, class_name = self.related_class.rpartition(".")
            if not module_name:
                module_name = instance.__class__.__module__
            module = __import__(module_name, fromlist=".")
            self.related_class = getattr(module, class_name)

    def __get__(self, instance, owner):
        cog = instance.__ogm__
        related = cog.related
        key = (self.direction, self.relationship_type)
        if key not in related:
            self.resolve_related_class(instance)
            related[key] = RelatedObjects(cog.node, self.direction, self.relationship_type, self.related_class)
        return related[key]


class RelatedTo(Related):
    """ Describe a set of related objects for a :class:`.GraphObject`
    that are connected by outgoing relationships.

    :param related_class: class of object to which these relationships connect
    :param relationship_type: underlying relationship type for these relationships
    """

    direction = OUTGOING


class RelatedFrom(Related):
    """ Describe a set of related objects  for a :class:`.GraphObject`
    that are connected by incoming relationships.

    :param related_class: class of object to which these relationships connect
    :param relationship_type: underlying relationship type for these relationships
    """

    direction = INCOMING


class RelatedObjects(object):
    """ A set of similarly-typed and similarly-related objects,
    relative to a central node.
    """

    def __init__(self, node, direction, relationship_type, related_class):
        assert isinstance(direction, int) and not isinstance(direction, bool)
        self.node = node
        self.related_class = related_class
        self.__related_objects = None
        if direction > 0:
            self.__match_args = {"nodes": (self.node, None), "r_type": relationship_type}
            self.__start_node = False
            self.__end_node = True
            self.__relationship_pattern = "(a)-[_:%s]->(b)" % cypher_escape(relationship_type)
        elif direction < 0:
            self.__match_args = {"nodes": (None, self.node), "r_type": relationship_type}
            self.__start_node = True
            self.__end_node = False
            self.__relationship_pattern = "(a)<-[_:%s]-(b)" % cypher_escape(relationship_type)
        else:
            self.__match_args = {"nodes": {self.node, None}, "r_type": relationship_type}
            self.__start_node = True
            self.__end_node = True
            self.__relationship_pattern = "(a)-[_:%s]-(b)" % cypher_escape(relationship_type)

    def __iter__(self):
        for obj, _ in self._related_objects:
            yield obj

    def __len__(self):
        return len(self._related_objects)

    def __contains__(self, obj):
        for related_object, _ in self._related_objects:
            if related_object == obj:
                return True
        return False

    @property
    def _related_objects(self):
        if self.__related_objects is None:
            self.__related_objects = []
            if self.node.graph:
                with self.node.graph.begin() as tx:
                    self.__db_pull__(tx)
        return self.__related_objects

    def add(self, obj, properties=None, **kwproperties):
        """ Add a related object.

        :param obj: the :py:class:`.GraphObject` to relate
        :param properties: dictionary of properties to attach to the relationship (optional)
        :param kwproperties: additional keyword properties (optional)
        """
        related_objects = self._related_objects
        properties = PropertyDict(properties or {}, **kwproperties)
        added = False
        for i, (related_object, _) in enumerate(related_objects):
            if related_object == obj:
                related_objects[i] = (obj, properties)
                added = True
        if not added:
            related_objects.append((obj, properties))

    def clear(self):
        """ Remove all related objects from this set.
        """
        self._related_objects[:] = []

    def get(self, obj, key, default=None):
        """ Return a relationship property associated with a specific related object.

        :param obj: related object
        :param key: relationship property key
        :param default: default value, in case the key is not found
        :return: property value
        """
        for related_object, properties in self._related_objects:
            if related_object == obj:
                return properties.get(key, default)
        return default

    def remove(self, obj):
        """ Remove a related object.

        :param obj: the :py:class:`.GraphObject` to separate
        """
        related_objects = self._related_objects
        related_objects[:] = [(related_object, properties)
                              for related_object, properties in related_objects
                              if related_object != obj]

    def update(self, obj, properties=None, **kwproperties):
        """ Add or update a related object.

        :param obj: the :py:class:`.GraphObject` to relate
        :param properties: dictionary of properties to attach to the relationship (optional)
        :param kwproperties: additional keyword properties (optional)
        """
        related_objects = self._related_objects
        properties = dict(properties or {}, **kwproperties)
        added = False
        for i, (related_object, p) in enumerate(related_objects):
            if related_object == obj:
                related_objects[i] = (obj, PropertyDict(p, **properties))
                added = True
        if not added:
            related_objects.append((obj, properties))

    def __db_pull__(self, tx):
        related_objects = {}
        for r in tx.graph.match(**self.__match_args):
            nodes = []
            n = self.node
            a = r.start_node
            b = r.end_node
            if a == b:
                nodes.append(a)
            else:
                if self.__start_node and a != n:
                    nodes.append(r.start_node)
                if self.__end_node and b != n:
                    nodes.append(r.end_node)
            for node in nodes:
                related_object = self.related_class.wrap(node)
                related_objects[node] = (related_object, PropertyDict(r))
        self._related_objects[:] = related_objects.values()

    def __db_push__(self, tx):
        related_objects = self._related_objects
        # 1. merge all nodes (create ones that don't)
        for related_object, _ in related_objects:
            tx.merge(related_object)
        # 2a. remove any relationships not in list of nodes
        subject_id = self.node.identity
        tx.run("MATCH %s WHERE id(a) = {x} AND NOT id(b) IN {y} DELETE _" % self.__relationship_pattern,
               x=subject_id, y=[obj.__ogm__.node.identity for obj, _ in related_objects])
        # 2b. merge all relationships
        for related_object, properties in related_objects:
            tx.run("MATCH (a) WHERE id(a) = {x} MATCH (b) WHERE id(b) = {y} "
                   "MERGE %s SET _ = {z}" % self.__relationship_pattern,
                   x=subject_id, y=related_object.__ogm__.node.identity, z=properties)


class OGM(object):

    def __init__(self, node):
        self.node = node
        self.related = {}


class GraphObjectType(type):

    def __new__(mcs, name, bases, attributes):
        for attr_name, attr in list(attributes.items()):
            if isinstance(attr, Property):
                if attr.key is None:
                    attr.key = attr_name
            elif isinstance(attr, Label):
                if attr.name is None:
                    attr.name = label_case(attr_name)
            elif isinstance(attr, RelatedTo):
                if attr.relationship_type is None:
                    attr.relationship_type = relationship_case(attr_name)

        attributes.setdefault("__primarylabel__", name)

        primary_key = attributes.get("__primarykey__")
        if primary_key is None:
            for base in bases:
                if primary_key is None and hasattr(base, "__primarykey__"):
                    primary_key = getattr(base, "__primarykey__")
                    break
            else:
                primary_key = "__id__"
            attributes["__primarykey__"] = primary_key

        return super(GraphObjectType, mcs).__new__(mcs, name, bases, attributes)


@metaclass(GraphObjectType)
class GraphObject(object):
    """ The base class for all OGM classes.
    """

    __primarylabel__ = None
    __primarykey__ = None

    __ogm = None

    def __eq__(self, other):
        if self is other:
            return True
        try:
            self_node = self.__ogm__.node
            other_node = other.__ogm__.node
            if any(x is None for x in [self_node.graph, other_node.graph, self_node.identity, other_node.identity]):
                return self.__primarylabel__ == other.__primarylabel__ and \
                       self.__primarykey__ == other.__primarykey__ and \
                       self.__primaryvalue__ == other.__primaryvalue__
            return type(self) is type(other) and self_node.graph == other_node.graph and self_node.identity == other_node.identity
        except (AttributeError, TypeError):
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    @property
    def __ogm__(self):
        if self.__ogm is None:
            self.__ogm = OGM(Node(self.__primarylabel__))
        node = self.__ogm.node
        if not hasattr(node, "__primarylabel__"):
            setattr(node, "__primarylabel__", self.__primarylabel__)
        if not hasattr(node, "__primarykey__"):
            setattr(node, "__primarykey__", self.__primarykey__)
        return self.__ogm

    @classmethod
    def wrap(cls, node):
        if node is None:
            return None
        inst = GraphObject()
        inst.__ogm = OGM(node)
        inst.__class__ = cls
        for attr in dir(inst):
            _ = getattr(inst, attr)
        return inst

    @classmethod
    def match(cls, graph, primary_value=None):
        """ Select one or more nodes from the database, wrapped as instances of this class.

        :param graph: the :class:`.Graph` instance in which to match
        :param primary_value: value of the primary property (optional)
        :rtype: :class:`.GraphObjectMatch`
        """
        return GraphObjectMatcher(cls, graph).match(primary_value)

    def __repr__(self):
        return "<%s %s=%r>" % (self.__class__.__name__, self.__primarykey__, self.__primaryvalue__)

    @property
    def __primaryvalue__(self):
        node = self.__ogm__.node
        primary_key = self.__primarykey__
        if primary_key == "__id__":
            return node.identity
        else:
            return node[primary_key]

    def __db_create__(self, tx):
        self.__db_merge__(tx)

    def __db_delete__(self, tx):
        ogm = self.__ogm__
        if ogm.node.identity is not None:
            tx.run("MATCH (a) WHERE id(a) = {x} OPTIONAL MATCH (a)-[r]->() DELETE r DELETE a", x=ogm.node.identity)
        for related_objects in ogm.related.values():
            related_objects.clear()

    def __db_merge__(self, tx, primary_label=None, primary_key=None):
        ogm = self.__ogm__
        node = ogm.node
        if primary_label is None:
            primary_label = getattr(node, "__primarylabel__", None)
        if primary_key is None:
            primary_key = getattr(node, "__primarykey__", "__id__")
        if node.graph is None:
            if primary_key == "__id__":
                node.add_label(primary_label)
                tx.create(node)
            else:
                tx.merge(node, primary_label, primary_key)
            for related_objects in ogm.related.values():
                related_objects.__db_push__(tx)

    def __db_pull__(self, tx):
        ogm = self.__ogm__
        if ogm.node.graph is None:
            matcher = GraphObjectMatcher(self.__class__, tx.graph)
            matcher._match_class = NodeMatch
            ogm.node = matcher.match(self.__primaryvalue__).first()
        tx.pull(ogm.node)
        for related_objects in ogm.related.values():
            related_objects.__db_pull__(tx)

    def __db_push__(self, tx):
        ogm = self.__ogm__
        node = ogm.node
        if node.graph is not None:
            tx.push(node)
        else:
            primary_key = getattr(node, "__primarykey__", "__id__")
            if primary_key == "__id__":
                tx.create(node)
            else:
                tx.merge(node)
        for related_objects in ogm.related.values():
            related_objects.__db_push__(tx)


class GraphObjectMatch(NodeMatch):
    """ A selection of :class:`.GraphObject` instances that match a
    given set of criteria.
    """

    _object_class = GraphObject

    def __iter__(self):
        """ Iterate through items drawn from the underlying graph that
        match the given criteria.
        """
        wrap = self._object_class.wrap
        for node in super(GraphObjectMatch, self).__iter__():
            yield wrap(node)

    def first(self):
        """ Return the first item that matches the given criteria.
        """
        return self._object_class.wrap(super(GraphObjectMatch, self).first())


class GraphObjectMatcher(NodeMatcher):

    _match_class = GraphObjectMatch

    def __init__(self, object_class, graph):
        NodeMatcher.__init__(self, graph)
        self._object_class = object_class
        self._match_class = type("%sMatch" % self._object_class.__name__,
                                 (GraphObjectMatch,), {"_object_class": object_class})

    def match(self, primary_value=None):
        cls = self._object_class
        properties = {}
        if primary_value is not None:
            properties[cls.__primarykey__] = primary_value
        return NodeMatcher.match(self, cls.__primarylabel__, **properties)
