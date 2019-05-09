#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2019, Nigel Small
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


from py2neo.cypher import cypher_escape
from py2neo.data import Node, PropertyDict
from py2neo.internal.compat import metaclass
from py2neo.internal.text import Words
from py2neo.matching import NodeMatch, NodeMatcher


OUTGOING = 1
UNDIRECTED = 0
INCOMING = -1


def resolve_class(cls, instance):
    if isinstance(cls, type):
        return cls
    module_name, _, class_name = cls.rpartition(".")
    if not module_name:
        module_name = instance.__class__.__module__
    module = __import__(module_name, fromlist=".")
    return getattr(module, class_name)


class Property(object):
    """ Property definition for a :class:`.GraphObject`.

    Attributes:
        key: The name of the node property within the database.
        default: The default value for the property, if it would
                 otherwise be :const:`None`.
    """

    def __init__(self, key=None, default=None):
        """ Initialise a property definition.

        Args:
            key: The name of the node property within the database. If
                 omitted, the name of the class attribute is used.
            default: The default value for the property, if it would
                     otherwise be :const:`None`.
        """
        self.key = key
        self.default = default

    def __get__(self, instance, owner):
        value = instance.__node__[self.key]
        if value is None:
            value = self.default
        return value

    def __set__(self, instance, value):
        instance.__node__[self.key] = value


class Label(object):
    """ Label definition for a :class:`.GraphObject`.

    Labels are toggleable tags applied to an object that can be used as type
    information or other forms of classification.
    """

    def __init__(self, name=None):
        self.name = name

    def __get__(self, instance, owner):
        return instance.__node__.has_label(self.name)

    def __set__(self, instance, value):
        if value:
            instance.__node__.add_label(self.name)
        else:
            instance.__node__.remove_label(self.name)


class Related(object):
    """ Descriptor for a set of related objects in a :class:`.GraphObject`.
    """

    direction = UNDIRECTED

    def __init__(self, related_class, relationship_type=None):
        """ Initialise a property definition.

        Args:
            related_class: The class of object to which these relationships
                           connect.
            relationship_type: The underlying relationship type for these
                               relationships.
        """
        self.related_class = related_class
        self.relationship_type = relationship_type

    def __get__(self, instance, owner):
        return instance.__ogm__.related(self.direction, self.relationship_type,
                                        resolve_class(self.related_class, instance))


class RelatedTo(Related):
    """ Descriptor for a set of related objects for a :class:`.GraphObject`
    that are connected by outgoing relationships.
    """

    direction = OUTGOING


class RelatedFrom(Related):
    """ Descriptor for a set of related objects for a :class:`.GraphObject`
    that are connected by incoming relationships.
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
               x=subject_id, y=[obj.__node__.identity for obj, _ in related_objects])
        # 2b. merge all relationships
        for related_object, properties in related_objects:
            tx.run("MATCH (a) WHERE id(a) = {x} MATCH (b) WHERE id(b) = {y} "
                   "MERGE %s SET _ = {z}" % self.__relationship_pattern,
                   x=subject_id, y=related_object.__node__.identity, z=properties)


class OGM(object):

    def __init__(self, node):
        self.node = node
        self._related = {}

    def all_related(self):
        """ Return an iterator through all :class:`.RelatedObjects`.
        """
        return iter(self._related.values())

    def related(self, direction, relationship_type, related_class):
        """ Return :class:`.RelatedObjects` for given criteria.
        """
        key = (direction, relationship_type)
        if key not in self._related:
            self._related[key] = RelatedObjects(self.node, direction, relationship_type, related_class)
        return self._related[key]


class GraphObjectType(type):

    def __new__(mcs, name, bases, attributes):
        for attr_name, attr in list(attributes.items()):
            if isinstance(attr, Property):
                if attr.key is None:
                    attr.key = attr_name
            elif isinstance(attr, Label):
                if attr.name is None:
                    attr.name = Words(attr_name).camel(upper_first=True)
            elif isinstance(attr, Related):
                if attr.relationship_type is None:
                    attr.relationship_type = Words(attr_name).upper("_")

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
            self_node = self.__node__
            other_node = other.__node__
            if any(x is None for x in [self_node.graph, other_node.graph, self_node.identity, other_node.identity]):
                return self.__primarylabel__ == other.__primarylabel__ and \
                       self.__primarykey__ == other.__primarykey__ and \
                       self.__primaryvalue__ == other.__primaryvalue__
            return (type(self) is type(other) and
                    self_node.graph == other_node.graph and
                    self_node.identity == other_node.identity)
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
        """ Convert a :class:`.Node` into a :class:`.GraphObject`.

        :param node:
        :return:
        """
        if node is None:
            return None
        inst = GraphObject()
        inst.__ogm = OGM(node)
        inst.__class__ = cls
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
        node = self.__node__
        primary_key = self.__primarykey__
        if primary_key == "__id__":
            return node.identity
        else:
            return node[primary_key]

    @property
    def __node__(self):
        """ The :class:`.Node` wrapped by this :class:`.GraphObject`.
        """
        return self.__ogm__.node

    def __db_create__(self, tx):
        self.__db_merge__(tx)

    def __db_delete__(self, tx):
        ogm = self.__ogm__
        tx.delete(ogm.node)
        for related_objects in ogm.all_related():
            related_objects.clear()

    def __db_exists__(self, tx):
        return tx.exists(self.__node__)

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
            for related_objects in ogm.all_related():
                related_objects.__db_push__(tx)

    def __db_pull__(self, tx):
        ogm = self.__ogm__
        if ogm.node.graph is None:
            matcher = GraphObjectMatcher(self.__class__, tx.graph)
            matcher._match_class = NodeMatch
            ogm.node = matcher.match(self.__primaryvalue__).first()
        tx.pull(ogm.node)
        for related_objects in ogm.all_related():
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
        for related_objects in ogm.all_related():
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
