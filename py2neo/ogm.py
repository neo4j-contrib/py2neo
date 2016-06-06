#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2016, Nigel Small
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


from py2neo.database import cypher_escape
from py2neo.types import Node, Relationship, Subgraph, remote, PropertyDict
from py2neo.util import label_case, relationship_case, metaclass


class Property(object):

    def __init__(self, key=None):
        self.key = key

    def __get__(self, instance, owner):
        return instance.__cog__.subject_node[self.key]

    def __set__(self, instance, value):
        instance.__cog__.subject_node[self.key] = value


class Label(object):

    def __init__(self, name=None):
        self.name = name

    def __get__(self, instance, owner):
        return instance.__cog__.subject_node.has_label(self.name)

    def __set__(self, instance, value):
        if value:
            instance.__cog__.subject_node.add_label(self.name)
        else:
            instance.__cog__.subject_node.remove_label(self.name)


class Related(object):
    """ Represents a set of relationships from a single start node to
    (potentially) multiple end nodes.
    """

    def __init__(self, related_class, relationship_type=None):
        self.related_class = related_class
        self.relationship_type = relationship_type

    def __get__(self, instance, owner):
        cog = instance.__cog__
        if not cog.is_defined(self.relationship_type):
            if isinstance(self.related_class, type):
                related_class = self.related_class
            else:
                module_name, _, class_name = self.related_class.rpartition(".")
                if not module_name:
                    module_name = instance.__class__.__module__
                module = __import__(module_name, fromlist=".")
                related_class = getattr(module, class_name)
            cog.define_related(self.relationship_type, related_class)
        return cog.related[self.relationship_type]


class RelatedObjects(object):
    """ A set of similarly-typed objects that are all related to a
    central object in a similar way.
    """

    def __init__(self, subject_node, relationship_type, related_class):
        self.subject_node = subject_node
        self.relationship_type = relationship_type
        self.related_class = related_class
        self.__related_objects = []

    def __iter__(self):
        for obj, _ in self.__related_objects:
            yield obj

    def __len__(self):
        return len(self.__related_objects)

    def __contains__(self, obj):
        if not isinstance(obj, GraphObject):
            obj = self.related_class.find_one(obj)
        for related_object, _ in self.__related_objects:
            if related_object == obj:
                return True
        return False

    def add(self, obj, properties=None, **kwproperties):
        """ Add a related object.

        :param obj: the :py:class:`.GraphObject` to relate
        :param properties: dictionary of properties to attach to the relationship (optional)
        :param kwproperties: additional keyword properties (optional)
        """
        properties = PropertyDict(properties or {}, **kwproperties)
        added = False
        for i, (related_object, _) in enumerate(self.__related_objects):
            if related_object == obj:
                self.__related_objects[i] = (obj, properties)
                added = True
        if not added:
            self.__related_objects.append((obj, properties))

    def clear(self):
        self.__related_objects.clear()

    def get(self, obj, key, default=None):
        for related_object, properties in self.__related_objects:
            if related_object == obj:
                return properties.get(key, default)
        return default

    def remove(self, obj):
        """ Remove a related object

        :param obj: the :py:class:`.GraphObject` to separate
        """
        self.__related_objects = [(related_object, properties)
                                  for related_object, properties in self.__related_objects
                                  if related_object != obj]

    def update(self, obj, properties=None, **kwproperties):
        """ Add or update a related object.

        :param obj: the :py:class:`.GraphObject` to relate
        :param properties: dictionary of properties to attach to the relationship (optional)
        :param kwproperties: additional keyword properties (optional)
        """
        properties = dict(properties or {}, **kwproperties)
        added = False
        for i, (related_object, p) in enumerate(self.__related_objects):
            if related_object == obj:
                self.__related_objects[i] = (obj, PropertyDict(p, **properties))
                added = True
        if not added:
            self.__related_objects.append((obj, properties))

    # def __db_create__(self, tx):
    #     self.__db_merge__(tx)

    # def __db_delete__(self, tx):
    #     escaped_relationship_type = cypher_escape(self.relationship_type)
    #     subject_id = remote(self.subject_node)._id
    #     tx.run("MATCH (a)-[r:%s]->(b) WHERE id(a) = {x} DELETE r" % escaped_relationship_type,
    #            x=subject_id, y=[remote(obj.__cog__.subject_node)._id for obj, _ in self.__related_objects])

    # def __db_merge__(self, tx, primary_label=None, primary_key=None):
    #     # 1. merge all nodes (create ones that don't)
    #     for related_object, _ in self.__related_objects:
    #         tx.merge(related_object)
    #     tx.process()
    #     # 2a. remove any relationships not in list of nodes
    #     escaped_relationship_type = cypher_escape(self.relationship_type)
    #     subject_id = remote(self.subject_node)._id
    #     tx.run("MATCH (a)-[r:%s]->(b) WHERE id(a) = {x} AND NOT id(b) IN {y} DELETE r" % escaped_relationship_type,
    #            x=subject_id, y=[remote(obj.__cog__.subject_node)._id for obj, _ in self.__related_objects])
    #     # 2b. merge all relationships
    #     for related_object, properties in self.__related_objects:
    #         tx.run("MATCH (a) WHERE id(a) = {x} MATCH (b) WHERE id(b) = {y} "
    #                "MERGE (a)-[r:%s]->(b) SET r = {z}" % escaped_relationship_type,
    #                x=subject_id, y=remote(related_object.__cog__.subject_node)._id, z=properties)

    def __db_pull__(self, graph):
        related_objects = []
        for r in graph.match(self.subject_node, self.relationship_type):
            related_object = self.related_class.wrap(r.end_node())
            related_objects.append((related_object, PropertyDict(r)))
        self.__related_objects[:] = related_objects

    def __db_push__(self, graph):
        tx = graph.begin()
        # 1. merge all nodes (create ones that don't)
        for related_object, _ in self.__related_objects:
            tx.merge(related_object)
        tx.process()
        # 2a. remove any relationships not in list of nodes
        escaped_relationship_type = cypher_escape(self.relationship_type)
        subject_id = remote(self.subject_node)._id
        tx.run("MATCH (a)-[r:%s]->(b) WHERE id(a) = {x} AND NOT id(b) IN {y} DELETE r" % escaped_relationship_type,
               x=subject_id, y=[remote(obj.__cog__.subject_node)._id for obj, _ in self.__related_objects])
        # 2b. merge all relationships
        for related_object, properties in self.__related_objects:
            tx.run("MATCH (a) WHERE id(a) = {x} MATCH (b) WHERE id(b) = {y} "
                   "MERGE (a)-[r:%s]->(b) SET r = {z}" % escaped_relationship_type,
                   x=subject_id, y=remote(related_object.__cog__.subject_node)._id, z=properties)
        tx.commit()


class Cog(object):
    """ A central node plus a set of RelatedObjects instances.
    """

    def __init__(self, subject_node):
        self.subject_node = subject_node
        self.related = {}  # key=relationship_type, value=related_objects

    def __eq__(self, other):
        try:
            return self.subject_node == other.subject_node
        except AttributeError:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def is_defined(self, relationship_type):
        return relationship_type in self.related

    def define_related(self, relationship_type, related_class):
        related_objects = RelatedObjects(self.subject_node, relationship_type, related_class)
        self.related[relationship_type] = related_objects

    def add(self, relationship_type, obj, properties=None, **kwproperties):
        self.related[relationship_type].add(obj, properties, **kwproperties)

    def get(self, relationship_type, obj, key, default=None):
        return self.related[relationship_type].get(obj, key, default)

    def remove(self, relationship_type, obj):
        self.related[relationship_type].remove(obj)

    def update(self, relationship_type, obj, properties=None, **kwproperties):
        self.related[relationship_type].update(obj, properties, **kwproperties)

    def __db_create__(self, tx):
        self.__db_merge__(tx)

    def __db_delete__(self, tx):
        # TODO make atomic
        graph = tx.graph
        for related_objects in self.related.values():
            related_objects.clear()
            related_objects.__db_push__(graph)
        graph.delete(self.subject_node)

    def __db_merge__(self, tx, primary_label=None, primary_key=None):
        # TODO make atomic
        graph = tx.graph
        remote_node = remote(self.subject_node)
        if not remote_node:
            graph.merge(self.subject_node, primary_label, primary_key)
            for related_objects in self.related.values():
                related_objects.__db_push__(graph)

    def __db_pull__(self, graph):
        remote_node = remote(self.subject_node)
        if remote_node:
            graph.pull(self.subject_node)
        else:
            graph.merge(self.subject_node)
        for related_objects in self.related.values():
            related_objects.__db_pull__(graph)

    def __db_push__(self, graph):
        remote_node = remote(self.subject_node)
        if remote_node:
            graph.push(self.subject_node)
        else:
            graph.merge(self.subject_node)
        for related_objects in self.related.values():
            related_objects.__db_push__(graph)


class GraphObjectType(type):

    def __new__(mcs, name, bases, attributes):
        for attr_name, attr in list(attributes.items()):
            if isinstance(attr, Property):
                if attr.key is None:
                    attr.key = attr_name
            elif isinstance(attr, Label):
                if attr.name is None:
                    attr.name = label_case(attr_name)
            elif isinstance(attr, Related):
                if attr.relationship_type is None:
                    attr.relationship_type = relationship_case(attr_name)
        attributes.setdefault("__primarylabel__", name)
        attributes.setdefault("__primarykey__", "__id__")
        return super(GraphObjectType, mcs).__new__(mcs, name, bases, attributes)


@metaclass(GraphObjectType)
class GraphObject(object):
    __graph__ = None
    __primarylabel__ = None
    __primarykey__ = None

    __cog = None

    def __eq__(self, other):
        if not isinstance(other, GraphObject):
            return False
        remote_self = remote(self.__cog__.subject_node)
        remote_other = remote(other.__cog__.subject_node)
        if remote_self and remote_other:
            return remote_self == remote_other
        else:
            return self.__primarylabel__ == other.__primarylabel__ and \
                   self.__primarykey__ == other.__primarykey__ and \
                   self.__primaryvalue__ == other.__primaryvalue__

    def __ne__(self, other):
        return not self.__eq__(other)

    @property
    def __cog__(self):
        if self.__cog is None:
            self.__cog = Cog(Node(self.__primarylabel__))
        node = self.__cog.subject_node
        if not hasattr(node, "__primarylabel__"):
            setattr(node, "__primarylabel__", self.__primarylabel__)
        if not hasattr(node, "__primarykey__"):
            setattr(node, "__primarykey__", self.__primarykey__)
        return self.__cog

    @classmethod
    def wrap(cls, node):
        if node is None:
            return None
        inst = GraphObject()
        inst.__cog = cog = Cog(node)
        inst.__class__ = cls
        for attr in dir(inst):
            _ = getattr(inst, attr)
        remote_node = remote(node)
        if remote_node:
            # TODO: bootstrap define_related (somehow)
            cog.__db_pull__(remote_node.graph)
        return inst

    @classmethod
    def find_one(cls, primary_value):
        graph = cls.__graph__
        if graph is None:
            raise RuntimeError("No graph database defined for %s" % cls.__name__)
        primary_key = cls.__primarykey__
        if primary_key == "__id__":
            node = graph.evaluate("MATCH (a:%s) WHERE id(a)={x} RETURN a" %
                                  cypher_escape(cls.__primarylabel__), x=primary_value)
        else:
            node = graph.find_one(cls.__primarylabel__, primary_key, primary_value)
        return cls.wrap(node)

    @classmethod
    def find(cls, primary_values):
        graph = cls.__graph__
        primary_key = cls.__primarykey__
        if primary_key == "__id__":
            for record in graph.run("MATCH (a:%s) WHERE id(a) IN {x} RETURN a" %
                                    cypher_escape(cls.__primarylabel__), x=list(primary_values)):
                yield cls.wrap(record["a"])
        else:
            for node in graph.find(cls.__primarylabel__, primary_key, tuple(primary_values)):
                yield cls.wrap(node)

    def __repr__(self):
        return "<%s %s>" % (self.__class__.__name__,
                            " ".join("%s=%r" % (k, getattr(self, k)) for k in dir(self)
                                     if not k.startswith("_") and not callable(getattr(self, k))))

    @property
    def __primaryvalue__(self):
        node = self.__cog__.subject_node
        primary_key = self.__primarykey__
        if primary_key == "__id__":
            remote_node = remote(node)
            if remote_node:
                return remote_node._id
            else:
                return None
        else:
            return node[primary_key]

    def __db_create__(self, tx):
        self.__cog__.__db_create__(tx)

    def __db_delete__(self, tx):
        self.__cog__.__db_delete__(tx)

    def __db_merge__(self, tx, primary_label=None, primary_key=None):
        self.__cog__.__db_merge__(tx, primary_label, primary_key)

    def __db_pull__(self, graph):
        self.__cog__.__db_pull__(graph)

    def __db_push__(self, graph):
        self.__cog__.__db_push__(graph)
