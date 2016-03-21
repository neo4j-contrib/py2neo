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
from py2neo.types import Node, remote
from py2neo.util import label_case, relationship_case


class Property(object):

    def __init__(self, key=None):
        self.key = key

    def __get__(self, instance, owner):
        return instance.__db_node__[self.key]

    def __set__(self, instance, value):
        instance.__db_node__[self.key] = value


class Label(object):

    def __init__(self, name=None):
        self.name = name

    def __get__(self, instance, owner):
        return instance.__db_node__.has_label(self.name)

    def __set__(self, instance, value):
        if value:
            instance.__db_node__.add_label(self.name)
        else:
            instance.__db_node__.remove_label(self.name)


class Related(object):

    __instance__ = None

    def __init__(self, type=None):
        self.type = type

    @property
    def items(self):
        return self.__instance__.__db_relationships__.setdefault(self.type, set())

    def add(self, value):
        if isinstance(value, GraphObject):
            self.items.add(value)
        else:
            raise TypeError("Related items must be GraphObject instances")

    def remove(self, value):
        if isinstance(value, GraphObject):
            self.items.remove(value)
        else:
            raise TypeError("Related items must be GraphObject instances")


class GraphObjectMeta(type):

    def __new__(mcs, name, bases, attributes):
        related = []
        for attr_name, attr in list(attributes.items()):
            if isinstance(attr, Property):
                attributes[attr_name] = Property(attr.key or attr_name)
            elif isinstance(attr, Label):
                attributes[attr_name] = Label(attr.name or label_case(attr_name))
            elif isinstance(attr, Related):
                attributes[attr_name] = Related(attr.type or relationship_case(attr_name))
                related.append(attributes[attr_name])
        attributes.setdefault("__primary_label__", name)
        attributes.setdefault("__primary_key__", "__id__")
        instance = super().__new__(mcs, name, bases, attributes)
        for attr in related:
            attr.__instance__ = instance
        return instance


class GraphObject(metaclass=GraphObjectMeta):
    __primary_label__ = None
    __primary_key__ = None

    __db_node = None
    __db_relationships = None

    @classmethod
    def load(cls, graph, primary_value):
        # Label:key=value
        primary_key = cls.__primary_key__
        if primary_key == "__id__":
            node = graph.evaluate("MATCH (a:%s) WHERE id(a)={x}" %
                                  cypher_escape(cls.__primary_label__), x=primary_value)
        else:
            node = graph.find_one(cls.__primary_label__, primary_key, primary_value)
        if node is None:
            raise LookupError("Cannot load object")
        inst = GraphObject()
        inst.__db_node = node
        print(list(graph.match(node)))
        # TODO: load relationships
        inst.__class__ = cls
        return inst

    @property
    def __primary_value__(self):
        node = self.__db_node__
        primary_key = self.__primary_key__
        if primary_key == "__id__":
            remote_node = remote(node)
            if remote_node:
                return remote_node._id
            else:
                return None
        else:
            return node[primary_key]

    @property
    def __db_node__(self):
        if self.__db_node is None:
            self.__db_node = Node(self.__primary_label__)
        return self.__db_node

    @property
    def __db_relationships__(self):
        if self.__db_relationships is None:
            self.__db_relationships = {}
        return self.__db_relationships

    @property
    def __remote__(self):
        return self.__db_node__.__remote__

    def __db_create__(self, tx):
        tx.merge(self.__db_node__, self.__primary_label__, self.__primary_key__)

    def __db_delete__(self, tx):
        # TODO: delete if not bound
        tx.delete(self.__db_node__)

    def __db_pull__(self, graph):
        graph.pull(self.__db_node__)

    def __db_push__(self, graph):
        graph.push(self.__db_node__)


class Person(GraphObject):
    __primary_key__ = "name"

    name = Property()
    year_of_birth = Property(key="born")

    acted_in = Related("Movie")
    directed = Related("Movie")
    produced = Related("Movie")

    def __init__(self, name, year_of_birth):
        self.name = name
        self.year_of_birth = year_of_birth


class Movie(GraphObject):
    __primary_key__ = "title"

    title = Property()
    year_of_release = Property(key="released")

    def __init__(self, title, year_of_release):
        self.title = title
        self.year_of_release = year_of_release


def main():
    keanu = Person("Keanu Reeves", 1968)
    the_matrix = Movie("The Matrix", 1999)
    keanu.acted_in.add(the_matrix)
