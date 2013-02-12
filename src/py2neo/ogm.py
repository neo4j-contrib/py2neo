#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright 2011-2012 Nigel Small
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

""" The ogm module provides Object to Graph Mapping features similar to ORM
facilities available for relational databases. All functionality is available
through the :py:class:`Store` class which is bound to a specific
:py:class:`neo4j.GraphDatabaseService` instance on creation.

Conceptually, a mapped object "owns" a single node within the graph along with
all of that node's outgoing relationships. These features are managed via a
pair of attributes called `__node__` and `__rel__` which store details of the
mapped node and the outgoing relationships respectively. There are specific
requirements for a mapped object except for a nullary constructor which can be
used to create new instances.

The `__node__` attribute holds a :py:class:`neo4j.Node` object which is the
node to which this object is mapped. If the attribute does not exist, or is
:py:const:`None`, the object is considered "unsaved".

The `__rel__` attribute holds a dictionary of outgoing relationship details.
Each key corresponds to a relationship type and each value to a list of
2-tuples representing the outgoing relationships of that type. Within each
2-tuple, the first value holds a dictionary of relationship properties (which
may be empty) and the second value holds the endpoint. The endpoint may be
either a :py:class:`neo4j.Node` instance or another mapped object. Any such
objects which are unsaved will be lazily saved as required by creation of the
relationship itself. The following data structure outline shows an example of
a `__rel__` attribute (where `alice` and `bob` represent other mapped objects::

    {
        "LIKES": [
            ({}, alice),
            ({"since": 1999}, bob)
        ]
    }

To manage relationships, use the :py:func:`Store.relate` and
:py:func:`Store.separate` methods. Neither method makes any calls to the
database and operates only on the local `__rel__` attribute. Changes must be
explicitly saved via one of the available save methods.

The code below shows a quick example of usage::

    from py2neo import neo4j, ogm

    class Person(object):

        def __init__(self, email=None, name=None, age=None):
            self.email = email
            self.name = name
            self.age = age

        def __str__(self):
            return self.name

    graph_db = neo4j.GraphDatabaseService()
    store = ogm.Store(graph_db)

    alice = Person("alice@example,com", "Alice", 34)
    store.save_unique(alice, "People", "email", alice.email)

    bob = Person("bob@example,org", "Bob", 66)
    carol = Person("carol@example,org", "Carol", 42)
    store.relate(alice, "LIKES", bob)
    store.relate(alice, "LIKES", carol)
    store.save(alice)

    friends = store.load_related(alice, "LIKES", Person)
    print("Alice likes {0}".format(" and ".join(str(f) for f in friends)))

"""

from __future__ import absolute_import, unicode_literals

from . import neo4j, cypher


class Store(object):

    def __init__(self, graph_db):
        self.graph_db = graph_db

    def _get_node(self, endpoint):
        if isinstance(endpoint, neo4j.Node):
            return endpoint
        if not hasattr(endpoint, "__node__"):
            self.save(endpoint)
        return endpoint.__node__

    def _is_same(self, obj, endpoint):
        if isinstance(endpoint, neo4j.Node):
            if hasattr(obj, "__node__"):
                return endpoint == obj.__node__
            else:
                return False
        else:
            return endpoint is obj

    def relate(self, subj, rel_type, obj, properties=None):
        """ Define a `rel_type` relationship between `subj` and `obj`. This
        is a local operation only: nothing is saved to the database until a
        save method is called. Relationship properties may optionally be
        specified.
        """
        if not hasattr(subj, "__rel__"):
            subj.__rel__ = {}
        if rel_type not in subj.__rel__:
            subj.__rel__[rel_type] = []
        subj.__rel__[rel_type].append((properties or {}, obj))

    def separate(self, subj, rel_type, obj=None):
        """ Remove any relationship definitions which match the criteria
        specified. This is a local operation only: nothing is saved to the
        database until a save method is called. If no object is specified, all
        relationships of type `rel_type` are removed.
        """
        if not hasattr(subj, "__rel__"):
            return
        if rel_type not in subj.__rel__:
            return
        if obj is None:
            del subj.__rel__[rel_type]
        else:
            subj.__rel__[rel_type] = [
                (props, endpoint)
                for props, endpoint in subj.__rel__[rel_type]
                if not self._is_same(obj, endpoint)
            ]

    def load_related(self, subj, rel_type, cls):
        if not hasattr(subj, "__rel__"):
            return []
        if rel_type not in subj.__rel__:
            return []
        return [
            self.load(cls(), self._get_node(endpoint))
            for rel_props, endpoint in subj.__rel__[rel_type]
        ]

    def load(self, obj, node=None):
        """ Load data from a database node into a local object. If the `node`
        parameter is not specified, an existing `__node__` attribute will be
        used instead, if available.

        :param obj: the object to load into
        :param node: the node to load from
        :return: the object
        :raise TypeError: if no `node` specified and no `__node__`
            attribute found
        """
        if node is not None:
            setattr(obj, "__node__", node)
        if hasattr(obj, "__node__"):
            node = obj.__node__
        else:
            node, = self.graph_db.create({})
            obj.__node__ = node
        setattr(obj, "__rel__", {})
        # naively copy properties from node to object
        for key, value in node.get_properties().items():
            if not key.startswith("_"):
                setattr(obj, key, value)
        for rel in node.match():
            if rel.type not in obj.__rel__:
                obj.__rel__[rel.type] = []
            obj.__rel__[rel.type].append((rel.get_properties(), rel.end_node))
        return obj

    def load_indexed(self, cls, index_name, key, value):
        """ Load zero or more indexed nodes from the database into a list of
        objects.

        :param cls:
        :param index_name:
        :param key:
        :param value:
        :return: a list of `cls` instances
        """
        index = self.graph_db.get_index(neo4j.Node, index_name)
        nodes = index.get(key, value)
        return [self.load(cls(), node) for node in nodes]

    def load_unique(self, cls, index_name, key, value):
        """ Load a uniquely indexed node from the database into an object.

        :param cls:
        :param index_name:
        :param key:
        :param value:
        :return: as instance of `cls` containing the loaded data
        """
        index = self.graph_db.get_index(neo4j.Node, index_name)
        nodes = index.get(key, value)
        if not nodes:
            return None
        if len(nodes) > 1:
            raise LookupError("Multiple nodes match the given criteria; "
                              "consider using `load_all` instead.")
        return self.load(cls(), nodes[0])

    def save(self, obj, node=None):
        """ Save an object to a database node.
        """
        if node is not None:
            obj.__node__ = node
        # naively copy properties from object to node
        props = {}
        for key, value in obj.__dict__.items():
            if not key.startswith("_"):
                props[key] = value
        if hasattr(obj, "__node__"):
            obj.__node__.set_properties(props)
            cypher.execute(self.graph_db, "START a=node({A}) "
                                          "MATCH (a)-[r]->(b) "
                                          "DELETE r", {"A": obj.__node__._id})
        else:
            obj.__node__, = self.graph_db.create(props)
        # write rels
        if hasattr(obj, "__rel__"):
            batch = neo4j.WriteBatch(self.graph_db)
            for rel_type, rels in obj.__rel__.items():
                for rel_props, endpoint in rels:
                    end_node = self._get_node(endpoint)
                    end_node._must_belong_to(self.graph_db)
                    batch.create_relationship(obj.__node__, rel_type, end_node, rel_props)
            batch._submit()
        return obj

    def save_indexed(self, obj_list, index_name, key, value):
        index = self.graph_db.get_index(neo4j.Node, index_name)
        for obj in obj_list:
            index.add(key, value, self.save(obj))

    def save_unique(self, obj, index_name, key, value):
        index = self.graph_db.get_index(neo4j.Node, index_name)
        node = index.get_or_create(key, value, {})
        self.save(obj, node)
