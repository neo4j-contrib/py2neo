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

""" The ogm (pronounced "Ogham") module provides Object to Graph Mapping
    features similar to ORM facilities available for relational databases.

    This module provides a :py:class:`Store` class which is the core of all
    object storage. The methods provided allow standard Python objects to be
    persisted and retrieved using save and load calls respectively::

        from py2neo import neo4j, ogm

        class Person(object):

            def __init__(self, email=None, name=None, age=None):
                self.email = email
                self.name = name
                self.age = age

        graph_db = neo4j.GraphDatabaseService()
        store = ogm.Store(graph_db)

        alice = Person("alice@example,com", "Alice Allison", 34)
        store.save_unique(alice, "People", "email", alice.email)

        bob = Person("bob@example,org", "Bob Robertson", 66)
        store.save_unique(bob, "People", "email", bob.email)

        carol = Person("carol@example,net", "Carol Carlsson", 42)
        store.save_unique(carol, "People", "email", carol.email)

        ogm.attach(alice, "LIKES", bob)
        ogm.attach(alice, "DISLIKES", carol)
        store.save(alice)

        ogm.attach(bob, "LIKES", alice)
        ogm.attach(bob, "LIKES", carol)
        store.save(bob)

        ogm.attach(carol, "LIKES", bob)
        store.save(carol)

"""

from __future__ import absolute_import, division, print_function, unicode_literals

from . import neo4j, cypher


def _export_properties(obj):
    # cast object attributes to neo4j-compatible properties
    props = {}
    for key, value in obj.__dict__.items():
        if not key.startswith("_"):
            # cast value
            props[key] = value
    return props

def _import_properties(subj, props):
    # write neo4j properties to object attributes
    for key, value in props.items():
        if not key.startswith("_"):
            # uncast value to key type
            setattr(subj, key, value)


class Store(object):

    def __init__(self, graph_db):
        self.graph_db = graph_db

    def relate(self, subj, rel_type, obj, properties=None):
        if not hasattr(obj, "__node__"):
            self.save(obj)
        if not hasattr(subj, "__rel__"):
            subj.__rel__ = {}
        if rel_type not in subj.__rel__:
            subj.__rel__[rel_type] = []
        subj.__rel__[rel_type].append((properties or {}, obj.__node__))

    def divorce(self, subj, rel_type):
        # make rel_type optional and add optional obj?
        try:
            del subj.__rel__[rel_type]
        except AttributeError:
            pass
        except KeyError:
            pass

    def load_related(self, subj, rel_type, cls):
        if not hasattr(subj, "__rel__"):
            return []
        if rel_type not in subj.__rel__:
            return []
        return [
            self.load(cls(), end_node)
            for rel_props, end_node in subj.__rel__[rel_type]
        ]

    def load(self, obj, node=None):
        """ Load data from a database node into a local object. If the `node`
        parameter is not specified, an existing `__node__` attribute will be
        used instead, if available. This (will be) an atomic operation, carried out
        in a single Neo4j batch.

        :param obj: the object to load into
        :param node: the node to load from
        :return: the object
        :raise TypeError: if no `node` specified and
            no `__node__` attribute found
        """
        # MAKE ATOMIC!
        if node is not None:
            setattr(obj, "__node__", node)
        if hasattr(obj, "__node__"):
            node = obj.__node__
        else:
            node, = self.graph_db.create({})
            obj.__node__ = node
        setattr(obj, "__rel__", {})
        _import_properties(obj, node.get_properties())
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
        """ Save object to node.
        """
        if node is not None:
            obj.__node__ = node
        # write properties
        props = _export_properties(obj)
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
                for rel_props, end_node in rels:
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
