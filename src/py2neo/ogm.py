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
"""

from __future__ import absolute_import, division, print_function, unicode_literals

from . import neo4j


def attach():
    pass

def detach():
    pass

def cast(value):
    return value

def uncast(value, cls):
    return cls(value)

def _export_properties(obj):
    props = {}
    for key, value in obj.__dict__.items():
        if not key.startswith("_"):
            props[key] = cast(value)
    return props

def _import_properties(obj, props):
    for key, value in props.items():
        if not key.startswith("_"):
            setattr(obj, key, value)


class Store(object):

    def __init__(self, graph_db):
        self.graph_db = graph_db

    def _load(self, cls, node):
        """ Load a specific node from the database into an object
            # MAKE ATOMIC!
        """
        obj = cls()
        setattr(obj, "__node__", node)
        setattr(obj, "__rel__", {})
        _import_properties(obj, node.get_properties())
        for rel in node.match():
            if rel.type not in obj.__rel__:
                obj.__rel__[rel.type] = []
            obj.__rel__[rel.type].append((rel.get_properties(), rel.end_node))
        return obj

    def _save(self, obj, node):
        """ Atomically save object to node.
        """
        obj.__node__ = node
        batch = neo4j.WriteBatch(self.graph_db)
        batch.set_node_properties(node, _export_properties(obj))
        # remove all outgoing relationships
        batch._post(batch._cypher_uri, {
            "query": (
                "START a=node({A}) "
                "MATCH (a)-[r]->(b) "
                "DELETE r"
            ),
            "params": {"A": node._id},
        })
        # rebuild outgoing relationships
        if hasattr(obj, "__rel__"):
            for rel_type, rels in obj.__rel__.items():
                for rel_props, end_node in rels:
                    end_node._must_belong_to(self.graph_db)
                    batch.create_relationship(node, rel_type, end_node, rel_props)
        batch._submit()

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
        return [self._load(cls, node) for node in nodes]

    def load_related(self, cls, obj, rel_type):
        if not hasattr(obj, "__rel__"):
            return []
        if rel_type not in obj.__rel__:
            return []
        return [
            self._load(cls, end_node)
            for rel_props, end_node in obj.__rel__[rel_type]
        ]

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
        return self._load(cls, nodes[0])

    def save_indexed(self, obj, index_name, key, value):
        index = self.graph_db.get_index(neo4j.Node, index_name)
        node = index.create(key, value, {})
        self._save(obj, node)

    def save_unique(self, obj, index_name, key, value):
        index = self.graph_db.get_index(neo4j.Node, index_name)
        node = index.get_or_create(key, value, {})
        self._save(obj, node)
