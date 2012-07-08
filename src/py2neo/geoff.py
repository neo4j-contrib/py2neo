#!/usr/bin/env python

# Copyright 2011 Nigel Small
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

"""
Geoff file handling (see `<http://geoff.nigelsmall.net/>`_).
"""

import sys
PY3K = sys.version_info[0] >= 3

__author__    = "Nigel Small <py2neo@nigelsmall.org>"
__copyright__ = "Copyright 2011 Nigel Small"
__license__   = "Apache License, Version 2.0"


try:
    import json
except ImportError:
    import simplejson as json
try:
    from . import neo4j
except ValueError:
    import neo4j
import re

try:
    from io import StringIO
except ImportError:
    from cStringIO import StringIO

import logging
logger = logging.getLogger(__name__)


NODE_DESCRIPTOR_PATTERN = re.compile(
    r"^\(([0-9A-Za-z_]+)\)$"
)
RELATIONSHIP_DESCRIPTOR_PATTERN = re.compile(
    r"^\(([0-9A-Za-z_]+)\)-\[([0-9A-Za-z_]*):([^\]]+)\]->\(([0-9A-Za-z_]+)\)$"
)

def _parse(string):
    """Convert Geoff string into abstract nodes and relationships.
    """
    nodes = []
    relationships = []
    for i, line in enumerate(string.splitlines()):
        if not line or line.startswith("#"):
            continue
        rule = re.split("\s+", line, 1)
        try:
            if len(rule) > 1:
                rule[1] = json.loads(rule[1])
        except TypeError:
            pass
        descriptor = str(rule[0])
        data = dict(rule[1]) if len(rule) > 1 else {}
        m = NODE_DESCRIPTOR_PATTERN.match(descriptor)
        if m:
            nodes.append((str(m.group(1)) or None, data))
            continue
        m = RELATIONSHIP_DESCRIPTOR_PATTERN.match(descriptor)
        if m:
            relationships.append((str(m.group(2)) or None, (
                str(m.group(1)), str(m.group(3)),
                str(m.group(4)), data,
            )))
            continue
        raise ValueError(descriptor)
    return nodes, relationships


class Subgraph(object):
    """Local, abstract representation of a graph portion.
    """

    NODE_KEY = 1
    RELATIONSHIP_KEY = 2

    def __init__(self, *entities):
        self._keys = []
        self._nodes = {}
        self._relationships = {}
        self._real_nodes = {}
        self._real_relationships = {}
        self.add(*entities)

    def __len__(self):
        return len(self._nodes) + len(self._relationships)

    def __str__(self):
        return self.dumps()

    def _add_abstract_node(self, abstract, key=None):
        if not key:
            key = len(self._nodes)
        key = str(key)
        self._keys.append((Subgraph.NODE_KEY, key))
        self._nodes[key] = abstract
        return key

    def _add_abstract_relationship(self, abstract, key=None):
        if not key:
            key = len(self._relationships)
        key = str(key)
        self._keys.append((Subgraph.RELATIONSHIP_KEY, key))
        self._relationships[key] = abstract
        return key

    def _merge_real_node(self, node):
        uri = str(node._uri)
        if uri not in self._real_nodes:
            self._real_nodes[uri] = self._add_abstract_node(node.get_properties())
        return self._real_nodes[uri]

    def _merge_real_relationship(self, relationship):
        uri = str(relationship._uri)
        if uri not in self._real_relationships:
            start_node = self._merge_real_node(relationship.start_node)
            end_node = self._merge_real_node(relationship.end_node)
            self._real_relationships[uri] = self._add_abstract_relationship((
                start_node, relationship.type, end_node,
                relationship.get_properties()
            ))
        return self._real_relationships[uri]

    @property
    def nodes(self):
        return self._nodes

    @property
    def relationships(self):
        return self._relationships

    def add(self, *entities):
        """Add nodes and relationships into this subgraph.
        """
        for entity in entities:
            if not entity:
                continue
            if isinstance(entity, list):
                self.add(*entity)
            elif isinstance(entity, (str, unicode)):
                nodes, rels = _parse(entity)
                for key, value in nodes:
                    self._add_abstract_node(value, key)
                for key, value in rels:
                    self._add_abstract_relationship(value, key)
            elif isinstance(entity, dict):
                self._add_abstract_node(entity)
            elif isinstance(entity, tuple):
                self._add_abstract_relationship(entity)
            elif isinstance(entity, neo4j.Node):
                self._merge_real_node(entity)
            elif isinstance(entity, neo4j.Relationship):
                self._merge_real_relationship(entity)
            elif isinstance(entity, neo4j.Path):
                self.add(*entity.nodes)
                self.add(*entity.relationships)
            elif isinstance(entity, Subgraph):
                self.add(*entity.nodes)
                self.add(*entity.relationships)
            else:
                raise TypeError(entity)

    def dump(self, file):
        """Dump Geoff rules from this subgraph into a file.
        """
        file.write(self.dumps())

    def dumps(self):
        """Dump Geoff rules from this subgraph into a string.
        """
        rules = []
        for type, key in self._keys:
            if type == Subgraph.NODE_KEY:
                abstract = self._nodes[key]
                rules.append("({0}) {1}".format(key, json.dumps(abstract)))
            elif type == Subgraph.RELATIONSHIP_KEY:
                abstract = self._relationships[key]
                if len(abstract) > 3:
                    data = json.dumps(abstract[3])
                else:
                    data = "{}"
                rules.append("({0})-[{1}:{2}]->({3}) {4}".format(
                    abstract[0], key, abstract[1], abstract[2], data
                ))
            else:
                raise ValueError("Unexpected rule type " + str(type))
        return "\n".join(rules)

    def load(self, file):
        """Load Geoff rules from a file into this subgraph.
        """
        self.add(file.read())

    def loads(self, str):
        """Load Geoff rules from a string into this subgraph.
        """
        self.add(str)

    def insert_into(self, graph_db, **params):
        """Insert this subgraph into a graph database via Geoff plugin.
        """
        try:
            uri = graph_db._extension_uri('GeoffPlugin', 'insert')
        except NotImplementedError:
            raise NotImplementedError("Geoff plugin not available for insert")
        response = graph_db._post(
            uri, {'subgraph': [self.dumps()], 'params': dict(params)}
        )
        return response['params']

    def merge_into(self, graph_db, **params):
        """Merge this subgraph into a graph database via Geoff plugin.
        """
        try:
            uri = graph_db._extension_uri('GeoffPlugin', 'merge')
        except NotImplementedError:
            raise NotImplementedError("Geoff plugin not available for merge")
        response = graph_db._post(
            uri, {'subgraph': [self.dumps()], 'params': dict(params)}
        )
        return response['params']

    def delete_from(self, graph_db, **params):
        """Delete this subgraph from a graph database via Geoff plugin.
        """
        try:
            uri = graph_db._extension_uri('GeoffPlugin', 'delete')
        except NotImplementedError:
            raise NotImplementedError("Geoff plugin not available for delete")
        response = graph_db._post(
            uri, {'subgraph': [self.dumps()], 'params': dict(params)}
        )
        return response['params']
