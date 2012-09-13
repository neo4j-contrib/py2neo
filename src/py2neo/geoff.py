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

"""
The :py:mod:`py2neo.geoff` module deals with Geoff data handling.

All Geoff functionality is focused around the :py:class:`Subgraph` class and
requires the Geoff server plugin to be installed (see
`<http://geoff.nigelsmall.net/>`_). A subgraph is a local, abstract
representation of a portion of graph data and may be used to build up a data
structure within a client application before submitting it to a database server
in a single request which can act to reduce the amount of network traffic
carried out.

The following example shows how to build a simple client-side graph and submit
it to the database server for insertion::

    >>> from py2neo import geoff
    >>> s = geoff.Subgraph({"name": "Alice"}, {"name": "Bob"}, (0, "KNOWS", 1)}
    >>> s.insert_into(graph_db)

"""

import json
import re

try:
    from io import StringIO
except ImportError:
    from cStringIO import StringIO

from . import neo4j, rest, compat

import logging
logger = logging.getLogger(__name__)


UNKNOWN, NODE, RELATIONSHIP = 0x00, 0x01, 0x02

PATTERNS = {
    NODE: re.compile(
        r"^\(([0-9A-Za-z_]+)\)$"
    ),
    RELATIONSHIP: re.compile(
        r"^\(([0-9A-Za-z_]+)\)-\[([0-9A-Za-z_]*):([^\]]+)\]->\(([0-9A-Za-z_]+)\)$"
    ),
}

def _parse(string):
    """Convert Geoff string into abstract nodes and relationships.
    """
    rules = []
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
        m = PATTERNS[NODE].match(descriptor)
        if m:
            rules.append((NODE, str(m.group(1)) or None, data))
            continue
        m = PATTERNS[RELATIONSHIP].match(descriptor)
        if m:
            rules.append((RELATIONSHIP, str(m.group(2)) or None, (
                str(m.group(1)), str(m.group(3)),
                str(m.group(4)), data,
            )))
            continue
        rules.append((UNKNOWN, None, (descriptor, data)))
    return rules


class Subgraph(object):
    """Local, abstract representation of a graph portion.
    """

    def __init__(self, *items):
        self._keys = []
        self._nodes = {}
        self._relationships = {}
        self._unknowns = []
        self._real_nodes = {}
        self._real_relationships = {}
        self.add(*items)

    def __len__(self):
        return len(self._nodes) + len(self._relationships)

    def __str__(self):
        return self.dumps()

    def _add_abstract_node(self, abstract, key=None):
        if not key:
            key = len(self._nodes)
        key = str(key)
        self._keys.append((NODE, key))
        self._nodes[key] = abstract
        return key

    def _add_abstract_relationship(self, abstract, key=None):
        if not key:
            key = len(self._relationships)
        key = str(key)
        self._keys.append((RELATIONSHIP, key))
        self._relationships[key] = abstract
        return key

    def _add_unknown_abstract(self, abstract):
        key = len(self._unknowns)
        self._keys.append((UNKNOWN, key))
        self._unknowns.append(abstract)
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
        """Return all nodes within this Subgraph.
        """
        return self._nodes

    @property
    def relationships(self):
        """Return all relationships within this Subgraph.
        """
        return self._relationships

    def add(self, *items):
        """Add nodes and relationships into this subgraph.
        This method will attempt to take the most appropriate action depending
        on the type of data supplied. Supported types are treated according to
        the list below:

        :py:const:`list`
            a sub-list of items; these will be added recursively

        :py:const:`str` or :py:const:`unicode`
            a textual Geoff rule (e.g. `'(A) {"name": "Alice"}'`)

        :py:const:`dict`
            an abstract node representation (e.g. `{u'name': u'Alice'}`)

        :py:const:`tuple`
            an abstract relationship representation (e.g. `(0, 'KNOWS', 1)`);
            the start and end node references may be numeric or textual and
            should refer to nodes within the same subgraph

        :py:class:`py2neo.neo4j.Node`
            a concrete node object

        :py:class:`py2neo.neo4j.Relationship`
            a concrete relationship object

        :py:class:`py2neo.neo4j.Path`
            a path object; all nodes and relationships will be added

        :py:class:`py2neo.geoff.Subgraph`
            a subgraph object; all nodes and relationships will be added

        """
        for item in items:
            if not item:
                continue
            if isinstance(item, list):
                self.add(*item)
            elif compat.is_string(item):
                rules = _parse(item)
                for type, key, abstract in rules:
                    if type == NODE:
                        self._add_abstract_node(abstract, key)
                    elif type == RELATIONSHIP:
                        self._add_abstract_relationship(abstract, key)
                    else:
                        self._add_unknown_abstract(abstract)
            elif isinstance(item, dict):
                self._add_abstract_node(item)
            elif isinstance(item, tuple):
                self._add_abstract_relationship(item)
            elif isinstance(item, neo4j.Node):
                self._merge_real_node(item)
            elif isinstance(item, neo4j.Relationship):
                self._merge_real_relationship(item)
            elif isinstance(item, neo4j.Path):
                self.add(*item.nodes)
                self.add(*item.relationships)
            elif isinstance(item, Subgraph):
                self.add(*item.nodes)
                self.add(*item.relationships)
            else:
                raise TypeError(item)

    def dump(self, file):
        """Dump Geoff rules from this subgraph into a file.
        """
        file.write(self.dumps())

    def dumps(self):
        """Dump Geoff rules from this subgraph into a string.
        """
        rules = []
        for type, key in self._keys:
            if type == NODE:
                abstract = self._nodes[key]
                rules.append("({0}) {1}".format(key, json.dumps(abstract)))
            elif type == RELATIONSHIP:
                abstract = self._relationships[key]
                if len(abstract) > 3:
                    data = json.dumps(abstract[3])
                else:
                    data = "{}"
                rules.append("({0})-[{1}:{2}]->({3}) {4}".format(
                    abstract[0], key, abstract[1], abstract[2], data
                ))
            else:
                abstract = self._unknowns[key]
                rules.append("{0} {1}".format(abstract[0], json.dumps(abstract[1])))
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
        rs = graph_db._send(
            rest.Request(graph_db, "POST", uri, {'subgraph': [self.dumps()], 'params': dict(params)}
        ))
        return rs.body['params']

    def merge_into(self, graph_db, **params):
        """Merge this subgraph into a graph database via Geoff plugin.
        """
        try:
            uri = graph_db._extension_uri('GeoffPlugin', 'merge')
        except NotImplementedError:
            raise NotImplementedError("Geoff plugin not available for merge")
        rs = graph_db._send(
            rest.Request(graph_db, "POST", uri, {'subgraph': [self.dumps()], 'params': dict(params)}
        ))
        return rs.body['params']

    def delete_from(self, graph_db, **params):
        """Delete this subgraph from a graph database via Geoff plugin.
        """
        try:
            uri = graph_db._extension_uri('GeoffPlugin', 'delete')
        except NotImplementedError:
            raise NotImplementedError("Geoff plugin not available for delete")
        rs = graph_db._send(
            rest.Request(graph_db, "POST", uri, {'subgraph': [self.dumps()], 'params': dict(params)}
        ))
        return rs.body['params']
