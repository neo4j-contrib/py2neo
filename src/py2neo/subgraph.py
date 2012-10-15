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

""" The :py:mod:`py2neo.subgraph` module deals with abstract subgraph data
    encoded in Geoff jr. For example::

        abstract = {
            "People": {
                "__uniquekey__": "email",
                "__nodes__": {
                    "alice": {"name": "Alice", "email": "alice@example.com"},
                    "bob":   {"name": "Bob", "email": "bob@example.com"}
                },
                "__rels__": [
                    ["alice", "KNOWS", "bob"]
                ]
            }
        }
        nodes = subgraph.merge(abstract, self.graph_db)
        alice, bob = nodes["People"]["alice"], nodes["People"]["bob"]

"""

import logging
logger = logging.getLogger(__name__)

from . import neo4j

CATEGORY_DELIMITER = ":"

class Subgraph(object):

    class Category(object):

        def __init__(self, name, data):
            self.name = name
            self.unique_key = data.get("__uniquekey__", None)
            self.nodes = {}
            self.rels = []
            if "__nodes__" in data:
                for key, value in data["__nodes__"].items():
                    self.nodes[key] = dict(value or {})
            if "__rels__" in data:
                for value in data["__rels__"]:
                    self.rels.append(tuple(value))

        def _qualify(self, name):
            name = name.rpartition(CATEGORY_DELIMITER)
            if name[0]:
                name = (name[0], name[2])
            else:
                name = (self.name, name[2])
            return name

        def _rel_abstract(self, abstract):
            if len(abstract) == 3:
                (start, type_, end), properties = abstract, {}
            elif len(abstract) >=4:
                start, type_, end, properties = abstract[0:4]
            else:
                raise ValueError("Broken relationship abstract")
            return self._qualify(start), type_, self._qualify(end), properties

        @property
        def unique_nodes(self):
            """ Return all nodes which contain the key designated as unique.
            """
            return dict([
                (name, abstract)
                for name, abstract in self.nodes.items()
                if self.unique_key in abstract
            ])

    def __init__(self, abstract):
        self.categories = {}
        for key, value in abstract.items():
            self.categories[key] = Subgraph.Category(key, value)

    @property
    def has_rels(self):
        for category in self.categories.values():
            if category.rels:
                return True
        return False

    @property
    def nodes(self):
        nodes = {}
        for category_name, category in self.categories.items():
            for node_name, node_abstract in category.nodes.items():
                nodes[(category_name, node_name)] = node_abstract
        return nodes

    def _create_relationship_query(self, input_nodes):
        # unique_nodes is an array of category/node name tuples
        start_clause, create_clause, output_nodes, params = [], [], [], {}
        #
        def node_pattern(i, name):
            if name in input_nodes:
                return "(in{0})".format(input_nodes.index(name))
            elif name in output_nodes:
                return "(out{0})".format(output_nodes.index(name))
            else:
                try:
                    params["sp" + str(i)] = self.categories[name[0]].nodes[name[1]]
                except KeyError:
                    raise KeyError("Node '{1}' in category '{0}' not found".format(*name))
                output_nodes.append(name)
                return "(out{0} {{{1}}})".format(output_nodes.index(name), "sp" + str(i))
            #
        def rel_pattern(i, type_, properties):
            if properties:
                params["rp" + str(i)] = properties
                return "-[:`{0}` {{{1}}}]->".format(type_, "rp" + str(i))
            else:
                return "-[:`{0}`]->".format(type_)
            #
        # build start clause from unique nodes
        for i, (category_name, node_name) in enumerate(input_nodes):
            category = self.categories[category_name]
            start_clause.append("in{0}=node:{1}(`{2}`={{val{0}}})".format(i, category_name, category.unique_key))
            params["val{0}".format(i)] = category.nodes[node_name][category.unique_key]
        i = 0
        for category_name, category in self.categories.items():
            for abstract in category.rels:
                start, type_, end, properties = category._rel_abstract(abstract)
                # build and append pattern
                create_clause.append(node_pattern(i, start) +\
                                     rel_pattern(i, type_, properties) +\
                                     node_pattern(i, end)
                )
                i += 1
        if start_clause:
            query = "START {0}\nCREATE UNIQUE {1}".format(
                ",\n      ".join(start_clause),
                ",\n              ".join(create_clause),
            )
        else:
            query = "CREATE {0}".format(
                ",\n              ".join(create_clause),
            )
        if output_nodes:
            query += "\nRETURN {0}".format(
                ",\n       ".join("out{0}".format(i) for i in range(len(output_nodes))),
            )
        return query, params, output_nodes

    def merge(self, graph_db):
        # build batch request
        batch = neo4j.WriteBatch(graph_db)
        # 1. unique nodes
        unique_nodes = []
        for category_name, category in self.categories.items():
            key = category.unique_key
            for node_name, abstract in category.unique_nodes.items():
                unique_nodes.append((category_name, node_name))
                batch.get_or_create_indexed_node(category_name, key, abstract[key], abstract)
        # 2. related nodes
        if self.has_rels:
            query, params, related_nodes = self._create_relationship_query(unique_nodes)
            batch._post(batch._cypher_uri, {"query": query, "params": params})
        else:
            related_nodes = []
        # 3. odd nodes
        odd_nodes = []
        for name, abstract in self.nodes.items():
            if name not in unique_nodes and name not in related_nodes:
                odd_nodes.append(name)
                batch.create_node(abstract)
        # submit batch unless empty (in which case bail out and return nothing)
        if batch:
            responses = batch._submit()
        else:
            return {}
        # parse response and build return value
        return_nodes = dict((name, {}) for name in self.categories.keys())
        # 1. unique_nodes
        for i, (category_name, node_name) in enumerate(unique_nodes):
            response = responses.pop(0)
            return_nodes[category_name][node_name] = graph_db._resolve(response.body, response.status)
        # 2. related nodes
        if self.has_rels:
            data = responses.pop(0).body["data"]
            if data:
                for i, value in enumerate(data[0]):
                    category_name, node_name = related_nodes[i]
                    value = graph_db._resolve(value)
                    return_nodes[category_name][node_name] = graph_db._resolve(value)
        # 3. odd nodes
        for i, (category_name, node_name) in enumerate(odd_nodes):
            response = responses.pop(0)
            return_nodes[category_name][node_name] = graph_db._resolve(response.body, response.status)
        return return_nodes

def merge(abstract, graph_db):
    """ Merge an abstract subgraph into a graph database.

    :param abstract: abstract subgraph data
    :param graph_db: the graph into which to merge the data
    :return: collection of subgraph nodes
    """
    logger.debug("Merging subgraph: {0}".format(abstract))
    return Subgraph(abstract).merge(graph_db)
