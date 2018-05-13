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


from collections import namedtuple

from py2neo.cypher.writing import cypher_escape
from py2neo.internal.collections import is_collection


RelationshipData = namedtuple("RelationshipData", ["nodes", "properties"])


def node_dict(nodes):
    d = {}
    for node in nodes:
        d.setdefault(frozenset(node.labels), []).append(node)
    return d


def relationship_dict(relationships):
    d = {}
    for relationship in relationships:
        d.setdefault(type(relationship).__name__, []).append(relationship)
    return d


def create_nodes(tx, labels, data):
    assert is_collection(labels)
    label_string = "".join(":" + cypher_escape(label) for label in sorted(labels))
    cypher = "UNWIND $x AS properties CREATE (_%s) SET _ = properties RETURN id(_)" % label_string
    for record in tx.run(cypher, x=data):
        yield record[0]


def merge_relationships(tx, r_type, data):
    cypher = ("UNWIND $x AS data "
              "MATCH (a) WHERE id(a) = data[0] "
              "MATCH (b) WHERE id(b) = data[1] "
              "MERGE (a)-[_:%s]->(b) SET _ = data[2] RETURN id(_)" % cypher_escape(r_type))
    for record in tx.run(cypher, x=data):
        yield record[0]


def create_subgraph(tx, subgraph):
    graph = tx.graph
    for labels, nodes in node_dict(n for n in subgraph.nodes if n.graph is None).items():
        identities = create_nodes(tx, labels, map(dict, nodes))
        for i, identity in enumerate(identities):
            node = nodes[i]
            node.graph = graph
            node.identity = identity
            node._remote_labels = labels
            graph.node_cache.update(identity, node)
    for r_type, relationships in relationship_dict(r for r in subgraph.relationships if r.graph is None).items():
        identities = merge_relationships(tx, r_type, map(
            lambda r: [r.start_node.identity, r.end_node.identity, dict(r)], relationships))
        for i, identity in enumerate(identities):
            relationship = relationships[i]
            relationship.graph = graph
            relationship.identity = identity
            graph.relationship_cache.update(identity, relationship)


def delete_subgraph(tx, subgraph):
    graph = tx.graph
    node_ids = set()
    relationship_ids = set()
    for i, node in enumerate(subgraph.nodes):
        if node.graph is graph:
            node_ids.add(node.identity)
    for i, relationship in enumerate(subgraph.relationships):
        if relationship.graph is graph:
            relationship_ids.add(relationship.identity)
    statement = ("OPTIONAL MATCH (a) WHERE id(a) IN $x "
                 "OPTIONAL MATCH ()-[r]->() WHERE id(r) IN $y "
                 "DELETE r, a")
    parameters = {"x": list(node_ids), "y": list(relationship_ids)}
    list(tx.run(statement, parameters))
