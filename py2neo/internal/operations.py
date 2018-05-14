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


__all__ = [
    "create_subgraph",
    "delete_subgraph",
    "merge_subgraph",
    "pull_subgraph",
    "push_subgraph",
    "separate_subgraph",
]


from collections import namedtuple

from py2neo.cypher.writing import cypher_escape


RelationshipData = namedtuple("RelationshipData", ["nodes", "properties"])


def node_dict(nodes):
    """

    :param nodes:
    :return: dict of frozenset(labels) to list(nodes)
    """
    d = {}
    for node in nodes:
        key = frozenset(node.labels)
        d.setdefault(key, []).append(node)
    return d


def node_merge_dict(primary_label, primary_key, nodes):
    """

    :param primary_label:
    :param primary_key:
    :param nodes:
    :return: dict of (p_label, p_key, frozenset(labels)) to list(nodes)
    """
    d = {}
    for node in nodes:
        p_label = getattr(node, "__primarylabel__", None) or primary_label
        p_key = getattr(node, "__primarykey__", None) or primary_key
        key = (p_label, p_key, frozenset(node.labels))
        d.setdefault(key, []).append(node)
    return d


def relationship_dict(relationships):
    d = {}
    for relationship in relationships:
        key = type(relationship).__name__
        d.setdefault(key, []).append(relationship)
    return d


def create_nodes(tx, labels, data):
    assert isinstance(labels, frozenset)
    label_string = "".join(":" + cypher_escape(label) for label in sorted(labels))
    cypher = "UNWIND $x AS data CREATE (_%s) SET _ = data RETURN id(_)" % label_string
    for record in tx.run(cypher, x=data):
        yield record[0]


def merge_nodes(tx, p_label, p_key, labels, data):
    """

    :param tx:
    :param p_label:
    :param p_key:
    :param labels:
    :param data: list of (p_value, properties)
    :return:
    """
    assert isinstance(labels, frozenset)
    label_string = ":".join(cypher_escape(label) for label in sorted(labels))
    cypher = "UNWIND $x AS data MERGE (_:%s {%s:data[0]}) SET _:%s SET _ = data[1] RETURN id(_)" % (
        cypher_escape(p_label), cypher_escape(p_key), label_string)
    for record in tx.run(cypher, x=data):
        yield record[0]


def merge_relationships(tx, r_type, data):
    """

    :param tx:
    :param r_type:
    :param data: list of (a_id, b_id, properties)
    :return:
    """
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


def merge_subgraph(tx, subgraph, p_label, p_key):
    graph = tx.graph
    for (pl, pk, labels), nodes in node_merge_dict(p_label, p_key, (n for n in subgraph.nodes if n.graph is None)).items():
        if pl is None or pk is None:
            raise ValueError("Primary label and primary key are required for MERGE operation")
        identities = merge_nodes(tx, pl, pk, labels, map(lambda n: [n.get(pk), dict(n)], nodes))
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
    node_identities = []
    for relationship in subgraph.relationships:
        if relationship.graph is graph:
            graph.relationship_cache.update(relationship.identity, None)
            relationship.graph = None
            relationship.identity = None
    for node in subgraph.nodes:
        if node.graph is graph:
            graph.node_cache.update(node.identity, None)
            node_identities.append(node.identity)
            node.graph = None
            node.identity = None
    list(tx.run("MATCH (_) WHERE id(_) IN $x DETACH DELETE _", x=node_identities))


def separate_subgraph(tx, subgraph):
    graph = tx.graph
    relationship_identities = []
    for relationship in subgraph.relationships:
        if relationship.graph is graph:
            graph.relationship_cache.update(relationship.identity, None)
            relationship_identities.append(relationship.identity)
            relationship.graph = None
            relationship.identity = None
    list(tx.run("MATCH ()-[_]->() WHERE id(_) IN $x DELETE _", x=relationship_identities))


def pull_subgraph(tx, subgraph):
    graph = tx.graph
    nodes = {node: None for node in subgraph.nodes}
    relationships = list(subgraph.relationships)
    for node in nodes:
        if node.graph is graph:
            tx.entities.append({"_": node})
            cursor = tx.run("MATCH (_) WHERE id(_) = {x} RETURN _, labels(_)", x=node.identity)
            nodes[node] = cursor
    for relationship in relationships:
        if relationship.graph is graph:
            tx.entities.append({"_": relationship})
            list(tx.run("MATCH ()-[_]->() WHERE id(_) = {x} RETURN _", x=relationship.identity))
    for node, cursor in nodes.items():
        new_labels = cursor.evaluate(1)
        if new_labels:
            node._remote_labels = frozenset(new_labels)
            labels = node._labels
            labels.clear()
            labels.update(new_labels)


def push_subgraph(tx, subgraph):
    graph = tx.graph
    for node in subgraph.nodes:
        if node.graph is graph:
            clauses = ["MATCH (_) WHERE id(_) = {x}", "SET _ = {y}"]
            parameters = {"x": node.identity, "y": dict(node)}
            old_labels = node._remote_labels - node._labels
            if old_labels:
                clauses.append("REMOVE _:%s" % ":".join(map(cypher_escape, old_labels)))
            new_labels = node._labels - node._remote_labels
            if new_labels:
                clauses.append("SET _:%s" % ":".join(map(cypher_escape, new_labels)))
            tx.run("\n".join(clauses), parameters)
    for relationship in subgraph.relationships:
        if relationship.graph is graph:
            clauses = ["MATCH ()-[_]->() WHERE id(_) = {x}", "SET _ = {y}"]
            parameters = {"x": relationship.identity, "y": dict(relationship)}
            tx.run("\n".join(clauses), parameters)
