#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2020, Nigel Small
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


from __future__ import absolute_import


__all__ = [
    "create_subgraph",
    "delete_subgraph",
    "merge_subgraph",
    "pull_subgraph",
    "push_subgraph",
    "separate_subgraph",
    "subgraph_exists",
]


from py2neo.cypher import cypher_escape


def _node_create_dict(nodes):
    """ Convert a set of :class:`.Node` objects into a dictionary of
    :class:`.Node` lists, keyed by frozenset(labels).

    :param nodes:
    :return: dict of frozenset(labels) to list(nodes)
    """
    d = {}
    for node in nodes:
        key = frozenset(node.labels)
        d.setdefault(key, []).append(node)
    return d


def _node_merge_dict(primary_label, primary_key, nodes):
    """ Convert a set of :class:`.Node` objects into a dictionary of
    :class:`.Node` lists, keyed by a 3-tuple of
    (primary_label, primary_key, frozenset(labels)).

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


def _rel_create_dict(relationships):
    """ Convert a set of :class:`.Relationship` objects into a dictionary
    of :class:`.Relationship` lists, keyed by type.

    :param relationships:
    :return:
    """
    d = {}
    for relationship in relationships:
        key = type(relationship).__name__
        d.setdefault(key, []).append(relationship)
    return d


def _create_nodes(tx, labels, data):
    assert isinstance(labels, frozenset)
    label_string = "".join(":" + cypher_escape(label) for label in sorted(labels))
    cypher = "UNWIND $x AS data CREATE (_%s) SET _ = data RETURN id(_)" % label_string
    for record in tx.run(cypher, x=data):
        yield record[0]


def _merge_nodes(tx, p_label, p_key, labels, data):
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


def _merge_relationships(tx, r_type, data):
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
    """ Create new data in a remote :class:`.Graph` from a local
    :class:`.Subgraph`.

    :param tx:
    :param subgraph:
    :return:
    """
    graph = tx.graph
    for labels, nodes in _node_create_dict(n for n in subgraph.nodes if n.graph is None).items():
        identities = _create_nodes(tx, labels, list(map(dict, nodes)))
        for i, identity in enumerate(identities):
            node = nodes[i]
            node.graph = graph
            node.identity = identity
            node._remote_labels = labels
            graph.node_cache.update(identity, node)
    for r_type, relationships in _rel_create_dict(r for r in subgraph.relationships if r.graph is None).items():
        identities = _merge_relationships(tx, r_type, list(map(
            lambda r: [r.start_node.identity, r.end_node.identity, dict(r)], relationships)))
        for i, identity in enumerate(identities):
            relationship = relationships[i]
            relationship.graph = graph
            relationship.identity = identity
            graph.relationship_cache.update(identity, relationship)


def merge_subgraph(tx, subgraph, p_label, p_key):
    """ Merge data into a remote :class:`.Graph` from a local
    :class:`.Subgraph`.

    :param tx:
    :param subgraph:
    :param p_label:
    :param p_key:
    :return:
    """
    graph = tx.graph
    for (pl, pk, labels), nodes in _node_merge_dict(p_label, p_key, (n for n in subgraph.nodes if n.graph is None)).items():
        if pl is None or pk is None:
            raise ValueError("Primary label and primary key are required for MERGE operation")
        identities = _merge_nodes(tx, pl, pk, labels, list(map(lambda n: [n.get(pk), dict(n)], nodes)))
        for i, identity in enumerate(identities):
            node = nodes[i]
            node.graph = graph
            node.identity = identity
            node._remote_labels = labels
            graph.node_cache.update(identity, node)
    for r_type, relationships in _rel_create_dict(r for r in subgraph.relationships if r.graph is None).items():
        identities = _merge_relationships(tx, r_type, list(map(
            lambda r: [r.start_node.identity, r.end_node.identity, dict(r)], relationships)))
        for i, identity in enumerate(identities):
            relationship = relationships[i]
            relationship.graph = graph
            relationship.identity = identity
            graph.relationship_cache.update(identity, relationship)


def delete_subgraph(tx, subgraph):
    """ Delete data in a remote :class:`.Graph` based on a local
    :class:`.Subgraph`.

    :param tx:
    :param subgraph:
    :return:
    """
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
    """ Delete relationships in a remote :class:`.Graph` based on a
    local :class:`.Subgraph`.

    :param tx:
    :param subgraph:
    :return:
    """
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
    """ Copy data from a remote :class:`.Graph` into a local
    :class:`.Subgraph`.

    :param tx:
    :param subgraph:
    :return:
    """
    graph = tx.graph
    nodes = {node: None for node in subgraph.nodes}
    relationships = list(subgraph.relationships)
    for node in nodes:
        if node.graph is graph:
            tx.entities.append({"_": node})
            cursor = tx.run("MATCH (_) WHERE id(_) = $x RETURN _, labels(_)", x=node.identity)
            nodes[node] = cursor
    for relationship in relationships:
        if relationship.graph is graph:
            tx.entities.append({"_": relationship})
            list(tx.run("MATCH ()-[_]->() WHERE id(_) = $x RETURN _", x=relationship.identity))
    for node, cursor in nodes.items():
        new_labels = cursor.evaluate(1)
        if new_labels:
            node._remote_labels = frozenset(new_labels)
            labels = node._labels
            labels.clear()
            labels.update(new_labels)


def push_subgraph(tx, subgraph):
    """ Copy data into a remote :class:`.Graph` from a local
    :class:`.Subgraph`.

    :param tx:
    :param subgraph:
    :return:
    """
    graph = tx.graph
    for node in subgraph.nodes:
        if node.graph is graph:
            clauses = ["MATCH (_) WHERE id(_) = $x", "SET _ = $y"]
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
            clauses = ["MATCH ()-[_]->() WHERE id(_) = $x", "SET _ = $y"]
            parameters = {"x": relationship.identity, "y": dict(relationship)}
            tx.run("\n".join(clauses), parameters)


def subgraph_exists(tx, subgraph):
    """ Determine whether one or more graph entities all exist within the
    database. Note that if any nodes or relationships in *subgraph* are not
    bound to remote counterparts, this method will return ``False``.

    :param tx:
    :param subgraph: a :class:`.Node`, :class:`.Relationship` or other
                   :class:`.Subgraph`
    :returns: ``True`` if all entities exist remotely, ``False`` otherwise
    """
    graph = tx.graph
    node_ids = set()
    relationship_ids = set()
    for i, node in enumerate(subgraph.nodes):
        if node.graph is graph:
            node_ids.add(node.identity)
        else:
            return False
    for i, relationship in enumerate(subgraph.relationships):
        if relationship.graph is graph:
            relationship_ids.add(relationship.identity)
        else:
            return False
    statement = ("OPTIONAL MATCH (a) WHERE id(a) IN $x "
                 "OPTIONAL MATCH ()-[r]->() WHERE id(r) IN $y "
                 "RETURN count(DISTINCT a) + count(DISTINCT r)")
    parameters = {"x": list(node_ids), "y": list(relationship_ids)}
    return tx.evaluate(statement, parameters) == len(node_ids) + len(relationship_ids)
