#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2021, Nigel Small
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
This module contains functions used to carry out basic data operations
such as creating or merging nodes, as well as operations on full
subgraphs. Each operation typically accepts a transaction object as its
first argument; it is in this transaction that the operation is carried
out. The remainder of the arguments depend on the nature of the
operation.

Many of these functions wrap well-tuned Cypher queries, and can avoid
the need to manually implement these operations. As an example,
:func:`.create_nodes` uses the fast ``UNWIND ... CREATE`` method to
iterate through a list of raw node data and create each node in turn.
"""

from __future__ import absolute_import


__all__ = [
    "create_nodes",
    "merge_nodes",
    "create_relationships",
    "merge_relationships",
    "create_subgraph",
    "delete_subgraph",
    "merge_subgraph",
    "pull_subgraph",
    "push_subgraph",
    "separate_subgraph",
    "subgraph_exists",
    "UniquenessError",
]

from py2neo.cypher import cypher_escape, cypher_join
from py2neo.cypher.queries import (
    unwind_create_nodes_query,
    unwind_merge_nodes_query,
    unwind_create_relationships_query,
    unwind_merge_relationships_query)


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


def create_nodes(tx, data, labels=None, keys=None):
    """ Create nodes from an iterable sequence of raw node data.

    The raw node `data` is supplied as either a list of lists or a list
    of dictionaries. If the former, then a list of `keys` must also be
    provided in the same order as the values. This option will also
    generally require fewer bytes to be sent to the server, since key
    duplication is removed.

    An iterable of extra `labels` can also be supplied, which will be
    attached to all new nodes.

    The example code below shows how to pass raw node data as a list of
    lists:

        >>> from py2neo import Graph
        >>> from py2neo.data.operations import create_nodes
        >>> g = Graph()
        >>> keys = ["name", "age"]
        >>> data = [
            ["Alice", 33],
            ["Bob", 44],
            ["Carol", 55],
        ]
        >>> create_nodes(g.auto(), data, labels={"Person"}, keys=keys)
        >>> g.nodes.match("Person").count()
        3

    This second example shows how to pass raw node data as a list of
    dictionaries. This alternative can be particularly useful if the
    fields are not uniform across records.

        >>> data = [
            {"name": "Dave", "age": 66},
            {"name": "Eve", "date_of_birth": "1943-10-01"},
            {"name": "Frank"},
        ]
        >>> create_nodes(g.auto(), data, labels={"Person"})
        >>> g.nodes.match("Person").count()
        6

    There are obviously practical limits to the amount of data that
    should be included in a single bulk load of this type. For that
    reason, it is advisable to batch the input data into chunks, and
    carry out each in a separate transaction.

    The example below shows how batching can be achieved using a simple
    loop. This code assumes that `data` is an iterable of raw node data
    (lists of values) and steps through that data in chunks of size
    `batch_size` until everything has been consumed.

        >>> from itertools import islice
        >>> stream = iter(data)
        >>> batch_size = 10000
        >>> while True:
        ...     batch = islice(stream, batch_size)
        ...     if batch:
        ...         create_nodes(g.auto(), batch, labels={"Person"})
        ...     else:
        ...         break

    There is no universal `batch_size` that performs optimally for all
    use cases. It is recommended to experiment with this value to
    discover what size works best.

    :param tx: :class:`.Transaction` in which to carry out this
        operation
    :param data: node data supplied as a list of lists (if `keys` are
        provided) or a list of dictionaries (if `keys` is :const:`None`)
    :param labels: labels to apply to the created nodes
    :param keys: an optional set of keys for the supplied `data`
    """
    list(tx.run(*unwind_create_nodes_query(data, labels, keys)))


def merge_nodes(tx, data, merge_key, labels=None, keys=None):
    """ Merge nodes from an iterable sequence of raw node data.

    In a similar way to :meth:`.create_nodes`, the raw node `data` can
    be supplied as either lists (with `keys`) or dictionaries. This
    method however uses an ``UNWIND ... MERGE`` construct in the
    underlying Cypher query to create or update nodes depending
    on what already exists.

    The merge is performed on the basis of the label and keys
    represented by the `merge_key`, updating a node if that combination
    is already present in the graph, and creating a new node otherwise.
    As with :meth:`.create_nodes`, extra `labels` may also be
    specified; these will be applied to all nodes, pre-existing or new.
    The label included in the `merge_key` does not need to be
    separately included here.

    The example code below shows a simple merge based on a `Person`
    label and a `name` property:

        >>> from py2neo import Graph
        >>> from py2neo.data.operations import merge_nodes
        >>> g = Graph()
        >>> keys = ["name", "age"]
        >>> data = [
            ["Alice", 33],
            ["Bob", 44],
            ["Carol", 55],
            ["Carol", 66],
            ["Alice", 77],
        ]
        >>> merge_nodes(g.auto(), data, ("Person", "name"), keys=keys)
        >>> g.nodes.match("Person").count()
        3

    :param tx: :class:`.Transaction` in which to carry out this
        operation
    :param data: node data supplied as a list of lists (if `keys` are
        provided) or a list of dictionaries (if `keys` is :const:`None`)
    :param merge_key: tuple of (label, key1, key2...) on which to merge
    :param labels: additional labels to apply to the merged nodes
    :param keys: an optional set of keys for the supplied `data`
    """
    list(tx.run(*unwind_merge_nodes_query(data, merge_key, labels, keys)))


def create_relationships(tx, data, rel_type, keys=None, start_node_key=None, end_node_key=None):
    """ Create relationships from an iterable sequence of raw
    relationship data.

    The raw relationship `data` is supplied as a list of triples (or
    3-item lists), each representing (start_node, detail, end_node).
    The `rel_type` specifies the type of relationship to create, and is
    fixed for the entire data set.

    Start and end node information can either be provided as an
    internal node ID or, in conjunction with a `start_node_key` or
    `end_node_key`, a tuple or list of property values to ``MATCH``.
    For example, to link people to their place of work, the code below
    could be used:

        >>> from py2neo import Graph
        >>> from py2neo.data.operations import create_relationships
        >>> g = Graph()
        >>> data = [
            (("Alice", "Smith"), {"since": 1999}, "ACME"),
            (("Bob", "Jones"), {"since": 2002}, "Bob Corp"),
            (("Carol", "Singer"), {"since": 1981}, "The Daily Planet"),
        ]
        >>> create_relationships(g.auto(), data, "WORKS_FOR", \
            start_node_key=("Person", "name", "family name"), \
            end_node_key=("Company", "name"))

    If the company node IDs were already known, the code could instead
    look like this:

        >>> data = [
            (("Alice", "Smith"), {"since": 1999}, 123),
            (("Bob", "Jones"), {"since": 2002}, 124),
            (("Carol", "Singer"), {"since": 1981}, 201),
        ]
        >>> create_relationships(g.auto(), data, "WORKS_FOR", \
            start_node_key=("Person", "name", "family name"))

    As with other methods, such as :meth:`.create_nodes`, the
    relationship `data` can also be supplied as a list of property
    values, indexed by `keys`. This can avoid sending duplicated key
    names over the network, and alters the method call as follows:

        >>> data = [
            (("Alice", "Smith"), [1999], 123),
            (("Bob", "Jones"), [2002], 124),
            (("Carol", "Singer"), [1981], 201),
        ]
        >>> create_relationships(g.auto(), data, "WORKS_FOR", keys=["since"] \
            start_node_key=("Person", "name", "family name"))

    :param tx: :class:`.Transaction` in which to carry out this
        operation
    :param data:
    :param rel_type:
    :param keys:
    :param start_node_key:
    :param end_node_key:
    :return:
    """
    list(tx.run(*unwind_create_relationships_query(data, rel_type, keys,
                                                   start_node_key, end_node_key)))


def merge_relationships(tx, data, merge_key, keys=None, start_node_key=None, end_node_key=None):
    """ Merge relationships from an iterable sequence of raw
    relationship data.

    :param tx: :class:`.Transaction` in which to carry out this
        operation
    :param data:
    :param merge_key:
    :param keys:
    :param start_node_key:
    :param end_node_key:
    :return:
    """
    list(tx.run(*unwind_create_relationships_query(data, merge_key, keys,
                                                   start_node_key, end_node_key)))


def create_subgraph(tx, subgraph):
    """ Create new data in a remote :class:`.Graph` from a local
    :class:`.Subgraph`.

    :param tx:
    :param subgraph:
    :return:
    """
    graph = tx.graph
    for labels, nodes in _node_create_dict(n for n in subgraph.nodes if n.graph is None).items():
        pq = unwind_create_nodes_query(list(map(dict, nodes)), labels=labels)
        pq = cypher_join(pq, "RETURN id(_)")
        records = tx.run(*pq)
        for i, record in enumerate(records):
            node = nodes[i]
            node.graph = graph
            node.identity = record[0]
            node._remote_labels = labels
    for r_type, relationships in _rel_create_dict(r for r in subgraph.relationships if r.graph is None).items():
        data = map(lambda r: [r.start_node.identity, dict(r), r.end_node.identity], relationships)
        pq = unwind_merge_relationships_query(data, r_type)
        pq = cypher_join(pq, "RETURN id(_)")
        for i, record in enumerate(tx.run(*pq)):
            relationship = relationships[i]
            relationship.graph = graph
            relationship.identity = record[0]


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
        pq = unwind_merge_nodes_query(map(dict, nodes), (pl, pk), labels)
        pq = cypher_join(pq, "RETURN id(_)")
        identities = [record[0] for record in tx.run(*pq)]
        if len(identities) > len(nodes):
            raise UniquenessError("Found %d matching nodes for primary label %r and primary "
                                  "key %r with labels %r but merging requires no more than "
                                  "one" % (len(identities), pl, pk, set(labels)))
        for i, identity in enumerate(identities):
            node = nodes[i]
            node.graph = graph
            node.identity = identity
            node._remote_labels = labels
    for r_type, relationships in _rel_create_dict(r for r in subgraph.relationships if r.graph is None).items():
        data = map(lambda r: [r.start_node.identity, dict(r), r.end_node.identity], relationships)
        pq = unwind_merge_relationships_query(data, r_type)
        pq = cypher_join(pq, "RETURN id(_)")
        for i, record in enumerate(tx.run(*pq)):
            relationship = relationships[i]
            relationship.graph = graph
            relationship.identity = record[0]


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
            relationship.graph = None
            relationship.identity = None
    for node in subgraph.nodes:
        if node.graph is graph:
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


# TODO: find a better home for this class
class UniquenessError(Exception):
    """ Raised when a condition assumed to be unique is determined
    non-unique.
    """
