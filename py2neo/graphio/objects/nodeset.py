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


import logging
from uuid import uuid4

from py2neo.graphio.queries import nodes_create_unwind, nodes_merge_unwind
from py2neo.graphio.objects.helper import chunks, create_single_index, create_composite_index
from py2neo.graphio import defaults
from py2neo.graphio.objects.relationshipset import RelationshipSet

log = logging.getLogger(__name__)


class NodeSet:
    """
    Container for a set of Nodes with the same labels and the same properties that define uniqueness.
    """

    def __init__(self, labels, merge_keys=None, batch_size=None):
        """

        :param labels: The labels for the nodes in this NodeSet.
        :type labels: list[str]
        :param merge_keys: The properties that define uniqueness of the nodes in this NodeSet.
        :type merge_keys: list[str]
        :param batch_size: Batch size for Neo4j operations.
        :type batch_size: int
        """
        self.labels = labels
        self.merge_keys = merge_keys

        self.combined = '_'.join(sorted(self.labels)) + '_' + '_'.join(sorted(self.merge_keys))
        self.uuid = str(uuid4())

        if batch_size:
            self.batch_size = batch_size
        else:
            self.batch_size = defaults.BATCHSIZE

        self.nodes = []

    def add_node(self, properties):
        """
        Create a node in this NodeSet.

        :param properties: Node properties.
        :type properties: dict
        """
        self.nodes.append(properties)

    def add_nodes(self, list_of_properties):
        for properties in list_of_properties:
            self.add_node(properties)

    def add_unique(self, properties):
        """
        Add a node to this NodeSet only if a node with the same `merge_keys` does not exist yet.

        Note: Right now this function iterates all nodes in the NodeSet. This is of course slow for large
        numbers of nodes. A better solution would be to create an 'index' as is done for RelationshipSet.

        :param properties: Node properties.
        :type properties: dict
        """

        compare_values = frozenset([properties[key] for key in self.merge_keys])

        for other_node_properties in self.node_properties():
            this_values = frozenset([other_node_properties[key] for key in self.merge_keys])
            if this_values == compare_values:
                return None

        # add node if not found
        self.add_node(properties)

    def item_iterator(self):
        """
        Generator function that yields the node properties for all nodes in this NodeSet.

        This is used to create chunks of the nodes without iterating all nodes. This can be removes in future
        when NodeSet and RelationshipSet fully support generators (instead of lists of nodes/relationships).
        """
        for node in self.nodes:
            yield node

    def to_dict(self):
        """
        Create dictionary defining the nodeset.
        """
        return {"labels":self.labels,"merge_keys":self.merge_keys,"nodes":self.nodes}

    @classmethod
    def from_dict(cls,nodeset_dict,batch_size=None):
        ns = cls(labels=nodeset_dict["labels"],merge_keys=nodeset_dict["merge_keys"])
        ns.add_nodes(nodeset_dict["nodes"])
        return ns

    def create(self, graph, batch_size=None):
        """
        Create all nodes from NodeSet.
        """
        log.debug('Create NodeSet')
        if not batch_size:
            batch_size = self.batch_size
        log.debug('Batch Size: {}'.format(batch_size))

        i = 1
        for batch in chunks(self.nodes, size=batch_size):
            batch = list(batch)
            log.debug('Batch {}'.format(i))

            query = nodes_create_unwind(self.labels)
            log.debug(query)

            result = graph.run(query, props=batch)

            i += 1

    # TODO remove py2neo Node here, the node is just a dict now
    def filter_nodes(self, filter_func):
        """
        Filter node properties with a filter function, remove nodes that do not match from main list.
        """
        filtered_nodes = []
        discarded_nodes = []
        for n in self.nodes:
            node_properties = dict(n)
            if filter_func(node_properties):
                filtered_nodes.append(n)
            else:
                discarded_nodes.append(n)

        self.nodes = filtered_nodes
        self.discarded_nodes = discarded_nodes

    # TODO remove py2neo Node here, the node is just a dict now
    def reduce_node_properties(self, *keep_props):
        filtered_nodes = []
        for n in self.nodes:
            new_props = {}
            for k, v in dict(n).items():
                if k in keep_props:
                    new_props[k] = v

            filtered_nodes.append(Node(*self.labels, **new_props))

        self.nodes = filtered_nodes

    def merge(self, graph, merge_properties=None, batch_size=None):
        """
        Merge nodes from NodeSet on merge properties.

        :param merge_properties: The merge properties.
        """
        log.debug('Merge NodeSet on {}'.format(merge_properties))

        if not batch_size:
            batch_size = self.batch_size

        if not merge_properties:
            merge_properties = self.merge_keys

        log.debug('Batch Size: {}'.format(batch_size))

        query = nodes_merge_unwind(self.labels, merge_properties)
        log.debug(query)

        i = 1
        for batch in chunks(self.node_properties(), size=batch_size):
            batch = list(batch)
            log.debug('Batch {}'.format(i))
            log.debug(batch[0])

            graph.run(query, props=batch)
            i += 1

    def map_to_1(self, graph, target_labels, target_properties, rel_type=None):
        """
        Create relationships from all nodes in this NodeSet to 1 target node.

        :param graph: The py2neo Graph
        :param other_node: The target node.
        :param rel_type: Relationship Type
        """

        if not rel_type:
            rel_type = 'FROM_SET'

        rels = RelationshipSet(rel_type, self.labels, target_labels, self.merge_keys, target_properties)

        for node in self.nodes:
            # get properties for merge_keys
            node_properties = {}
            for k in self.merge_keys:
                node_properties[k] = node[k]

            rels.add_relationship(node_properties, target_properties, {})

        rels.create(graph)

    def node_properties(self):
        """
        Yield properties of the nodes in this set. Used for create function.
        """
        for n in self.nodes:
            yield dict(n)

    def all_properties_in_nodeset(self):
        """
        Return a set of all property keys in this NodeSet

        :return: A set of unique property keys of a NodeSet
        """
        all_props = set()

        # collect properties
        for props in self.node_properties():
            for k in props:
                all_props.add(k)

        return all_props

    def create_index(self, graph):
        """
        Create indices for all label/merge ky combinations as well as a composite index if multiple merge keys exist.

        In Neo4j 3.x recreation of an index did not raise an error. In Neo4j 4 you cannot create an existing index.

        Index creation syntax changed from Neo4j 3.5 to 4. So far the old syntax is still supported. All py2neo
        functions (v4.4) work on both versions.
        """
        if self.merge_keys:
            for label in self.labels:
                # create individual indexes
                for prop in self.merge_keys:
                    create_single_index(graph, label, prop)

                # composite indexes
                if len(self.merge_keys) > 1:
                    create_composite_index(graph, label, self.merge_keys)
