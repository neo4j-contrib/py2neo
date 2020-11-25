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


from itertools import chain, islice
import json
import logging
from uuid import uuid4

from py2neo import ClientError
from py2neo.graphio.queries import nodes_create_unwind, nodes_merge_unwind, \
                                   _query_create_rels_unwind, _query_merge_rels_unwind, \
                                   _params_create_rels_unwind_from_objects


log = logging.getLogger(__name__)


# make sure to never change/override the values here
# consider implementing this in a safer way
BATCHSIZE = '1000'


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
            self.batch_size = BATCHSIZE

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
        for batch in _chunks(self.nodes, size=batch_size):
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
        for batch in _chunks(self.node_properties(), size=batch_size):
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
                    _create_single_index(graph, label, prop)

                # composite indexes
                if len(self.merge_keys) > 1:
                    _create_composite_index(graph, label, self.merge_keys)


class Relationship(object):

    TYPE = None

    def __init__(self, start_node_labels, end_node_labels, start_node_properties,
                 end_node_properties, properties):

        self.start_node_labels = start_node_labels
        self.end_node_labels = end_node_labels
        self.start_node_properties = start_node_properties
        self.end_node_properties = end_node_properties
        self.properties = properties
        self.object_type = self.TYPE

    def to_dict(self):
        return {
            'start_node_properties': self.start_node_properties,
            'end_node_properties': self.end_node_properties,
            'properties': self.properties
        }


class RelationshipSet:
    """
    Container for a set of Relationships with the same type of start and end nodes.
    """

    def __init__(self, rel_type, start_node_labels, end_node_labels, start_node_properties, end_node_properties,
                 batch_size=None):
        """

        :param rel_type: Realtionship type.
        :type rel_type: str
        :param start_node_labels: Labels of the start node.
        :type start_node_labels: list[str]
        :param end_node_labels: Labels of the end node.
        :type end_node_labels: list[str]
        :param start_node_properties: Property keys to identify the start node.
        :type start_node_properties: list[str]
        :param end_node_properties: Properties to identify the end node.
        :type end_node_properties: list[str]
        :param batch_size: Batch size for Neo4j operations.
        :type batch_size: int
        """

        self.rel_type = rel_type
        self.start_node_labels = start_node_labels
        self.end_node_labels = end_node_labels
        self.start_node_properties = start_node_properties
        self.end_node_properties = end_node_properties

        self.uuid = str(uuid4())
        self.combined = '{0}_{1}_{2}_{3}_{4}'.format(self.rel_type,
                                                     '_'.join(sorted(self.start_node_labels)),
                                                     '_'.join(sorted(self.end_node_labels)),
                                                     '_'.join(sorted(self.start_node_properties)),
                                                     '_'.join(sorted(self.end_node_properties))
                                                     )

        if batch_size:
            self.batch_size = batch_size
        else:
            self.batch_size = BATCHSIZE

        self.relationships = []

        self.unique = False
        self.unique_rels = set()

    def add_relationship(self, start_node_properties, end_node_properties, properties):
        """
        Add a relationship to this RelationshipSet.

        :param properties: Relationship properties.
        """
        if self.unique:
            # construct a check set with start_node_properties (values), end_node_properties (values) and properties (values)
            check_set = frozenset(
                list(start_node_properties.values()) + list(end_node_properties.values()) + list(properties.values()))

            if check_set not in self.unique_rels:
                rel = Relationship(self.start_node_labels, self.end_node_labels, start_node_properties,
                                   end_node_properties, properties)
                self.relationships.append(rel)
                self.unique_rels.add(check_set)
        else:
            rel = Relationship(self.start_node_labels, self.end_node_labels, start_node_properties,
                               end_node_properties, properties)
            self.relationships.append(rel)

    def item_iterator(self):
        """
        Generator function that yields the to_dict function for all relationships in this RelationshipSet.

        This is used to create chunks of the relationships without iterating all relationships. This can be removes in future
        when NodeSet and RelationshipSet fully support generators (instead of lists of nodes/relationships).
        """
        for rel in self.relationships:
            yield rel.to_dict()

    def to_dict(self):
        return {"rel_type":self.rel_type,
                "start_node_labels":self.start_node_labels,
                "end_node_labels":self.end_node_labels,
                "start_node_properties":self.start_node_properties,
                "end_node_properties":self.end_node_properties,
                "unique":self.unique,
                "relationships":[rel.to_dict() for rel in self.relationships]}

    @classmethod
    def from_dict(cls,relationship_dict,batch_size=None):
        rs = cls(rel_type=relationship_dict["rel_type"],
            start_node_labels=relationship_dict["start_node_labels"],
            end_node_labels=relationship_dict["end_node_labels"],
            start_node_properties=relationship_dict["start_node_properties"],
            end_node_properties=relationship_dict["end_node_properties"],
            batch_size=batch_size)
        rs.unique = relationship_dict["unique"]
        [rs.add_relationship(start_node_properties=rel["start_node_properties"],end_node_properties=rel["end_node_properties"],properties=rel["properties"]) for rel in relationship_dict["relationships"]]
        return rs

    def filter_relationships_target_node(self, filter_func):
        """
        Filter properties of target node with a filter function, remove relationships that do not match from main list.
        """
        filtered_rels = []
        discarded_rels = []

        for rel in self.relationships:

            if filter_func(rel.end_node_properties):
                filtered_rels.append(rel)
            else:
                discarded_rels.append(rel)

        self.relationships = filtered_rels
        self.discarded_nodes = discarded_rels

    def filter_relationships_start_node(self, filter_func):
        """
        Filter properties of target node with a filter function, remove relationships that do not match from main list.
        """
        filtered_rels = []
        discarded_rels = []

        for rel in self.relationships:

            if filter_func(rel.start_node_properties):
                filtered_rels.append(rel)
            else:
                discarded_rels.append(rel)

        self.relationships = filtered_rels
        self.discarded_nodes = discarded_rels

    def check_if_rel_exists(self, start_node_properties, end_node_properties, properties):
        for rel in self.relationships:
            if rel.start_node_properties == start_node_properties and rel.end_node_properties == end_node_properties and rel.properties == properties:
                return True

    def create(self, graph, batch_size=None):
        """
        Create relationships in this RelationshipSet
        """
        log.debug('Create RelationshipSet')
        if not batch_size:
            batch_size = self.batch_size
        log.debug('Batch Size: {}'.format(batch_size))

        # get query
        query = _query_create_rels_unwind(self.start_node_labels, self.end_node_labels, self.start_node_properties,
                                          self.end_node_properties, self.rel_type)
        log.debug(query)

        i = 1
        # iterate over chunks of rels
        for batch in _chunks(self.relationships, size=batch_size):
            batch = list(batch)
            log.debug('Batch {}'.format(i))
            log.debug(batch[0])
            # get parameters
            query_parameters = _params_create_rels_unwind_from_objects(batch)
            log.debug(json.dumps(query_parameters))

            graph.run(query, **query_parameters)
            i += 1

    def merge(self, graph, batch_size=None):
        """
        Create relationships in this RelationshipSet
        """
        log.debug('Create RelationshipSet')
        if not batch_size:
            batch_size = self.batch_size
        log.debug('Batch Size: {}'.format(batch_size))

        # get query
        query = _query_merge_rels_unwind(self.start_node_labels, self.end_node_labels, self.start_node_properties,
                                         self.end_node_properties, self.rel_type)
        log.debug(query)

        i = 1
        # iterate over chunks of rels
        for batch in _chunks(self.relationships, size=batch_size):
            batch = list(batch)
            log.debug('Batch {}'.format(i))
            log.debug(batch[0])
            # get parameters
            query_parameters = _params_create_rels_unwind_from_objects(batch)
            log.debug(json.dumps(query_parameters))

            graph.run(query, **query_parameters)
            i += 1

    def create_index(self, graph):
        """
        Create indices for start node and end node definition of this relationshipset. If more than one start or end
        node property is defined, all single property indices as well as the composite index are created.

        In Neo4j 3.x recreation of an index did not raise an error. In Neo4j 4 you cannot create an existing index.

        Index creation syntax changed from Neo4j 3.5 to 4. So far the old syntax is still supported. All py2neo
        functions (v4.4) work on both versions.
        """

        # from start nodes
        for label in self.start_node_labels:
            # create individual indexes
            for prop in self.start_node_properties:
                _create_single_index(graph, label, prop)

            # composite indexes
            if len(self.start_node_properties) > 1:
                _create_composite_index(graph, label, self.start_node_properties)

        for label in self.end_node_labels:
            for prop in self.end_node_properties:
                _create_single_index(graph, label, prop)

            # composite indexes
            if len(self.end_node_properties) > 1:
                _create_composite_index(graph, label, self.end_node_properties)


class Container:
    """
    A container for a collection of Nodes, Relationships, NodeSets and RelationshipSets.

    A typical parser function to e.g. read an Excel file produces a mixed output which then has to
    be processed accordingly.

    Also, sanity checks and data statistics are useful.
    """

    def __init__(self, objects=None):
        self.objects = []

        # add objects if they are passed
        if objects:
            for o in objects:
                self.objects.append(o)

    @property
    def nodesets(self):
        """
        Get the NodeSets in the Container.
        """
        return _get_instances_from_list(self.objects, NodeSet)

    @property
    def relationshipsets(self):
        """
        Get the RelationshipSets in the Container.
        """
        return _get_instances_from_list(self.objects, RelationshipSet)

    def get_nodeset(self, labels, merge_keys):
        for nodeset in self.nodesets:
            if set(nodeset.labels) == set(labels) and set(nodeset.merge_keys) == set(merge_keys):
                return nodeset

    def add(self, object):
        self.objects.append(object)

    def add_all(self, objects):
        for o in objects:
            self.add(o)

    def merge_nodesets(self):
        """
        Merge all node sets if merge_key is defined.
        """
        for nodeset in self.nodesets:
            nodeset.merge(nodeset.merge_keys)

    def create_relationshipsets(self):
        for relationshipset in self.relationshipsets:
            relationshipset.create()


def _get_instances_from_list(list, klass):
    """
    From a list of objects, get all objects that are instance of klass.

    :param list: List of objects.
    :param klass: The reference class.
    :return: Filtered list if objects.
    """
    output = []
    for o in list:
        if isinstance(o, klass):
            output.append(o)
    return output


def _chunks(iterable, size=10):
    """
    Get chunks of an iterable without pre-walking it.

    https://stackoverflow.com/questions/24527006/split-a-generator-into-chunks-without-pre-walking-it

    :param iterable: The iterable.
    :param size: Chunksize.
    :return: Yield chunks of defined size.
    """
    iterator = iter(iterable)
    for first in iterator:
        yield chain([first], islice(iterator, int(size) - 1))


def _create_single_index(graph, label, prop):
    """
    Create an inidex on a single property.

    :param label: The label.
    :param prop: The property.
    """
    try:
        log.debug("Create index {}, {}".format(label, prop))
        q = "CREATE INDEX ON :{0}({1})".format(label, prop)
        log.debug(q)
        graph.run(q)

    except ClientError:
        # TODO check if the index exists instead of catching the (very general) ClientError
        log.debug("Index {}, {} cannot be created, it likely exists alredy.".format(label, prop))


def _create_composite_index(graph, label, properties):
    """
    Create an inidex on a single property.

    :param label: The label.
    :param prop: The property.
    """
    try:
        property_string = ', '.join(properties)
        log.debug("Create index {}, {}".format(label, property_string))
        q = "CREATE INDEX ON :{0}({1})".format(label, property_string)
        log.debug(q)
        graph.run(q)

    except ClientError:
        # TODO check if the index exists instead of catching the (very general) ClientError
        log.debug("Index {}, {} cannot be created, it likely exists alredy.".format(label, properties))
