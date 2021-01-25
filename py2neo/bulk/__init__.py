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


"""
This module contains facilities to carry out bulk data operations
such as creating or merging nodes and relationships.
"""


__all__ = [
    "create_nodes",
    "merge_nodes",
    "create_relationships",
    "merge_relationships",
    "NodeSet",
    "RelationshipSet",
    "Container",
]


from itertools import chain, islice
from logging import getLogger
from uuid import uuid4

from py2neo import ClientError
from py2neo.bulk.queries import nodes_merge_unwind, \
                                   _query_create_rels_unwind, _query_merge_rels_unwind, \
                                   _params_create_rels_unwind_from_objects
from py2neo.cypher.queries import (
    unwind_create_nodes_query,
    unwind_merge_nodes_query,
    unwind_create_relationships_query,
    unwind_merge_relationships_query,
)


log = getLogger(__name__)


def create_nodes(tx, data, labels=None, keys=None):
    """ Create nodes from an iterable sequence of raw node data.

    The raw node `data` is supplied as either a list of lists or a list
    of dictionaries. If the former, then a list of `keys` must also be
    provided in the same order as the values. This option will also
    generally require fewer bytes to be sent to the server, since key
    duplication is removed. An iterable of extra `labels` can also be
    supplied, which will be attached to all new nodes.

    The example code below shows how to pass raw node data as a list of
    lists:

        >>> from py2neo import Graph
        >>> from py2neo.bulk import create_nodes
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

    The code below shows how batching can be achieved using a simple
    loop. This assumes that `data` is an iterable of raw node data
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
    :param keys: optional set of keys for the supplied `data` (if
        supplied as value lists)
    """
    list(tx.run(*unwind_create_nodes_query(data, labels, keys)))


def merge_nodes(tx, data, merge_key, labels=None, keys=None):
    """ Merge nodes from an iterable sequence of raw node data.

    In a similar way to :func:`.create_nodes`, the raw node `data` can
    be supplied as either lists (with field `keys`) or as dictionaries.
    This method however uses an ``UNWIND ... MERGE`` construct in the
    underlying Cypher query to create or update nodes depending
    on what already exists.

    The merge is performed on the basis of the label and keys
    represented by the `merge_key`, updating a node if that combination
    is already present in the graph, and creating a new node otherwise.
    The value of this argument may take one of several forms and is
    used internally to construct an appropriate ``MERGE`` pattern. The
    table below gives examples of the values permitted, and how each is
    interpreted, using ``x`` as the input value from the source data.

    .. table::
        :widths: 40 60

        =================================================  ===========================================================
        Argument                                           ``MERGE`` Clause
        =================================================  ===========================================================
        ``("Person", "name")``                             ``MERGE (a:Person {name:x})``
        ``("Person", "name", "family name")``              ``MERGE (a:Person {name:x[0], `family name`:x[1]})``
        ``(("Person", "Female"), "name")``                 ``MERGE (a:Female:Person {name:x})``
        ``(("Person", "Female"), "name", "family name")``  ``MERGE (a:Female:Person {name:x[0], `family name`:x[1]})``
        =================================================  ===========================================================

    As with :func:`.create_nodes`, extra `labels` may also be
    specified; these will be applied to all nodes, pre-existing or new.
    The label included in the `merge_key` does not need to be
    separately included here.

    The example code below shows a simple merge based on a `Person`
    label and a `name` property:

        >>> from py2neo import Graph
        >>> from py2neo.bulk import merge_nodes
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
    :param keys: optional set of keys for the supplied `data` (if
        supplied as value lists)
    """
    list(tx.run(*unwind_merge_nodes_query(data, merge_key, labels, keys)))


def create_relationships(tx, data, rel_type, start_node_key=None, end_node_key=None, keys=None):
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
        >>> from py2neo.bulk import create_relationships
        >>> g = Graph()
        >>> data = [
            (("Alice", "Smith"), {"since": 1999}, "ACME"),
            (("Bob", "Jones"), {"since": 2002}, "Bob Corp"),
            (("Carol", "Singer"), {"since": 1981}, "The Daily Planet"),
        ]
        >>> create_relationships(g.auto(), data, "WORKS_FOR", \\
            start_node_key=("Person", "name", "family name"), end_node_key=("Company", "name"))

    If the company node IDs were already known by other means, the code
    could instead look like this:

        >>> data = [
            (("Alice", "Smith"), {"since": 1999}, 123),
            (("Bob", "Jones"), {"since": 2002}, 124),
            (("Carol", "Singer"), {"since": 1981}, 201),
        ]
        >>> create_relationships(g.auto(), data, "WORKS_FOR", \\
            start_node_key=("Person", "name", "family name"))

    These `start_node_key` and `end_node_key` arguments are interpreted
    in a similar way to the `merge_key` of :func:`merge_nodes`, except
    that the values are instead used to construct ``MATCH`` patterns.
    Additionally, passing :py:const:`None` indicates that a match by
    node ID should be used. The table below shows example combinations,
    where ``x`` is the input value drawn from the source data.

    .. table::
        :widths: 40 60

        =================================================  ===========================================================
        Argument                                           ``MATCH`` Clause
        =================================================  ===========================================================
        :py:const:`None`                                   ``MATCH (a) WHERE id(a) = x``
        ``("Person", "name")``                             ``MATCH (a:Person {name:x})``
        ``("Person", "name", "family name")``              ``MATCH (a:Person {name:x[0], `family name`:x[1]})``
        ``(("Person", "Female"), "name")``                 ``MATCH (a:Female:Person {name:x})``
        ``(("Person", "Female"), "name", "family name")``  ``MATCH (a:Female:Person {name:x[0], `family name`:x[1]})``
        =================================================  ===========================================================


    As with other methods, such as :func:`.create_nodes`, the
    relationship `data` can also be supplied as a list of property
    values, indexed by `keys`. This can avoid sending duplicated key
    names over the network, and alters the method call as follows:

        >>> data = [
            (("Alice", "Smith"), [1999], 123),
            (("Bob", "Jones"), [2002], 124),
            (("Carol", "Singer"), [1981], 201),
        ]
        >>> create_relationships(g.auto(), data, "WORKS_FOR" \\
            start_node_key=("Person", "name", "family name")), keys=["since"])

    :param tx: :class:`.Transaction` in which to carry out this
        operation
    :param data: relationship data supplied as a list of triples of
        `(start_node, detail, end_node)`
    :param rel_type: relationship type name to create
    :param start_node_key: optional tuple of (label, key1, key2...) on
        which to match relationship start nodes, matching by node ID
        if not provided
    :param end_node_key: optional tuple of (label, key1, key2...) on
        which to match relationship end nodes, matching by node ID
        if not provided
    :param keys: optional set of field names for the relationship
        `detail` (if supplied as value lists)
    :return:
    """
    list(tx.run(*unwind_create_relationships_query(
        data, rel_type, start_node_key, end_node_key, keys)))


def merge_relationships(tx, data, merge_key, start_node_key=None, end_node_key=None, keys=None):
    """ Merge relationships from an iterable sequence of raw
    relationship data.

    The `merge_key` argument operates according to the the same general
    principle as its namesake in :func:`.merge_nodes`, but instead of a
    variable number of labels, exactly one relationship type must be
    specified. This allows for the following input options:

    .. table::
        :widths: 40 60

        =======================================  ============================================================
        Argument                                 ``MERGE`` Clause
        =======================================  ============================================================
        ``"KNOWS"``                              ``MERGE (a)-[ab:KNOWS]->(b)``
        ``("KNOWS",)``                           ``MERGE (a)-[ab:KNOWS]->(b)``
        ``("KNOWS", "since")``                   ``MERGE (a)-[ab:KNOWS {since:$x}]->(b)``
        ``("KNOWS", "since", "introduced by")``  ``MERGE (a)-[ab:KNOWS {since:$x, `introduced by`:$y}]->(b)``
        =======================================  ============================================================

    For details on how the `start_node_key` and `end_node_key`
    arguments can be used, see :func:`.create_relationships`.

    :param tx: :class:`.Transaction` in which to carry out this
        operation
    :param data: relationship data supplied as a list of triples of
        `(start_node, detail, end_node)`
    :param merge_key: tuple of (rel_type, key1, key2...) on which to
        merge
    :param start_node_key: optional tuple of (label, key1, key2...) on
        which to match relationship start nodes, matching by node ID
        if not provided
    :param end_node_key: optional tuple of (label, key1, key2...) on
        which to match relationship end nodes, matching by node ID
        if not provided
    :param keys: optional set of field names for the relationship
        `detail` (if supplied as value lists)
    :return:
    """
    list(tx.run(*unwind_merge_relationships_query(
        data, merge_key, start_node_key, end_node_key, keys)))


class NodeSet:
    """
    Container for a set of Nodes with the same labels and the same properties that define uniqueness.

    :param data:
    :param merge_key: optional tuple of (label, key1, key2...) on which
        to :meth:`.merge`
    :param labels: secondary labels to add (not part of the merge key)
    :param keys:
    """

    batch_size = 1000

    def __init__(self, data=None, merge_key=None, labels=None, keys=None):
        self.__data = list(data or ())  # TODO: index for fast searching
        self.__merge_key = merge_key
        self.__labels = labels
        self.__keys = keys

    def __contains__(self, properties):
        return self.__normal(properties) in self.__data

    def __iter__(self):
        for properties in self.__data:
            yield properties

    def __len__(self):
        return len(self.__data)

    def __normal(self, properties):
        if self.__keys:
            if isinstance(properties, (tuple, list)):
                return properties
            elif isinstance(properties, dict):
                return [properties[key] for key in self.__keys]
            else:
                raise TypeError("Properties should be supplied as a dictionary, list or tuple")
        else:
            if isinstance(properties, dict):
                return properties
            else:
                raise TypeError("Properties should be supplied as a dictionary")

    @property
    def merge_key(self):
        return self.merge_key

    @property
    def labels(self):
        return self.labels

    def add(self, properties):
        """ Create a node in this NodeSet.

        :param properties: Node properties.
        :type properties: dict or list
        """
        self.__data.append(self.__normal(properties))

    def remove(self, properties):
        self.__data.remove(self.__normal(properties))

    def create(self, graph, batch_size=None):
        """
        Create all nodes from NodeSet.
        """
        if batch_size is None:
            batch_size = self.batch_size

        log.debug("Beginning bulk node create with batch size %r", batch_size)
        for n, batch in enumerate(_chunks(self.__data, size=batch_size), start=1):
            log.debug("Creating batch %r", n)
            create_nodes(graph.auto(), batch, self.__labels)
        log.debug("Bulk node create completed")

    def merge(self, graph, batch_size=None):
        """
        Merge nodes from NodeSet on merge properties.
        """
        if self.__merge_key is None:
            raise TypeError("Cannot merge nodes from a NodeSet defined without a merge key")

        if not batch_size:
            batch_size = self.batch_size

        log.debug("Beginning bulk node merge with batch size %r", batch_size)
        for n, batch in enumerate(_chunks(self.__data, size=batch_size), start=1):
            log.debug("Merging batch %r", n)
            merge_nodes(graph.auto(), batch, self.__merge_key, self.__labels)
        log.debug("Bulk node merge completed")


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
        self.triples = []

        self.unique = False
        self.unique_rels = set()

    def add(self, start_node, detail, end_node):
        self.triples.append((start_node, detail, end_node))

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
        start_node_key = (tuple(self.start_node_labels),) + tuple(self.start_node_properties)
        end_node_key = (tuple(self.end_node_labels),) + tuple(self.end_node_properties)

        # iterate over chunks of rels
        for i, batch in enumerate(_chunks(self.triples, size=batch_size), start=1):
            batch = list(batch)
            log.debug('Batch {}'.format(i))
            create_relationships(graph.auto(), batch, self.rel_type, start_node_key, end_node_key)

    def merge(self, graph, batch_size=None):
        """
        Create relationships in this RelationshipSet
        """
        log.debug('Merge RelationshipSet')
        if not batch_size:
            batch_size = self.batch_size
        log.debug('Batch Size: {}'.format(batch_size))

        # get query
        start_node_key = (tuple(self.start_node_labels),) + tuple(self.start_node_properties)
        end_node_key = (tuple(self.end_node_labels),) + tuple(self.end_node_properties)

        # iterate over chunks of rels
        for i, batch in enumerate(_chunks(self.triples, size=batch_size), start=1):
            batch = list(batch)
            log.debug('Batch {}'.format(i))
            merge_relationships(graph.auto(), batch, self.rel_type, start_node_key, end_node_key)

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
        return [o for o in self.objects if isinstance(o, NodeSet)]

    @property
    def relationshipsets(self):
        """
        Get the RelationshipSets in the Container.
        """
        return [o for o in self.objects if isinstance(o, RelationshipSet)]

    def get_nodeset(self, labels, merge_keys):
        for nodeset in self.nodesets:
            if set(nodeset.__labels) == set(labels) and set(nodeset.merge_keys) == set(merge_keys):
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
