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


def nodes_create_unwind(labels, property_parameter=None):
    """
    Generate a :code:`CREATE` query using :code:`UNWIND` for batch creation of nodes.::

        UNWIND $props AS properties CREATE (n:Gene) SET n = properties

    Pass the node properties as parameter to the query, e.g. with a :py:obj:`py2neo.Graph`::

        graph.run(query, props=[{'id': 1}, {'id': 2}, ...])

    You can optionally set the name of the parameter with the argument :code:`property_parameter`::

        query = nodes_create_unwind(['Foo'], query_parameter='mynodes')

        graph.run(query, mynodes=[{'id': 1}, {'id': 2}, ...])


    :param labels: Labels for the create query.
    :type labels: list[str]
    :param property_parameter: Optional name of the parameter used in the query. Default is 'props'.
    :type property_parameter: str
    :return: Query
    """
    if not property_parameter:
        property_parameter = 'props'

    return "UNWIND ${0} AS properties CREATE (n:{1}) SET n = properties".format(property_parameter, ":".join(labels))


def nodes_merge_unwind(labels, merge_properties, property_parameter=None):
    """
    Generate a :code:`MERGE` query which uses defined properties to :code:`MERGE` upon::

        UNWIND $props AS properties
        MERGE (n:Node {properties.sid: '1234'})
        ON CREATE SET n = properties
        ON MATCH SET n += properties

    The '+=' operator in ON MATCH updates the node properties provided and leaves the others untouched.

    Call with the labels and a list of properties to :code:`MERGE` on::

        query = nodes_merge_unwind(['Foo', 'Bar'], ['node_id'])

    Pass the node properties as parameter to the query, e.g. with a :py:obj:`py2neo.Graph`::

        graph.run(query, props=[{'node_id': 1}, {'node_id': 2}, ...])

    You can optionally set the name of the parameter with the argument :code:`property_parameter`::

        query = nodes_merge_unwind([['Foo', 'Bar'], ['node_id'], query_parameter='mynodes')

        graph.run(query, mynodes=[{'node_id': 1}, {'node_id': 2}, ...])


    :param labels: Labels for the query.
    :type labels: list[str]
    :param merge_properties: Unique properties for the node.
    :type merge_properties: list[str]
    :param property_parameter: Optional name of the parameter used in the query. Default is 'props'.
    :type property_parameter: str
    :return: Query
    """
    if not property_parameter:
        property_parameter = 'props'

    label_string = ':'.join(labels)

    merge_strings = []
    for u in merge_properties:
        merge_strings.append("{0}: properties.{0}".format(u))

    merge_string = ', '.join(merge_strings)

    q = "UNWIND ${0} AS properties\n" \
        "MERGE (n:{1} {{ {2} }} )\n" \
        "ON CREATE SET n = properties\n" \
        "ON MATCH SET n += properties".format(property_parameter, label_string, merge_string)

    return q


def query_create_rels_unwind(start_node_labels, end_node_labels, start_node_properties,
                             end_node_properties, rel_type, property_identifier=None):
    """
    Create relationship query with explicit arguments.

    UNWIND $rels AS rel
    MATCH (a:Gene), (b:GeneSymbol)
    WHERE a.sid = rel.start_sid AND b.sid = rel.end_sid AND b.taxid = rel.end_taxid
    CREATE (a)-[r:MAPS]->(b)
    SET r = rel.properties

    Call with params:
        {'start_sid': 1, 'end_sid': 2, 'end_taxid': '9606', 'properties': {'foo': 'bar} }

    Within UNWIND you cannot access nested dictionaries such as 'rel.start_node.sid'. Thus the
    parameters are created in a separate function.

    :param relationship: A Relationship object to create the query.
    :param property_identifier: The variable used in UNWIND.
    :return: Query
    """

    if not property_identifier:
        property_identifier = 'rels'

    start_node_label_string = ':'.join(start_node_labels)
    end_node_label_string = ':'.join(end_node_labels)

    q = "UNWIND ${0} AS rel \n".format(property_identifier)
    q += "MATCH (a:{0}), (b:{1}) \n".format(start_node_label_string, end_node_label_string)

    # collect WHERE clauses
    where_clauses = []
    for property in start_node_properties:
        where_clauses.append('a.{0} = rel.start_{0}'.format(property))
    for property in end_node_properties:
        where_clauses.append('b.{0} = rel.end_{0}'.format(property))

    q += "WHERE " + ' AND '.join(where_clauses) + " \n"

    q += "CREATE (a)-[r:{0}]->(b) \n".format(rel_type)
    q += "SET r = rel.properties RETURN count(r)\n"

    return q


def query_merge_rels_unwind(start_node_labels, end_node_labels, start_node_properties,
                             end_node_properties, rel_type, property_identifier=None):
    """
    Merge relationship query with explicit arguments.

    Note: The MERGE on relationships does not take relationship properties into account!

    UNWIND $rels AS rel
    MATCH (a:Gene), (b:GeneSymbol)
    WHERE a.sid = rel.start_sid AND b.sid = rel.end_sid AND b.taxid = rel.end_taxid
    MERGE (a)-[r:MAPS]->(b)
    SET r = rel.properties

    Call with params:
        {'start_sid': 1, 'end_sid': 2, 'end_taxid': '9606', 'properties': {'foo': 'bar} }

    Within UNWIND you cannot access nested dictionaries such as 'rel.start_node.sid'. Thus the
    parameters are created in a separate function.

    :param relationship: A Relationship object to create the query.
    :param property_identifier: The variable used in UNWIND.
    :return: Query
    """

    if not property_identifier:
        property_identifier = 'rels'

    start_node_label_string = ':'.join(start_node_labels)
    end_node_label_string = ':'.join(end_node_labels)

    q = "UNWIND ${0} AS rel \n".format(property_identifier)
    q += "MATCH (a:{0}), (b:{1}) \n".format(start_node_label_string, end_node_label_string)

    # collect WHERE clauses
    where_clauses = []
    for property in start_node_properties:
        where_clauses.append('a.{0} = rel.start_{0}'.format(property))
    for property in end_node_properties:
        where_clauses.append('b.{0} = rel.end_{0}'.format(property))

    q += "WHERE " + ' AND '.join(where_clauses) + " \n"

    q += "MERGE (a)-[r:{0}]->(b) \n".format(rel_type)
    q += "SET r = rel.properties RETURN count(r)\n"

    return q
