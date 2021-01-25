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


from collections import OrderedDict

from py2neo.cypher import cypher_join, cypher_escape, cypher_repr, CypherExpression


def unwind_create_nodes_query(data, labels=None, keys=None):
    """ Generate a parameterised ``UNWIND...CREATE`` query for bulk
    loading nodes into Neo4j.

    :param data:
    :param labels:
    :param keys:
    :return: (query, parameters) tuple
    """
    return cypher_join("UNWIND $data AS r",
                       "CREATE (_%s)" % _label_string(*labels or ()),
                       _set_properties_clause("r", keys),
                       data=list(data))


def unwind_merge_nodes_query(data, merge_key, labels=None, keys=None):
    """ Generate a parameterised ``UNWIND...MERGE`` query for bulk
    loading nodes into Neo4j.

    :param data:
    :param merge_key:
    :param labels:
    :param keys:
    :return: (query, parameters) tuple
    """
    return cypher_join("UNWIND $data AS r",
                       _merge_clause("r", merge_key, keys, "(", ")"),
                       _set_labels_clause(labels),
                       _set_properties_clause("r", keys),
                       data=list(data))


def unwind_create_relationships_query(data, rel_type, keys=None,
                                      start_node_key=None, end_node_key=None):
    """ Generate a parameterised ``UNWIND...CREATE`` query for bulk
    loading relationships into Neo4j.

    :param data:
    :param rel_type:
    :param keys:
    :param start_node_key:
    :param end_node_key:
    :return: (query, parameters) tuple
    """
    return cypher_join("UNWIND $data AS r",
                       _match_clause("r[0]", "a", start_node_key),
                       _match_clause("r[2]", "b", end_node_key),
                       "CREATE (a)-[_:%s]->(b)" % cypher_escape(rel_type),
                       _set_properties_clause("r[1]", keys),
                       data=list(data))


def unwind_merge_relationships_query(data, merge_key, keys=None,
                                     start_node_key=None, end_node_key=None):
    """ Generate a parameterised ``UNWIND...MERGE`` query for bulk
    loading relationships into Neo4j.

    :param data:
    :param merge_key: tuple of (rel_type, key1, key2...)
    :param keys:
    :param start_node_key:
    :param end_node_key:
    :return: (query, parameters) tuple
    """
    return cypher_join("UNWIND $data AS r",
                       _match_clause("r[0]", "a", start_node_key),
                       _match_clause("r[2]", "b", end_node_key),
                       _merge_clause("r[1]", merge_key, keys, "(a)-[", "]->(b)"),
                       _set_properties_clause("r[1]", keys),
                       data=list(data))


def _label_string(*labels):
    label_set = set(labels or ())
    return "".join(":" + cypher_escape(label) for label in sorted(label_set))


def _unpack_merge_key(merge_key):
    if isinstance(merge_key, tuple):
        return merge_key[0], merge_key[1:]
    else:
        return merge_key, ()


def _match_clause(value, name, node_key):
    if node_key is None:
        # ... add MATCH by id clause
        return "MATCH (%s) WHERE id(%s) = %s" % (name, name, value)
    else:
        # ... add MATCH by label/property clause
        pl, pk = _unpack_merge_key(node_key)
        n_pk = len(pk)
        if n_pk == 0:
            return "MATCH (%s:%s)" % (name, cypher_escape(pl))
        elif n_pk == 1:
            return "MATCH (%s:%s {%s:%s})" % (name, cypher_escape(pl), cypher_escape(pk[0]), value)
        else:
            match_key_string = ", ".join("%s:%s[%s]" % (cypher_escape(key), value, j)
                                         for j, key in enumerate(pk))
            return "MATCH (%s:%s {%s})" % (name, cypher_escape(pl), match_key_string)


def _merge_clause(value, merge_key, keys, prefix, suffix):
    pl, pk = _unpack_merge_key(merge_key)
    if keys is None:
        ix = list(pk)
    else:
        ix = [keys.index(key) for key in pk]
    merge_key_string = ", ".join("%s:%s[%s]" % (cypher_escape(key), value, cypher_repr(ix[i]))
                                 for i, key in enumerate(pk))
    if merge_key_string:
        return "MERGE %s_:%s {%s}%s" % (prefix, cypher_escape(pl), merge_key_string, suffix)
    else:
        return "MERGE %s_:%s%s" % (prefix, cypher_escape(pl), suffix)


def _set_labels_clause(labels):
    if labels:
        return "SET _%s" % _label_string(*labels)
    else:
        return None


def _set_properties_clause(value, keys):
    if keys is None:
        # data is list of dicts
        return "SET _ = %s" % value
    else:
        # data is list of lists
        fields = [CypherExpression("%s[%d]" % (value, i)) for i in range(len(keys))]
        return "SET _ = " + cypher_repr(OrderedDict(zip(keys, fields)))
