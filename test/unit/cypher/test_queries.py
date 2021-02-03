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

from pytest import mark, fixture

from py2neo.cypher.queries import (
    unwind_create_nodes_query,
    unwind_merge_nodes_query,
    unwind_create_relationships_query,
    unwind_merge_relationships_query,
)


@fixture
def node_keys():
    return ["name", "family name", "age"]


@fixture
def node_data_lists():
    return [
        ["Alice", "Allison", 33],
        ["Bob", "Robertson", 44],
        ["Carol", "Carlson", 55],
        ["Alice", "Smith", 66],
    ]


@fixture
def node_data_dicts(node_data_lists, node_keys):
    data = [OrderedDict(zip(node_keys, a)) for a in node_data_lists]
    data[1]["nickname"] = "Bobby"
    del data[2]["age"]
    return data


@fixture
def rel_type():
    return "WORKS_FOR"


@fixture
def rel_keys():
    return ["employee id", "job title", "since"]


@fixture
def rel_data_lists():
    return [
        ("Alice", [1, "CEO", 1990], "ACME"),
        ("Bob", [123, "PA", 1999], "ACME"),
        ("Carol", [555, "Programmer", 2010], "Foo Corp"),
    ]


@fixture
def rel_data_lists_double_key():
    return [
        (("Alice", "Smith"), [1, "CEO", 1990], "ACME"),
        (("Bob", "Jones"), [123, "PA", 1999], "ACME"),
        (("Carol", "Brown"), [555, "Programmer", 2010], "Foo Corp"),
    ]


@fixture
def rel_data_lists_no_key():
    return [
        ("Alice", [1, "CEO", 1990], None),
        ("Bob", [123, "PA", 1999], None),
        ("Carol", [555, "Programmer", 2010], None),
    ]


@fixture
def rel_data_dicts(rel_data_lists, rel_keys):
    data = [(a, OrderedDict(zip(rel_keys, r)), b) for a, r, b in rel_data_lists]
    return data


@fixture
def start_node_key():
    return "Person", "name"


@fixture
def start_node_double_key():
    return "Person", "name", "family name"


@fixture
def end_node_key():
    return "Company", "name"


class TestUnwindCreateNodesQuery(object):

    def test_dict_data(self, node_data_dicts):
        q, p = unwind_create_nodes_query(node_data_dicts)
        assert q == ("UNWIND $data AS r\n"
                     "CREATE (_)\n"
                     "SET _ += r")
        assert p == {"data": node_data_dicts}

    def test_list_data(self, node_data_lists, node_keys):
        q, p = unwind_create_nodes_query(node_data_lists, keys=node_keys)
        assert q == ("UNWIND $data AS r\n"
                     "CREATE (_)\n"
                     "SET _ += {name: r[0], `family name`: r[1], age: r[2]}")
        assert p == {"data": node_data_lists}

    def test_with_one_label(self, node_data_dicts):
        q, p = unwind_create_nodes_query(node_data_dicts, labels=["Person"])
        assert q == ("UNWIND $data AS r\n"
                     "CREATE (_:Person)\n"
                     "SET _ += r")
        assert p == {"data": node_data_dicts}

    def test_with_two_labels(self, node_data_dicts):
        q, p = unwind_create_nodes_query(node_data_dicts, labels=["Person", "Employee"])
        assert q == ("UNWIND $data AS r\n"
                     "CREATE (_:Employee:Person)\n"
                     "SET _ += r")
        assert p == {"data": node_data_dicts}


class TestUnwindMergeNodesQuery(object):

    def test_dict_data(self, node_data_dicts):
        q, p = unwind_merge_nodes_query(node_data_dicts, ("Person", "name"))
        assert q == ("UNWIND $data AS r\n"
                     "MERGE (_:Person {name:r['name']})\n"
                     "SET _ += r")
        assert p == {"data": node_data_dicts}

    def test_list_data(self, node_data_lists, node_keys):
        q, p = unwind_merge_nodes_query(node_data_lists, ("Person", "name"), keys=node_keys)
        assert q == ("UNWIND $data AS r\n"
                     "MERGE (_:Person {name:r[0]})\n"
                     "SET _ += {name: r[0], `family name`: r[1], age: r[2]}")
        assert p == {"data": node_data_lists}

    def test_with_extra_labels(self, node_data_dicts):
        q, p = unwind_merge_nodes_query(node_data_dicts, ("Person", "name"),
                                        labels=["Human", "Employee"])
        assert q == ("UNWIND $data AS r\n"
                     "MERGE (_:Person {name:r['name']})\n"
                     "SET _:Employee:Human\n"
                     "SET _ += r")
        assert p == {"data": node_data_dicts}

    @mark.parametrize("merge_key", ["Person", ("Person",)])
    def test_with_no_merge_keys(self, node_data_dicts, merge_key):
        q, p = unwind_merge_nodes_query(node_data_dicts, "Person")
        assert q == ("UNWIND $data AS r\n"
                     "MERGE (_:Person)\n"
                     "SET _ += r")
        assert p == {"data": node_data_dicts}

    def test_with_one_merge_key(self, node_data_dicts):
        q, p = unwind_merge_nodes_query(node_data_dicts, ("Person", "name"))
        assert q == ("UNWIND $data AS r\n"
                     "MERGE (_:Person {name:r['name']})\n"
                     "SET _ += r")
        assert p == {"data": node_data_dicts}

    def test_with_two_merge_keys(self, node_data_dicts):
        q, p = unwind_merge_nodes_query(node_data_dicts, ("Person", "name", "family name"))
        assert q == ("UNWIND $data AS r\n"
                     "MERGE (_:Person {name:r['name'], `family name`:r['family name']})\n"
                     "SET _ += r")
        assert p == {"data": node_data_dicts}


class TestUnwindCreateRelationshipsQuery(object):

    def test_dict_data(self, rel_data_dicts, rel_type):
        q, p = unwind_create_relationships_query(rel_data_dicts, rel_type)
        assert q == ("UNWIND $data AS r\n"
                     "MATCH (a) WHERE id(a) = r[0]\n"
                     "MATCH (b) WHERE id(b) = r[2]\n"
                     "CREATE (a)-[_:WORKS_FOR]->(b)\n"
                     "SET _ += r[1]")
        assert p == {"data": rel_data_dicts}

    def test_list_data(self, rel_data_lists, rel_type, rel_keys):
        q, p = unwind_create_relationships_query(rel_data_lists, rel_type, keys=rel_keys)
        assert q == ("UNWIND $data AS r\n"
                     "MATCH (a) WHERE id(a) = r[0]\n"
                     "MATCH (b) WHERE id(b) = r[2]\n"
                     "CREATE (a)-[_:WORKS_FOR]->(b)\n"
                     "SET _ += {`employee id`: r[1][0], `job title`: r[1][1], since: r[1][2]}")
        assert p == {"data": rel_data_lists}

    def test_with_start_node_key(self, rel_data_dicts, rel_type, start_node_key):
        q, p = unwind_create_relationships_query(rel_data_dicts, rel_type,
                                                 start_node_key=start_node_key)
        assert q == ("UNWIND $data AS r\n"
                     "MATCH (a:Person {name:r[0]})\n"
                     "MATCH (b) WHERE id(b) = r[2]\n"
                     "CREATE (a)-[_:WORKS_FOR]->(b)\n"
                     "SET _ += r[1]")
        assert p == {"data": rel_data_dicts}

    def test_with_end_node_key(self, rel_data_dicts, rel_type, end_node_key):
        q, p = unwind_create_relationships_query(rel_data_dicts, rel_type,
                                                 end_node_key=end_node_key)
        assert q == ("UNWIND $data AS r\n"
                     "MATCH (a) WHERE id(a) = r[0]\n"
                     "MATCH (b:Company {name:r[2]})\n"
                     "CREATE (a)-[_:WORKS_FOR]->(b)\n"
                     "SET _ += r[1]")
        assert p == {"data": rel_data_dicts}

    def test_with_start_and_end_node_keys(self, rel_data_dicts, rel_type,
                                          start_node_key, end_node_key):
        q, p = unwind_create_relationships_query(rel_data_dicts, rel_type,
                                                 start_node_key=start_node_key,
                                                 end_node_key=end_node_key)
        assert q == ("UNWIND $data AS r\n"
                     "MATCH (a:Person {name:r[0]})\n"
                     "MATCH (b:Company {name:r[2]})\n"
                     "CREATE (a)-[_:WORKS_FOR]->(b)\n"
                     "SET _ += r[1]")
        assert p == {"data": rel_data_dicts}

    def test_with_start_node_double_key(self, rel_data_lists_double_key, rel_keys,
                                        rel_type, start_node_double_key):
        q, p = unwind_create_relationships_query(rel_data_lists_double_key, rel_type,
                                                 start_node_key=start_node_double_key,
                                                 keys=rel_keys)
        assert q == ("UNWIND $data AS r\n"
                     "MATCH (a:Person {name:r[0][0], `family name`:r[0][1]})\n"
                     "MATCH (b) WHERE id(b) = r[2]\n"
                     "CREATE (a)-[_:WORKS_FOR]->(b)\n"
                     "SET _ += {`employee id`: r[1][0], `job title`: r[1][1], since: r[1][2]}")
        assert p == {"data": rel_data_lists_double_key}

    def test_with_start_node_no_keys(self, rel_data_lists_no_key, rel_type, rel_keys,
                                     start_node_key):
        q, p = unwind_create_relationships_query(rel_data_lists_no_key, rel_type,
                                                 start_node_key=start_node_key,
                                                 end_node_key="Company", keys=rel_keys)
        assert q == ("UNWIND $data AS r\n"
                     "MATCH (a:Person {name:r[0]})\n"
                     "MATCH (b:Company)\n"
                     "CREATE (a)-[_:WORKS_FOR]->(b)\n"
                     "SET _ += {`employee id`: r[1][0], `job title`: r[1][1], since: r[1][2]}")
        assert p == {"data": rel_data_lists_no_key}


class TestUnwindMergeRelationshipsQuery(object):

    def test_dict_data(self, rel_data_dicts, rel_type):
        q, p = unwind_merge_relationships_query(rel_data_dicts, ("WORKS_FOR", "employee id"))
        assert q == ("UNWIND $data AS r\n"
                     "MATCH (a) WHERE id(a) = r[0]\n"
                     "MATCH (b) WHERE id(b) = r[2]\n"
                     "MERGE (a)-[_:WORKS_FOR {`employee id`:r[1]['employee id']}]->(b)\n"
                     "SET _ += r[1]")
        assert p == {"data": rel_data_dicts}

    def test_list_data(self, rel_data_lists, rel_type, rel_keys):
        q, p = unwind_merge_relationships_query(rel_data_lists, ("WORKS_FOR", "employee id"),
                                                keys=rel_keys)
        assert q == ("UNWIND $data AS r\n"
                     "MATCH (a) WHERE id(a) = r[0]\n"
                     "MATCH (b) WHERE id(b) = r[2]\n"
                     "MERGE (a)-[_:WORKS_FOR {`employee id`:r[1][0]}]->(b)\n"
                     "SET _ += {`employee id`: r[1][0], `job title`: r[1][1], since: r[1][2]}")
        assert p == {"data": rel_data_lists}

    def test_with_start_node_key(self, rel_data_dicts, rel_type, start_node_key):
        q, p = unwind_merge_relationships_query(rel_data_dicts, ("WORKS_FOR", "employee id"),
                                                start_node_key=start_node_key)
        assert q == ("UNWIND $data AS r\n"
                     "MATCH (a:Person {name:r[0]})\n"
                     "MATCH (b) WHERE id(b) = r[2]\n"
                     "MERGE (a)-[_:WORKS_FOR {`employee id`:r[1]['employee id']}]->(b)\n"
                     "SET _ += r[1]")
        assert p == {"data": rel_data_dicts}

    def test_with_end_node_key(self, rel_data_dicts, rel_type, end_node_key):
        q, p = unwind_merge_relationships_query(rel_data_dicts, ("WORKS_FOR", "employee id"),
                                                end_node_key=end_node_key)
        assert q == ("UNWIND $data AS r\n"
                     "MATCH (a) WHERE id(a) = r[0]\n"
                     "MATCH (b:Company {name:r[2]})\n"
                     "MERGE (a)-[_:WORKS_FOR {`employee id`:r[1]['employee id']}]->(b)\n"
                     "SET _ += r[1]")
        assert p == {"data": rel_data_dicts}

    def test_with_start_and_end_node_keys(self, rel_data_dicts, rel_type,
                                          start_node_key, end_node_key):
        q, p = unwind_merge_relationships_query(rel_data_dicts, ("WORKS_FOR", "employee id"),
                                                start_node_key=start_node_key,
                                                end_node_key=end_node_key)
        assert q == ("UNWIND $data AS r\n"
                     "MATCH (a:Person {name:r[0]})\n"
                     "MATCH (b:Company {name:r[2]})\n"
                     "MERGE (a)-[_:WORKS_FOR {`employee id`:r[1]['employee id']}]->(b)\n"
                     "SET _ += r[1]")
        assert p == {"data": rel_data_dicts}
