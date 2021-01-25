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


import pytest

from py2neo.bulk import NodeSet
from py2neo.compat import uchr


def create_node_set(size):
    import unicodedata
    ns = NodeSet(merge_key=("Unicode", "code_point"))
    for cp in range(0, size):
        u = uchr(cp)
        data = {"code_point": cp,
                "category": unicodedata.category(u)}
        for attr in ["name", "decimal", "digit", "numeric"]:
            f = getattr(unicodedata, attr)
            try:
                value = f(u)
            except ValueError:
                pass
            else:
                data[attr] = value
        ns.add(data)
    return ns


@pytest.fixture
def ascii_node_set():
    return create_node_set(0x100)


@pytest.fixture
def unicode_node_set():
    return create_node_set(0x110000)


@pytest.fixture
def small_nodeset(make_unique_id):
    label = make_unique_id()
    ns = NodeSet(merge_key=(label, "uuid"))
    for i in range(100):
        ns.add({'uuid': i, 'key': 'value'})

    return ns


@pytest.fixture
def nodeset_multiple_labels():
    ns = NodeSet(merge_key=(("Test", "Foo", "Bar"), "uuid"))
    for i in range(100):
        ns.add({'uuid': i})

    return ns


class TestNodeSet:
    """
    Test basic function such as adding nodes.
    """
    def test_item_iterator(self, small_nodeset):
        for i in small_nodeset:
            assert i['key'] == 'value'
            assert isinstance(i['uuid'], int)

    def test_create(self, ascii_node_set, clear_graph):
        ascii_node_set.create(clear_graph)
        assert clear_graph.nodes.match("Unicode").count() == len(ascii_node_set)
        assert clear_graph.evaluate("MATCH (u:Unicode) WHERE u.code_point == 48 RETURN map(u)") == {
            "code_point": 48,
        }

    def test_create_twice(self, unicode_node_set, clear_graph):
        unicode_node_set.create(clear_graph)
        unicode_node_set.create(clear_graph)
        assert clear_graph.nodes.match("Unicode").count() == 2 * len(unicode_node_set)

    def test__create_properties(self, small_nodeset, clear_graph):
        small_nodeset.create(clear_graph)
        for row in clear_graph.run("MATCH (n) RETURN n"):
            node = row[0]
            assert node['key'] == 'value'

    def test_create_nodeset_multiple_labels(self, nodeset_multiple_labels, clear_graph):
        nodeset_multiple_labels.create(clear_graph)
        assert clear_graph.nodes.match().count() == len(small_nodeset)


class TestNodeSetIndex:

    def test_nodeset_create_single_index(self, clear_graph):
        labels = ['TestNode']
        properties = ['some_key']
        ns = NodeSet(merge_key=("TestNode", "some_key"))

        ns.create_index(clear_graph)

        result = list(
            clear_graph.run("CALL db.indexes()")
        )

        for row in result:
            # the result of the db.indexes() procedure is different for Neo4j 3.5 and 4
            # this should also be synced with differences in py2neo versions
            if 'tokenNames' in row:
                assert row['tokenNames'] == labels and row['properties'] == properties \
                       or row['tokenNames'] == labels and row['properties'] == properties

            elif 'labelsOrTypes' in row:
                assert row['labelsOrTypes'] == labels and row['properties'] == properties \
                       or row['labelsOrTypes'] == labels and row['properties'] == properties

    def test_nodeset_create_composite_index(self, clear_graph):
        labels = ['TestNode']
        properties = ['some_key', 'other_key']
        ns = NodeSet(merge_key=("TestNode", "some_key", "other_key"))

        ns.create_index(clear_graph)

        result = list(
            clear_graph.run("CALL db.indexes()")
        )

        for row in result:
            # the result of the db.indexes() procedure is different for Neo4j 3.5 and 4
            # this should also be synced with differences in py2neo versions
            if 'tokenNames' in row:
                assert row['tokenNames'] == labels and row['properties'] == properties \
                       or row['tokenNames'] == labels and row['properties'] == properties

            elif 'labelsOrTypes' in row:
                assert row['labelsOrTypes'] == labels and row['properties'] == properties \
                       or row['labelsOrTypes'] == labels and row['properties'] == properties

    def test_nodeset_recreate_existing_single_index(self, clear_graph):
        """
        The output/error when you try to recreate an existing index is different in Neo4j 3.5 and 4.

        Create an index a few times to make sure this error is handled.
        """
        labels = ['TestNode']
        properties = ['some_key']
        ns = NodeSet(merge_key=("TestNode", "some_key"))

        ns.create_index(clear_graph)
        ns.create_index(clear_graph)
        ns.create_index(clear_graph)


class TestNodeSetMerge:
    def test_nodeset_merge_number(self, small_nodeset, clear_graph):
        """
        Merge a nodeset 3 times and check number of nodes.
        """
        small_nodeset.merge(clear_graph)
        small_nodeset.merge(clear_graph)
        small_nodeset.merge(clear_graph)

        result = list(
            clear_graph.run("MATCH (n) RETURN count(n)")
        )

        print(result)
        assert result[0][0] == 100
