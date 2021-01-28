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


from py2neo.bulk import create_nodes, merge_nodes


HEADERS = ["name", "age"]
DATA_LISTS = [
    ["Alice", 33],
    ["Bob", 44],
    ["Carol", 55],
    ["Alice", 66],
]
DATA_DICTS = [dict(zip(HEADERS, record)) for record in DATA_LISTS]


def test_create_nodes_from_property_lists(graph):
    graph.delete_all()
    with graph.begin() as tx:
        create_nodes(tx, DATA_LISTS, labels={"Person", "Employee"}, keys=HEADERS)
    matched = graph.nodes.match("Person")
    assert matched.count() == 4
    assert all(node.labels == {"Person", "Employee"} for node in matched)


def test_create_nodes_from_property_dicts(graph):
    graph.delete_all()
    with graph.begin() as tx:
        create_nodes(tx, DATA_DICTS, labels={"Person", "Employee"})
    matched = graph.nodes.match("Person")
    assert matched.count() == 4
    assert all(node.labels == {"Person", "Employee"} for node in matched)


def test_merge_nodes_from_property_lists(graph):
    graph.delete_all()
    with graph.begin() as tx:
        merge_nodes(tx, DATA_LISTS, ("Person", "name"),
                    labels={"Person", "Employee"}, keys=HEADERS)
    matched = graph.nodes.match("Person")
    assert matched.count() == 3
    assert all(node.labels == {"Person", "Employee"} for node in matched)


def test_merge_nodes_from_property_dicts(graph):
    graph.delete_all()
    with graph.begin() as tx:
        merge_nodes(tx, DATA_DICTS, ("Person", "name"),
                    labels={"Person", "Employee"})
    matched = graph.nodes.match("Person")
    assert matched.count() == 3
    assert all(node.labels == {"Person", "Employee"} for node in matched)
