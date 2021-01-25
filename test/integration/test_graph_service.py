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


from pytest import skip, raises

from py2neo import GraphService, Graph


def test_can_generate_graph(graph_service):
    graph = graph_service[None]
    assert isinstance(graph, Graph)


def test_graph_service_equality(graph_service):
    uri = graph_service.uri
    gs1 = GraphService(uri)
    gs2 = GraphService(uri)
    assert gs1 == gs2
    assert hash(gs1) == hash(gs2)


def test_graph_service_is_not_equal_to_non_graph_service(graph_service):
    assert graph_service != object()


def test_graph_service_kernel_version(graph_service):
    assert graph_service.kernel_version


def test_graph_service_product(graph_service):
    assert graph_service.product


def test_graph_service_config(graph_service):
    assert graph_service.config


def test_kernel_version(graph_service):
    try:
        version = graph_service.kernel_version
    except NotImplementedError:
        skip("Kernel version not available in this version of Neo4j")
    else:
        assert version


def test_can_get_set_of_graphs_in_service(graph_service):
    graph_names = set(graph_service)
    assert (graph_names == set() or                 # Neo4j 3.x
            graph_names == {"neo4j", "system"})     # Neo4j 4.x+


def test_valid_graph_name(graph_service):
    graph_names = set(graph_service)
    if "neo4j" in graph_names:
        _ = graph_service["neo4j"]
    else:
        skip("Multi-database not available")


def test_invalid_graph_name(graph_service):
    with raises(KeyError):
        _ = graph_service["x"]
