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


from pytest import skip

from py2neo import GraphService, Graph


def test_can_generate_graph(graph_service):
    graph = graph_service["data"]
    assert isinstance(graph, Graph)


def test_repr(graph_service):
    assert repr(graph_service).startswith("<GraphService uri=")


def test_same_uri_gives_same_instance(graph_service):
    uri = graph_service.uri
    gs1 = GraphService(uri)
    gs2 = GraphService(uri)
    assert gs1 is gs2


def test_graph_service_equality(graph_service):
    uri = graph_service.uri
    gs1 = GraphService(uri)
    gs2 = GraphService(uri)
    assert gs1 == gs2
    assert hash(gs1) == hash(gs2)


def test_graph_service_is_not_equal_to_non_graph_service(graph_service):
    assert graph_service != object()


def test_graph_service_metadata(graph_service):
    try:
        assert graph_service.kernel_start_time
        assert graph_service.kernel_version
        assert graph_service.store_creation_time
        assert graph_service.store_id
        assert graph_service.primitive_counts
        assert graph_service.store_file_sizes
        assert graph_service.config
    except NotImplementedError:
        skip("JMX data not available in this version of Neo4j")


def test_graph_service_name(graph_service):
    if graph_service.name is None:
        skip("Graph service name not available in this version of Neo4j")
    else:
        assert graph_service.name == "graph.db"


def test_kernel_version(graph_service):
    try:
        version = graph_service.kernel_version
    except NotImplementedError:
        skip("Kernel version not available in this version of Neo4j")
    else:
        assert isinstance(version, tuple)
        assert 3 <= len(version) <= 4
        assert isinstance(version[0], int)
        assert isinstance(version[1], int)
        assert isinstance(version[2], int)


def test_can_get_set_of_graphs_in_service(graph_service):
    assert set(graph_service) == {"data"}


def test_can_get_dictionary_of_graphs(graph_service):
    graphs = dict(graph_service)
    assert len(graphs) == 1
    assert graphs["data"] is graph_service.default_graph
