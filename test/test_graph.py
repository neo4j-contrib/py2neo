#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2016, Nigel Small
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


from py2neo.graph import Graph
from py2neo.json import JSONValueSystem
from py2neo.remoting import remote
from py2neo.types import Node, Relationship, cast_node

from test.util import GraphTestCase
from test.compat import patch


class GraphObjectTestCase(GraphTestCase):

    def test_can_create_graph_with_trailing_slash(self):
        uri = "http://localhost:7474/db/data/"
        graph = Graph(uri)
        assert remote(graph).uri == uri
        index = remote(graph).get().content
        assert "node" in index

    def test_can_create_graph_without_trailing_slash(self):
        uri = "http://localhost:7474/db/data/"
        graph = Graph(uri[:-1])
        assert remote(graph).uri == uri
        index = remote(graph).get().content
        assert "node" in index

    def test_same_uri_gives_same_instance(self):
        uri = "http://localhost:7474/db/data/"
        graph_1 = Graph(uri)
        graph_2 = Graph(uri)
        assert graph_1 is graph_2

    def test_graph_len_returns_number_of_rels(self):
        size = len(self.graph)
        statement = "MATCH ()-[r]->() RETURN COUNT(r)"
        num_rels = self.graph.evaluate(statement)
        assert size == num_rels

    def test_graph_bool_returns_true(self):
        assert self.graph.__bool__()
        assert self.graph.__nonzero__()

    def test_graph_contains(self):
        node = Node()
        self.graph.create(node)
        assert node in self.graph

    def test_can_hydrate_map_from_json_result(self):
        # TODO: check that a warning is raised
        data = {"foo": "bar"}
        value_system = JSONValueSystem(self.graph, ["a"])
        hydrated = value_system.hydrate([data])
        assert hydrated[0] == data

    def test_can_open_browser(self):
        with patch("webbrowser.open") as mocked:
            self.graph.open_browser()
            assert mocked.called_once_with(remote(self.graph.dbms).uri)

    def test_graph_is_not_equal_to_non_graph(self):
        graph = Graph()
        assert graph != object()

    def test_can_create_and_delete_node(self):
        a = Node()
        self.graph.create(a)
        assert isinstance(a, Node)
        assert remote(a)
        assert self.graph.exists(a)
        self.graph.delete(a)
        assert not self.graph.exists(a)

    def test_can_create_and_delete_relationship(self):
        ab = Relationship(Node(), "KNOWS", Node())
        self.graph.create(ab)
        assert isinstance(ab, Relationship)
        assert remote(ab)
        assert self.graph.exists(ab)
        self.graph.delete(ab | ab.start_node() | ab.end_node())
        assert not self.graph.exists(ab)

    def test_can_get_node_by_id_when_cached(self):
        node = Node()
        self.graph.create(node)
        assert remote(node).uri in Node.cache
        got = self.graph.node(remote(node)._id)
        assert got is node

    def test_can_get_node_by_id_when_not_cached(self):
        node = Node()
        self.graph.create(node)
        Node.cache.clear()
        assert remote(node).uri not in Node.cache
        got = self.graph.node(remote(node)._id)
        assert remote(got)._id == remote(node)._id

    def test_get_non_existent_node_by_id(self):
        node = Node()
        self.graph.create(node)
        node_id = remote(node)._id
        self.graph.delete(node)
        Node.cache.clear()
        with self.assertRaises(IndexError):
            _ = self.graph.node(node_id)

    def test_node_cache_is_thread_local(self):
        from threading import Thread
        node = Node()
        self.graph.create(node)
        assert remote(node).uri in Node.cache
        other_cache_keys = []

        def check_cache():
            other_cache_keys.extend(Node.cache.keys())

        thread = Thread(target=check_cache)
        thread.start()
        thread.join()

        assert remote(node).uri in Node.cache
        assert remote(node).uri not in other_cache_keys

    def test_graph_hashes(self):
        assert hash(self.graph) == hash(self.graph)

    def test_graph_repr(self):
        assert repr(self.graph).startswith("<Graph")

    def test_can_get_same_instance(self):
        graph_1 = Graph()
        graph_2 = Graph()
        assert graph_1 is graph_2

    def test_create_single_empty_node(self):
        a = Node()
        self.graph.create(a)
        assert remote(a)

    def test_get_node_by_id(self):
        a1 = Node(foo="bar")
        self.graph.create(a1)
        a2 = self.graph.node(remote(a1)._id)
        assert a1 == a2

    def test_create_node_with_mixed_property_types(self):
        a = cast_node({"number": 13, "foo": "bar", "true": False, "fish": "chips"})
        self.graph.create(a)
        assert len(a) == 4
        assert a["fish"] == "chips"
        assert a["foo"] == "bar"
        assert a["number"] == 13
        assert not a["true"]

    def test_create_node_with_null_properties(self):
        a = cast_node({"foo": "bar", "no-foo": None})
        self.graph.create(a)
        assert a["foo"] == "bar"
        assert a["no-foo"] is None
