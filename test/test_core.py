#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2014, Nigel Small
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

try:
    from unittest.mock import Mock, patch
except ImportError:
    from mock import Mock, patch

from py2neo import Graph, Node, Relationship, GraphError
from test.util import DatabaseTestCase
from py2neo.packages.httpstream import ClientError, Resource as _Resource


class DodgyClientError(ClientError):
    status_code = 499


class GraphTestCase(DatabaseTestCase):

    def test_can_create_graph_with_trailing_slash(self):
        uri = "http://localhost:7474/db/data/"
        graph = Graph(uri)
        assert graph.uri == uri
        index = graph.resource.get().content
        assert "node" in index

    def test_can_create_graph_without_trailing_slash(self):
        uri = "http://localhost:7474/db/data/"
        graph = Graph(uri[:-1])
        assert graph.uri == uri
        index = graph.resource.get().content
        assert "node" in index

    def test_same_uri_gives_same_instance(self):
        uri = "http://localhost:7474/db/data/"
        graph_1 = Graph(uri)
        graph_2 = Graph(uri)
        assert graph_1 is graph_2

    def test_graph_len_returns_number_of_rels(self):
        size = len(self.graph)
        statement = "MATCH ()-[r]->() RETURN COUNT(r)"
        num_rels = self.cypher.execute_one(statement)
        assert size == num_rels

    def test_graph_bool_returns_true(self):
        assert self.graph.__bool__()
        assert self.graph.__nonzero__()

    def test_can_hydrate_graph(self):
        data = self.graph.resource.get().content
        hydrated = self.graph.hydrate(data)
        assert hydrated is self.graph

    def test_graph_contains(self):
        node, = self.graph.create({})
        assert node in self.graph

    def test_can_hydrate_map(self):
        data = {"foo": "bar"}
        hydrated = self.graph.hydrate(data)
        assert isinstance(hydrated, dict)

    def test_can_open_browser(self):
        with patch("webbrowser.open") as mocked:
            self.graph.open_browser()
            assert mocked.called_once_with(self.graph.service_root.resource.uri.string)

    def test_graph_is_not_equal_to_non_graph(self):
        graph = Graph()
        assert graph != object()

    def test_can_get_neo4j_version(self):
        assert isinstance(self.graph.neo4j_version, tuple)

    def test_can_create_and_delete_node(self):
        a, = self.graph.create(Node())
        assert isinstance(a, Node)
        assert a.bound
        assert a.exists
        self.graph.delete(a)
        assert not a.exists

    def test_can_create_and_delete_relationship(self):
        ab, = self.graph.create(Relationship(Node(), "KNOWS", Node()))
        assert isinstance(ab, Relationship)
        assert ab.bound
        assert ab.exists
        self.graph.delete(ab, ab.start_node, ab.end_node)
        assert not ab.exists

    def test_can_get_node_by_id_when_cached(self):
        node, = self.graph.create({})
        assert node.uri in Node.cache
        got = self.graph.node(node._id)
        assert got is node

    def test_can_get_node_by_id_when_not_cached(self):
        node, = self.graph.create({})
        Node.cache.clear()
        assert node.uri not in Node.cache
        got = self.graph.node(node._id)
        assert got._id == node._id

    def test_node_cache_is_thread_local(self):
        import threading
        node, = self.graph.create({})
        assert node.uri in Node.cache
        other_cache_keys = []

        def check_cache():
            other_cache_keys.extend(Node.cache.keys())

        thread = threading.Thread(target=check_cache)
        thread.start()
        thread.join()

        assert node.uri in Node.cache
        assert node.uri not in other_cache_keys

    def test_can_get_node_by_id_even_when_id_does_not_exist(self):
        node, = self.graph.create({})
        node_id = node._id
        self.graph.delete(node)
        Node.cache.clear()
        node = self.graph.node(node_id)
        assert not node.exists


class NodeTestCase(DatabaseTestCase):

    def test_can_create_node(self):
        a = Node("Person", name="Alice", age=33)
        assert a.labels == {"Person"}
        assert a.properties == {"name": "Alice", "age": 33}

    def test_bound_node_equals_unbound_node_with_same_properties(self):
        alice_1 = Node(name="Alice")
        alice_1.bind("http://localhost:7474/db/data/node/1")
        alice_2 = Node(name="Alice")
        assert alice_1 == alice_2

    def test_bound_node_equality(self):
        alice_1 = Node(name="Alice")
        alice_1.bind("http://localhost:7474/db/data/node/1")
        Node.cache.clear()
        alice_2 = Node(name="Alice")
        alice_2.bind(alice_1.uri)
        assert alice_1 == alice_2

    def test_unbound_node_equality(self):
        alice_1 = Node("Person", name="Alice")
        alice_2 = Node("Person", name="Alice")
        assert alice_1 == alice_2

    def test_node_exists_will_raise_non_404_errors(self):
        with patch.object(_Resource, "get") as mocked:
            error = GraphError("bad stuff happened")
            error.response = DodgyClientError()
            mocked.side_effect = error
            alice = Node(name="Alice Smith")
            alice.bind("http://localhost:7474/db/data/node/1")
            with self.assertRaises(GraphError):
                _ = alice.exists
