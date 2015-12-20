#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2015, Nigel Small
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


from py2neo import Graph, Node, Relationship, cast_node
from test.util import Py2neoTestCase
from test.compat import patch, assert_repr


class GraphTestCase(Py2neoTestCase):

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
        num_rels = self.cypher.evaluate(statement)
        assert size == num_rels

    def test_graph_bool_returns_true(self):
        assert self.graph.__bool__()
        assert self.graph.__nonzero__()

    def test_can_hydrate_graph(self):
        data = self.graph.resource.get().content
        hydrated = self.graph.hydrate(data)
        assert hydrated is self.graph

    def test_graph_contains(self):
        node = Node()
        self.graph.create(node)
        assert node in self.graph

    def test_can_hydrate_map(self):
        data = {"foo": "bar"}
        hydrated = self.graph.hydrate(data)
        assert isinstance(hydrated, dict)

    def test_can_open_browser(self):
        with patch("webbrowser.open") as mocked:
            self.graph.open_browser()
            assert mocked.called_once_with(self.graph.dbms.resource.uri.string)

    def test_graph_is_not_equal_to_non_graph(self):
        graph = Graph()
        assert graph != object()

    def test_can_get_neo4j_version(self):
        assert isinstance(self.graph.neo4j_version, tuple)

    def test_can_create_and_delete_node(self):
        a = Node()
        self.graph.create(a)
        assert isinstance(a, Node)
        assert a.remote()
        assert self.graph.exists(a)
        self.graph.delete(a)
        assert not self.graph.exists(a)

    def test_can_create_and_delete_relationship(self):
        ab = Relationship(Node(), "KNOWS", Node())
        self.graph.create(ab)
        assert isinstance(ab, Relationship)
        assert ab.remote()
        assert self.graph.exists(ab)
        self.graph.delete(ab | ab.start_node() | ab.end_node())
        assert not self.graph.exists(ab)

    def test_can_get_node_by_id_when_cached(self):
        node = Node()
        self.graph.create(node)
        assert node.uri in Node.cache
        got = self.graph.node(node._id)
        assert got is node

    def test_can_get_node_by_id_when_not_cached(self):
        node = Node()
        self.graph.create(node)
        Node.cache.clear()
        assert node.uri not in Node.cache
        got = self.graph.node(node._id)
        assert got._id == node._id

    def test_node_cache_is_thread_local(self):
        import threading
        node = Node()
        self.graph.create(node)
        assert node.uri in Node.cache
        other_cache_keys = []

        def check_cache():
            other_cache_keys.extend(Node.cache.keys())

        thread = threading.Thread(target=check_cache)
        thread.start()
        thread.join()

        assert node.uri in Node.cache
        assert node.uri not in other_cache_keys

    def test_graph_hashes(self):
        assert hash(self.graph) == hash(self.graph)

    def test_graph_repr(self):
        assert_repr(self.graph, "<Graph uri='http://localhost:7474/db/data/'>",
                                "<Graph uri=u'http://localhost:7474/db/data/'>")

    def test_can_get_same_instance(self):
        graph_1 = Graph()
        graph_2 = Graph()
        assert graph_1 is graph_2

    def test_neo4j_version_format(self):
        version = self.graph.neo4j_version
        assert isinstance(version, tuple)
        assert 3 <= len(version) <= 4
        assert isinstance(version[0], int)
        assert isinstance(version[1], int)
        assert isinstance(version[2], int)

    def test_create_single_empty_node(self):
        a = Node()
        self.graph.create(a)
        assert a.remote()

    def test_get_node_by_id(self):
        a1 = Node(foo="bar")
        self.graph.create(a1)
        a2 = self.graph.node(a1._id)
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
