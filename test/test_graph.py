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


from py2neo import Graph, Node, Relationship
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
        assert self.graph.exists(a)
        self.graph.delete(a)
        assert not self.graph.exists(a)

    def test_can_create_and_delete_relationship(self):
        ab, = self.graph.create(Relationship(Node(), "KNOWS", Node()))
        assert isinstance(ab, Relationship)
        assert ab.bound
        assert self.graph.exists(ab)
        self.graph.delete(ab, ab.start_node(), ab.end_node())
        assert not self.graph.exists(ab)

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
        a, = self.graph.create({})

    def test_get_node_by_id(self):
        a1, = self.graph.create({"foo": "bar"})
        a2 = self.graph.node(a1._id)
        assert a1 == a2

    def test_create_node_with_property_dict(self):
        node, = self.graph.create({"foo": "bar"})
        assert node["foo"] == "bar"

    def test_create_node_with_mixed_property_types(self):
        node, = self.graph.create(
            {"number": 13, "foo": "bar", "true": False, "fish": "chips"}
        )
        assert len(node) == 4
        assert node["fish"] == "chips"
        assert node["foo"] == "bar"
        assert node["number"] == 13
        assert not node["true"]

    def test_create_node_with_null_properties(self):
        node, = self.graph.create({"foo": "bar", "no-foo": None})
        assert node["foo"] == "bar"
        assert node["no-foo"] is None

    def test_create_multiple_nodes(self):
        nodes = self.graph.create(
                {},
                {"foo": "bar"},
                {"number": 42, "foo": "baz", "true": True},
                {"fish": ["cod", "haddock", "plaice"], "number": 109}
        )
        assert len(nodes) == 4
        assert len(nodes[0]) == 0
        assert len(nodes[1]) == 1
        assert nodes[1]["foo"] == "bar"
        assert len(nodes[2]) == 3
        assert nodes[2]["number"] == 42
        assert nodes[2]["foo"] == "baz"
        assert nodes[2]["true"]
        assert len(nodes[3]) == 2
        assert nodes[3]["fish"][0] == "cod"
        assert nodes[3]["fish"][1] == "haddock"
        assert nodes[3]["fish"][2] == "plaice"
        assert nodes[3]["number"] == 109

    def test_batch_pull_and_check_properties(self):
        nodes = self.graph.create(
            {},
            {"foo": "bar"},
            {"number": 42, "foo": "baz", "true": True},
            {"fish": ["cod", "haddock", "plaice"], "number": 109}
        )
        self.graph.pull(*nodes)
        props = [dict(n) for n in nodes]
        assert len(props) == 4
        assert len(props[0]) == 0
        assert len(props[1]) == 1
        assert props[1]["foo"] == "bar"
        assert len(props[2]) == 3
        assert props[2]["number"] == 42
        assert props[2]["foo"] == "baz"
        assert props[2]["true"]
        assert len(props[3]) == 2
        assert props[3]["fish"][0] == "cod"
        assert props[3]["fish"][1] == "haddock"
        assert props[3]["fish"][2] == "plaice"
        assert props[3]["number"] == 109
