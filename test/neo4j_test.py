#/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright 2011-2013, Nigel Small
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

import sys
PY3K = sys.version_info[0] >= 3

from httpstream import NetworkAddressError, SocketError, ClientError
from py2neo import neo4j, cypher

import logging
import unittest

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    level=logging.DEBUG,
)


def default_graph_db():
    return neo4j.GraphDatabaseService()

def recycle(*entities):
    for entity in entities:
        try:
            entity.delete()
        except Exception:
            pass


def test_wrong_host_will_fail():
    graph_db = neo4j.GraphDatabaseService("http://localtoast:7474/db/data/")
    try:
        graph_db.refresh()
    except NetworkAddressError:
        assert True
    else:
        assert False


def test_wrong_port_will_fail():
    graph_db = neo4j.GraphDatabaseService("http://localhost:7575/db/data/")
    try:
        graph_db.refresh()
    except SocketError:
        assert True
    else:
        assert False


def test_wrong_path_will_fail():
    graph_db = neo4j.GraphDatabaseService("http://localhost:7474/foo/bar/")
    try:
        graph_db.refresh()
    except ClientError:
        assert True
    else:
        assert False


class BadDatabaseURITest(unittest.TestCase):

    def test_no_trailing_slash(self):
        graph_db = neo4j.GraphDatabaseService("http://localhost:7474/db/data")
        self.assertEqual("http://localhost:7474/db/data/", graph_db.__uri__)

    def test_no_path(self):
        try:
            neo4j.GraphDatabaseService("http://localhost:7474")
            assert False
        except ValueError as err:
            sys.stderr.write(str(err) + "\n")
            assert True


    def test_root_path(self):
        try:
            neo4j.GraphDatabaseService("http://localhost:7474/")
            assert False
        except ValueError as err:
            sys.stderr.write(str(err) + "\n")
            assert True


class GraphDatabaseServiceTest(unittest.TestCase):

    def setUp(self):
        self.graph_db = default_graph_db()
        #print("Neo4j Version: {0}".format(repr(self.graph_db.neo4j_version)))
        #print("Node count: {0}".format(self.graph_db.order()))
        #print("Relationship count: {0}".format(self.graph_db.size()))

    def test_can_get_same_instance(self):
        graph_db_1 = neo4j.GraphDatabaseService.get_instance(neo4j.DEFAULT_URI)
        graph_db_2 = neo4j.GraphDatabaseService.get_instance(neo4j.DEFAULT_URI)
        assert graph_db_1 is graph_db_2

    def test_neo4j_version_format(self):
        version = self.graph_db.neo4j_version
        print(version)
        assert isinstance(version, tuple)
        assert len(version) == 4
        assert isinstance(version[0], int)
        assert isinstance(version[1], int)
        assert isinstance(version[2], int)

    def test_create_single_empty_node(self):
        a, = self.graph_db.create({})

    def test_get_node_by_id(self):
        a1, = self.graph_db.create({"foo": "bar"})
        a2 = self.graph_db.node(a1._id)
        self.assertEqual(a1, a2)

    def test_create_node_with_property_dict(self):
        node, = self.graph_db.create({"foo": "bar"})
        self.assertEqual("bar", node["foo"])

    def test_create_node_with_mixed_property_types(self):
        node, = self.graph_db.create(
            {"number": 13, "foo": "bar", "true": False, "fish": "chips"}
        )
        self.assertEqual(4, len(node.get_properties()))
        self.assertEqual("chips", node["fish"])
        self.assertEqual("bar", node["foo"])
        self.assertEqual(13, node["number"])
        self.assertEqual(False, node["true"])

    def test_create_node_with_null_properties(self):
        node, = self.graph_db.create({"foo": "bar", "no-foo": None})
        self.assertEqual("bar", node["foo"])
        self.assertEqual(None, node["no-foo"])

    def test_create_multiple_nodes(self):
        nodes = self.graph_db.create(
                {},
                {"foo": "bar"},
                {"number": 42, "foo": "baz", "true": True},
                {"fish": ["cod", "haddock", "plaice"], "number": 109}
        )
        self.assertEqual(4, len(nodes))
        self.assertEqual(0, len(nodes[0].get_properties()))
        self.assertEqual(1, len(nodes[1].get_properties()))
        self.assertEqual("bar", nodes[1]["foo"])
        self.assertEqual(3, len(nodes[2].get_properties()))
        self.assertEqual(42, nodes[2]["number"])
        self.assertEqual("baz", nodes[2]["foo"])
        self.assertEqual(True, nodes[2]["true"])
        self.assertEqual(2, len(nodes[3].get_properties()))
        self.assertEqual("cod", nodes[3]["fish"][0])
        self.assertEqual("haddock", nodes[3]["fish"][1])
        self.assertEqual("plaice", nodes[3]["fish"][2])
        self.assertEqual(109, nodes[3]["number"])

    def test_batch_get_properties(self):
        nodes = self.graph_db.create(
            {},
            {"foo": "bar"},
            {"number": 42, "foo": "baz", "true": True},
            {"fish": ["cod", "haddock", "plaice"], "number": 109}
        )
        props = self.graph_db.get_properties(*nodes)
        self.assertEqual(4, len(props))
        self.assertEqual(0, len(props[0]))
        self.assertEqual(1, len(props[1]))
        self.assertEqual("bar", props[1]["foo"])
        self.assertEqual(3, len(props[2]))
        self.assertEqual(42, props[2]["number"])
        self.assertEqual("baz", props[2]["foo"])
        self.assertEqual(True, props[2]["true"])
        self.assertEqual(2, len(props[3]))
        self.assertEqual("cod", props[3]["fish"][0])
        self.assertEqual("haddock", props[3]["fish"][1])
        self.assertEqual("plaice", props[3]["fish"][2])
        self.assertEqual(109, props[3]["number"])


class NewCreateTestCase(unittest.TestCase):

    def setUp(self):
        self.graph_db = default_graph_db()

    def test_can_create_single_node(self):
        results = self.graph_db.create(
            {"name": "Alice"}
        )
        self.assertIsNotNone(results)
        self.assertIsInstance(results, list)
        self.assertEqual(1, len(results))
        self.assertIsInstance(results[0], neo4j.Node)
        self.assertTrue("name" in results[0])
        self.assertEqual("Alice", results[0]["name"])

    def test_can_create_simple_graph(self):
        results = self.graph_db.create(
            {"name": "Alice"},
            {"name": "Bob"},
            (0, "KNOWS", 1)
        )
        self.assertIsNotNone(results)
        self.assertIsInstance(results, list)
        self.assertEqual(3, len(results))
        self.assertIsInstance(results[0], neo4j.Node)
        self.assertTrue("name" in results[0])
        self.assertEqual("Alice", results[0]["name"])
        self.assertIsInstance(results[1], neo4j.Node)
        self.assertTrue("name" in results[1])
        self.assertEqual("Bob", results[1]["name"])
        self.assertIsInstance(results[2], neo4j.Relationship)
        self.assertEqual("KNOWS", results[2].type)
        self.assertEqual(results[0], results[2].start_node)
        self.assertEqual(results[1], results[2].end_node)

    def test_can_create_simple_graph_with_rel_data(self):
        results = self.graph_db.create(
            {"name": "Alice"},
            {"name": "Bob"},
            (0, "KNOWS", 1, {"since": 1996})
        )
        self.assertIsNotNone(results)
        self.assertIsInstance(results, list)
        self.assertEqual(3, len(results))
        self.assertIsInstance(results[0], neo4j.Node)
        self.assertTrue("name" in results[0])
        self.assertEqual("Alice", results[0]["name"])
        self.assertIsInstance(results[1], neo4j.Node)
        self.assertTrue("name" in results[1])
        self.assertEqual("Bob", results[1]["name"])
        self.assertIsInstance(results[2], neo4j.Relationship)
        self.assertEqual("KNOWS", results[2].type)
        self.assertEqual(results[0], results[2].start_node)
        self.assertEqual(results[1], results[2].end_node)
        self.assertTrue("since" in results[2])
        self.assertEqual(1996, results[2]["since"])

    def test_can_create_graph_against_existing_node(self):
        ref_node, = self.graph_db.create({})
        results = self.graph_db.create(
            {"name": "Alice"},
            (ref_node, "PERSON", 0)
        )
        self.assertIsNotNone(results)
        self.assertIsInstance(results, list)
        self.assertEqual(2, len(results))
        self.assertIsInstance(results[0], neo4j.Node)
        self.assertTrue("name" in results[0])
        self.assertEqual("Alice", results[0]["name"])
        self.assertIsInstance(results[1], neo4j.Relationship)
        self.assertEqual("PERSON", results[1].type)
        self.assertEqual(ref_node, results[1].start_node)
        self.assertEqual(results[0], results[1].end_node)
        self.graph_db.delete(*results)
        ref_node.delete()

    def test_fails_on_bad_reference(self):
        self.assertRaises(Exception, self.graph_db.create,
            {"name": "Alice"},
            (0, "KNOWS", 1)
        )

    def test_can_create_big_graph(self):
        size = 2000
        nodes = [
            {"number": i}
            for i in range(size)
        ]
        results = self.graph_db.create(*nodes)
        self.assertIsNotNone(results)
        self.assertIsInstance(results, list)
        self.assertEqual(size, len(results))
        for i in range(size):
            self.assertIsInstance(results[i], neo4j.Node)

class MultipleNodeTestCase(unittest.TestCase):

    flintstones = [
        {"name": "Fred"},
        {"name": "Wilma"},
        {"name": "Barney"},
        {"name": "Betty"}
    ]

    def setUp(self):
        self.gdb = default_graph_db()
        self.ref_node, = self.gdb.create({})
        self.nodes = self.gdb.create(*self.flintstones)

    def test_is_created(self):
        self.assertIsNotNone(self.nodes)
        self.assertEqual(len(self.nodes), len(self.flintstones))

    def test_has_correct_properties(self):
        self.assertEqual([
            node.get_properties()
            for node in self.nodes
        ], self.flintstones)

    def test_create_relationships(self):
        rels = self.gdb.create(*[
            (self.ref_node, "FLINTSTONE", node)
            for node in self.nodes
        ])
        self.gdb.delete(*rels)
        self.assertEqual(len(self.nodes), len(rels))

    def tearDown(self):
        self.gdb.delete(*self.nodes)
        self.ref_node.delete()


class TestRelatedDelete(unittest.TestCase):

    def setUp(self):
        self.graph_db = default_graph_db()
        self.recycling = []

    def tearDown(self):
        recycle(*self.recycling)

    def test_can_delete_entire_subgraph(self):
        query = '''\
        CREATE (en {place: "England"}),
               (sc {place: "Scotland"}),
               (cy {place: "Wales"}),
               (fr {place: "France"}),
               (de {place: "Germany"}),
               (es {place: "Spain"}),
               (eng {lang: "English"}),
               (fre {lang: "French"}),
               (deu {lang: "German"}),
               (esp {lang: "Spanish"}),
               (A {name: "Alice"}),
               (A)-[:LIVES_IN]->(en),
               (A)-[:SPEAKS]->(eng),
               (B {name: "Bob"}),
               (B)-[:LIVES_IN]->(cy),
               (B)-[:SPEAKS]->(eng),
               (C {name: "Carlos"}),
               (C)-[:LIVES_IN]->(es),
               (C)-[:SPEAKS]->(esp),
               (D {name: "Dagmar"}),
               (D)-[:LIVES_IN]->(de),
               (D)-[:SPEAKS]->(deu),
               (E {name: "Elspeth"}),
               (E)-[:LIVES_IN]->(sc),
               (E)-[:SPEAKS]->(eng),
               (E)-[:SPEAKS]->(deu),
               (F {name: "François"}),
               (F)-[:LIVES_IN]->(fr),
               (F)-[:SPEAKS]->(eng),
               (F)-[:SPEAKS]->(fre),
               (G {name: "Gina"}),
               (G)-[:LIVES_IN]->(de),
               (G)-[:SPEAKS]->(eng),
               (G)-[:SPEAKS]->(fre),
               (G)-[:SPEAKS]->(deu),
               (G)-[:SPEAKS]->(esp),
               (H {name: "Hans"}),
               (H)-[:LIVES_IN]->(de),
               (H)-[:SPEAKS]->(deu)
        RETURN en, sc, cy, fr, de, es, eng, fre, deu, esp,
               A, B, C, D, E, F, G, H
        '''
        data, metadata = cypher.execute(self.graph_db, query)
        entities = data[0]
        for entity in entities:
            assert entity.exists()
        alice = entities[10]
        alice.delete_related()
        for entity in entities:
            assert not entity.exists()


if __name__ == '__main__':
    unittest.main()
