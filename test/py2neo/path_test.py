#/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright 2011-2012 Nigel Small
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

__author__    = "Nigel Small <nasmall@gmail.com>"
__copyright__ = "Copyright 2011-2012 Nigel Small"
__license__   = "Apache License, Version 2.0"

from py2neo import neo4j, cypher, rest

import logging
import unittest

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    level=logging.DEBUG,
)


class GetPathTestCase(unittest.TestCase):

    def test_can_get_path(self):
        graph_db = neo4j.GraphDatabaseService()
        alice, bob, alice_bob = graph_db.create({"name": "Alice"}, {"name": "Bob"}, (0, "KNOWS", 1))
        path = neo4j.Path(alice, "KNOWS", None)
        assert path.nodes[0] == alice
        assert path.relationships[0] == "KNOWS"
        assert path.nodes[1] is None
        path = path.get(graph_db)
        assert isinstance(path.nodes[0], neo4j.Node)
        assert path.nodes[0]["name"] == "Alice"
        assert isinstance(path.relationships[0], neo4j.Relationship)
        assert path.relationships[0].type == "KNOWS"
        assert isinstance(path.nodes[1], neo4j.Node)
        assert path.nodes[1]["name"] == "Bob"

    def test_can_get_path_including_node_properties(self):
        graph_db = neo4j.GraphDatabaseService()
        alice, bob, alice_bob = graph_db.create({"name": "Alice"}, {"name": "Bob"}, (0, "KNOWS", 1))
        path = neo4j.Path(alice, "KNOWS", {"name": "Bob"})
        assert path.nodes[0] == alice
        assert path.relationships[0] == "KNOWS"
        assert path.nodes[1] == {"name": "Bob"}
        path = path.get(graph_db)
        assert isinstance(path.nodes[0], neo4j.Node)
        assert path.nodes[0]["name"] == "Alice"
        assert isinstance(path.relationships[0], neo4j.Relationship)
        assert path.relationships[0].type == "KNOWS"
        assert isinstance(path.nodes[1], neo4j.Node)
        assert path.nodes[1]["name"] == "Bob"

#    def test_cannot_get_path_with_no_existing_nodes(self):
#        graph_db = neo4j.GraphDatabaseService()
#        path = neo4j.Path({"name": "Alice"}, "KNOWS", {"name": "Bob"})
#        assert path.nodes[0] == {"name": "Alice"}
#        assert path.relationships[0] == "KNOWS"
#        assert path.nodes[1] == {"name": "Bob"}
#        path = path.get(graph_db)
#        assert isinstance(path.nodes[0], neo4j.Node)
#        assert path.nodes[0]["name"] == "Alice"
#        assert isinstance(path.relationships[0], neo4j.Relationship)
#        assert path.relationships[0].type == "KNOWS"
#        assert isinstance(path.nodes[1], neo4j.Node)
#        assert path.nodes[1]["name"] == "Bob"


class CreatePathTestCase(unittest.TestCase):

    def test_can_create_path(self):
        graph_db = neo4j.GraphDatabaseService()
        path = neo4j.Path({"name": "Alice"}, "KNOWS", {"name": "Bob"})
        assert path.nodes[0] == {"name": "Alice"}
        assert path.relationships[0] == "KNOWS"
        assert path.nodes[1] == {"name": "Bob"}
        path = path.create(graph_db)
        assert isinstance(path.nodes[0], neo4j.Node)
        assert path.nodes[0]["name"] == "Alice"
        assert isinstance(path.relationships[0], neo4j.Relationship)
        assert path.relationships[0].type == "KNOWS"
        assert isinstance(path.nodes[1], neo4j.Node)
        assert path.nodes[1]["name"] == "Bob"

    def test_can_create_path_with_rel_properties(self):
        graph_db = neo4j.GraphDatabaseService()
        path = neo4j.Path({"name": "Alice"}, ("KNOWS", {"since": 1999}), {"name": "Bob"})
        assert path.nodes[0] == {"name": "Alice"}
        assert path.relationships[0] == ("KNOWS", {"since": 1999})
        assert path.nodes[1] == {"name": "Bob"}
        path = path.create(graph_db)
        assert isinstance(path.nodes[0], neo4j.Node)
        assert path.nodes[0]["name"] == "Alice"
        assert isinstance(path.relationships[0], neo4j.Relationship)
        assert path.relationships[0].type == "KNOWS"
        assert path.relationships[0]["since"] == 1999
        assert isinstance(path.nodes[1], neo4j.Node)
        assert path.nodes[1]["name"] == "Bob"


class GetOrCreatePathTestCase(unittest.TestCase):

    def setUp(self):
        self.graph_db = neo4j.GraphDatabaseService()

    def test_can_create_single_path(self):
        start_node, = self.graph_db.create({})
        p1 = start_node.get_or_create_path(
            ("YEAR",  {"number": 2000}),
            ("MONTH", {"number": 12, "name": "December"}),
            ("DAY",   {"number": 25}),
        )
        print p1
        self.assertIsInstance(p1, neo4j.Path)
        self.assertEqual(3, len(p1))
        self.assertEqual(start_node, p1.nodes[0])

    def test_can_create_overlapping_paths(self):
        start_node, = self.graph_db.create({})
        p1 = start_node.get_or_create_path(
            ("YEAR",  {"number": 2000}),
            ("MONTH", {"number": 12, "name": "December"}),
            ("DAY",   {"number": 25, "name": "Christmas Day"}),
        )
        self.assertIsInstance(p1, neo4j.Path)
        self.assertEqual(3, len(p1))
        self.assertEqual(start_node, p1.nodes[0])
        print p1
        p2 = start_node.get_or_create_path(
            ("YEAR",  {"number": 2000}),
            ("MONTH", {"number": 12, "name": "December"}),
            ("DAY",   {"number": 24, "name": "Christmas Eve"}),
        )
        self.assertIsInstance(p2, neo4j.Path)
        self.assertEqual(3, len(p2))
        self.assertEqual(p1.nodes[0], p2.nodes[0])
        self.assertEqual(p1.nodes[1], p2.nodes[1])
        self.assertEqual(p1.nodes[2], p2.nodes[2])
        self.assertNotEqual(p1.nodes[3], p2.nodes[3])
        self.assertEqual(p1.relationships[0], p2.relationships[0])
        self.assertEqual(p1.relationships[1], p2.relationships[1])
        self.assertNotEqual(p1.relationships[2], p2.relationships[2])
        print p2
        p3 = start_node.get_or_create_path(
            ("YEAR",  {"number": 2000}),
            ("MONTH", {"number": 11, "name": "November"}),
            ("DAY",   {"number": 5, "name": "Bonfire Night"}),
        )
        self.assertIsInstance(p3, neo4j.Path)
        self.assertEqual(3, len(p3))
        self.assertEqual(p2.nodes[0], p3.nodes[0])
        self.assertEqual(p2.nodes[1], p3.nodes[1])
        self.assertNotEqual(p2.nodes[2], p3.nodes[2])
        self.assertNotEqual(p2.nodes[3], p3.nodes[3])
        self.assertEqual(p2.relationships[0], p3.relationships[0])
        self.assertNotEqual(p2.relationships[1], p3.relationships[1])
        self.assertNotEqual(p2.relationships[2], p3.relationships[2])
        print p3

    def test_can_use_none_for_nodes(self):
        start_node, = self.graph_db.create({})
        p1 = start_node.get_or_create_path(
            ("YEAR",  {"number": 2000}),
            ("MONTH", {"number": 12, "name": "December"}),
            ("DAY",   {"number": 25}),
        )
        p2 = start_node.get_or_create_path(
            ("YEAR",  {"number": 2000}),
            ("MONTH", None),
            ("DAY",   {"number": 25}),
        )
        self.assertIsInstance(p2, neo4j.Path)
        self.assertEqual(3, len(p2))
        self.assertEqual(p1.nodes[0], p2.nodes[0])
        self.assertEqual(p1.nodes[1], p2.nodes[1])
        self.assertEqual(p1.nodes[2], p2.nodes[2])
        self.assertEqual(p1.nodes[3], p2.nodes[3])
        self.assertEqual(p1.relationships[0], p2.relationships[0])
        self.assertEqual(p1.relationships[1], p2.relationships[1])
        self.assertEqual(p1.relationships[2], p2.relationships[2])

    def test_can_use_node_for_nodes(self):
        start_node, = self.graph_db.create({})
        p1 = start_node.get_or_create_path(
            ("YEAR",  {"number": 2000}),
            ("MONTH", {"number": 12, "name": "December"}),
            ("DAY",   {"number": 25}),
        )
        p2 = start_node.get_or_create_path(
            ("YEAR",  {"number": 2000}),
            ("MONTH", p1.nodes[2]),
            ("DAY",   {"number": 25}),
        )
        self.assertIsInstance(p2, neo4j.Path)
        self.assertEqual(3, len(p2))
        self.assertEqual(p1.nodes[0], p2.nodes[0])
        self.assertEqual(p1.nodes[1], p2.nodes[1])
        self.assertEqual(p1.nodes[2], p2.nodes[2])
        self.assertEqual(p1.nodes[3], p2.nodes[3])
        self.assertEqual(p1.relationships[0], p2.relationships[0])
        self.assertEqual(p1.relationships[1], p2.relationships[1])
        self.assertEqual(p1.relationships[2], p2.relationships[2])

    def test_can_use_int_for_nodes(self):
        start_node, = self.graph_db.create({})
        p1 = start_node.get_or_create_path(
            ("YEAR",  {"number": 2000}),
            ("MONTH", {"number": 12, "name": "December"}),
            ("DAY",   {"number": 25}),
        )
        p2 = start_node.get_or_create_path(
            ("YEAR",  {"number": 2000}),
            ("MONTH", p1.nodes[2]._id),
            ("DAY",   {"number": 25}),
        )
        self.assertIsInstance(p2, neo4j.Path)
        self.assertEqual(3, len(p2))
        self.assertEqual(p1.nodes[0], p2.nodes[0])
        self.assertEqual(p1.nodes[1], p2.nodes[1])
        self.assertEqual(p1.nodes[2], p2.nodes[2])
        self.assertEqual(p1.nodes[3], p2.nodes[3])
        self.assertEqual(p1.relationships[0], p2.relationships[0])
        self.assertEqual(p1.relationships[1], p2.relationships[1])
        self.assertEqual(p1.relationships[2], p2.relationships[2])

    def test_can_use_tuple_for_nodes(self):
        start_node, = self.graph_db.create({})
        p1 = start_node.get_or_create_path(
            ("YEAR",  {"number": 2000}),
            ("MONTH", {"number": 12, "name": "December"}),
            ("DAY",   {"number": 25}),
        )
        events = self.graph_db.get_or_create_index(neo4j.Node, "EVENTS")
        events.remove("name", "Christmas")
        events.add("name", "Christmas", p1.nodes[3])
        p2 = start_node.get_or_create_path(
            ("YEAR",  {"number": 2000}),
            ("MONTH", {"number": 12, "name": "December"}),
            ("DAY",   ("EVENTS", "name", "Christmas")),
        )
        self.assertIsInstance(p2, neo4j.Path)
        self.assertEqual(3, len(p2))
        self.assertEqual(p1.nodes[0], p2.nodes[0])
        self.assertEqual(p1.nodes[1], p2.nodes[1])
        self.assertEqual(p1.nodes[2], p2.nodes[2])
        self.assertEqual(p1.nodes[3], p2.nodes[3])
        self.assertEqual(p1.relationships[0], p2.relationships[0])
        self.assertEqual(p1.relationships[1], p2.relationships[1])
        self.assertEqual(p1.relationships[2], p2.relationships[2])


if __name__ == '__main__':
    unittest.main()
