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

from py2neo import neo4j

#import logging
import unittest

#logging.basicConfig(
#    format="%(asctime)s %(levelname)s %(name)s %(message)s",
#    level=logging.DEBUG,
#)


class PathTestCase(unittest.TestCase):

    def test_can_create_path(self):
        path = neo4j.Path({"name": "Alice"}, "KNOWS", {"name": "Bob"})
        assert len(path) == 1
        assert path.nodes[0]["name"] == "Alice"
        assert path._relationships[0]._type == "KNOWS"
        assert path.nodes[-1]["name"] == "Bob"
        path = neo4j.Path.join(path, "KNOWS", {"name": "Carol"})
        assert len(path) == 2
        assert path.nodes[0]["name"] == "Alice"
        assert path._relationships[0]._type == "KNOWS"
        assert path.nodes[1]["name"] == "Bob"
        path = neo4j.Path.join({"name": "Zach"}, "KNOWS", path)
        assert len(path) == 3
        assert path.nodes[0]["name"] == "Zach"
        assert path._relationships[0]._type == "KNOWS"
        assert path.nodes[1]["name"] == "Alice"
        assert path._relationships[1]._type == "KNOWS"
        assert path.nodes[2]["name"] == "Bob"

    def test_can_slice_path(self):
        path = neo4j.Path({"name": "Alice"},
            "KNOWS", {"name": "Bob"},
            "KNOWS", {"name": "Carol"},
            "KNOWS", {"name": "Dave"},
            "KNOWS", {"name": "Eve"},
            "KNOWS", {"name": "Frank"},
        )
        assert len(path) == 5
        assert path[0] == neo4j.Path({"name": "Alice"}, "KNOWS", {"name": "Bob"})
        assert path[1] == neo4j.Path({"name": "Bob"}, "KNOWS", {"name": "Carol"})
        assert path[2] == neo4j.Path({"name": "Carol"}, "KNOWS", {"name": "Dave"})
        assert path[-1] == neo4j.Path({"name": "Eve"}, "KNOWS", {"name": "Frank"})
        assert path[0:2] == neo4j.Path({"name": "Alice"}, "KNOWS", {"name": "Bob"}, "KNOWS", {"name": "Carol"})
        assert path[3:5] == neo4j.Path({"name": "Dave"}, "KNOWS", {"name": "Eve"}, "KNOWS", {"name": "Frank"})
        assert path[:] == neo4j.Path({"name": "Alice"}, "KNOWS", {"name": "Bob"}, "KNOWS", {"name": "Carol"}, "KNOWS", {"name": "Dave"}, "KNOWS", {"name": "Eve"}, "KNOWS", {"name": "Frank"})

    def test_can_iterate_path(self):
        path = neo4j.Path({"name": "Alice"},
            "KNOWS", {"name": "Bob"},
            "KNOWS", {"name": "Carol"},
            "KNOWS", {"name": "Dave"},
            "KNOWS", {"name": "Eve"},
            "KNOWS", {"name": "Frank"},
        )
        assert list(iter(path)) == [
            ({'name': 'Alice'}, 'KNOWS', {'name': 'Bob'}),
            ({'name': 'Bob'}, 'KNOWS', {'name': 'Carol'}),
            ({'name': 'Carol'}, 'KNOWS', {'name': 'Dave'}),
            ({'name': 'Dave'}, 'KNOWS', {'name': 'Eve'}),
            ({'name': 'Eve'}, 'KNOWS', {'name': 'Frank'}),
        ]
        assert list(enumerate(path)) == [
            (0, ({'name': 'Alice'}, 'KNOWS', {'name': 'Bob'})),
            (1, ({'name': 'Bob'}, 'KNOWS', {'name': 'Carol'})),
            (2, ({'name': 'Carol'}, 'KNOWS', {'name': 'Dave'})),
            (3, ({'name': 'Dave'}, 'KNOWS', {'name': 'Eve'})),
            (4, ({'name': 'Eve'}, 'KNOWS', {'name': 'Frank'}))
        ]

    def test_can_join_paths(self):
        path1 = neo4j.Path({"name": "Alice"}, "KNOWS", {"name": "Bob"})
        path2 = neo4j.Path({"name": "Carol"}, "KNOWS", {"name": "Dave"})
        path = neo4j.Path.join(path1, "KNOWS", path2)
        assert list(iter(path)) == [
            ({'name': 'Alice'}, 'KNOWS', {'name': 'Bob'}),
            ({'name': 'Bob'}, 'KNOWS', {'name': 'Carol'}),
            ({'name': 'Carol'}, 'KNOWS', {'name': 'Dave'}),
        ]

    def test_path_representation(self):
        path = neo4j.Path({"name": "Alice"}, "KNOWS", {"name": "Bob"})
        #print(str(path))
        assert str(path) == '({"name":"Alice"})-[:"KNOWS"]->({"name":"Bob"})'
        #print(repr(path))
        assert repr(path) == (
            "Path(node(**{'name': 'Alice'}), "
            "('KNOWS', **{}), "
            "node(**{'name': 'Bob'}))"
        )


class CreatePathTestCase(unittest.TestCase):

    def test_can_create_path(self):
        graph_db = neo4j.GraphDatabaseService()
        path = neo4j.Path({"name": "Alice"}, "KNOWS", {"name": "Bob"})
        assert path.nodes[0] == {"name": "Alice"}
        assert path._relationships[0]._type == "KNOWS"
        assert path.nodes[1] == {"name": "Bob"}
        path = path.create(graph_db)
        assert isinstance(path.nodes[0], neo4j.Node)
        assert path.nodes[0]["name"] == "Alice"
        assert isinstance(path.relationships[0], neo4j.Relationship)
        assert path._relationships[0]._type == "KNOWS"
        assert isinstance(path.nodes[1], neo4j.Node)
        assert path.nodes[1]["name"] == "Bob"

    def test_can_create_path_with_rel_properties(self):
        graph_db = neo4j.GraphDatabaseService()
        path = neo4j.Path({"name": "Alice"}, ("KNOWS", {"since": 1999}), {"name": "Bob"})
        assert path.nodes[0] == {"name": "Alice"}
        assert path._relationships[0]._type == "KNOWS"
        assert path._relationships[0]._properties == {"since": 1999}
        assert path.nodes[1] == {"name": "Bob"}
        path = path.create(graph_db)
        assert isinstance(path.nodes[0], neo4j.Node)
        assert path.nodes[0]["name"] == "Alice"
        assert isinstance(path.relationships[0], neo4j.Relationship)
        assert path._relationships[0]._type == "KNOWS"
        assert path._relationships[0]._properties == {"since": 1999}
        assert isinstance(path.nodes[1], neo4j.Node)
        assert path.nodes[1]["name"] == "Bob"


class GetOrCreatePathTestCase(unittest.TestCase):

    def setUp(self):
        self.graph_db = neo4j.GraphDatabaseService()

    def test_can_create_single_path(self):
        start_node, = self.graph_db.create({})
        p1 = start_node.get_or_create_path(
            "YEAR",  {"number": 2000},
            "MONTH", {"number": 12, "name": "December"},
            "DAY",   {"number": 25},
        )
        #print(p1)
        self.assertIsInstance(p1, neo4j.Path)
        self.assertEqual(3, len(p1))
        self.assertEqual(start_node, p1.nodes[0])

    def test_can_create_overlapping_paths(self):
        start_node, = self.graph_db.create({})
        p1 = start_node.get_or_create_path(
            "YEAR",  {"number": 2000},
            "MONTH", {"number": 12, "name": "December"},
            "DAY",   {"number": 25, "name": "Christmas Day"},
        )
        self.assertIsInstance(p1, neo4j.Path)
        self.assertEqual(3, len(p1))
        self.assertEqual(start_node, p1.nodes[0])
        #print(p1)
        p2 = start_node.get_or_create_path(
            "YEAR",  {"number": 2000},
            "MONTH", {"number": 12, "name": "December"},
            "DAY",   {"number": 24, "name": "Christmas Eve"},
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
        #print(p2)
        p3 = start_node.get_or_create_path(
            "YEAR",  {"number": 2000},
            "MONTH", {"number": 11, "name": "November"},
            "DAY",   {"number": 5, "name": "Bonfire Night"},
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
        #print(p3)

    def test_can_use_none_for_nodes(self):
        start_node, = self.graph_db.create({})
        p1 = start_node.get_or_create_path(
            "YEAR",  {"number": 2000},
            "MONTH", {"number": 12, "name": "December"},
            "DAY",   {"number": 25},
        )
        p2 = start_node.get_or_create_path(
            "YEAR",  {"number": 2000},
            "MONTH", None,
            "DAY",   {"number": 25},
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
            "YEAR",  {"number": 2000},
            "MONTH", {"number": 12, "name": "December"},
            "DAY",   {"number": 25},
        )
        p2 = start_node.get_or_create_path(
            "YEAR",  {"number": 2000},
            "MONTH", p1.nodes[2],
            "DAY",   {"number": 25},
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
            "YEAR",  {"number": 2000},
            "MONTH", {"number": 12, "name": "December"},
            "DAY",   {"number": 25},
        )
        p2 = start_node.get_or_create_path(
            "YEAR",  {"number": 2000},
            "MONTH", p1.nodes[2]._id,
            "DAY",   {"number": 25},
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
            "YEAR",  {"number": 2000},
            "MONTH", {"number": 12, "name": "December"},
            "DAY",   {"number": 25},
        )
        events = self.graph_db.get_or_create_index(neo4j.Node, "EVENTS")
        events.remove("name", "Christmas")
        events.add("name", "Christmas", p1.nodes[3])
        p2 = start_node.get_or_create_path(
            "YEAR",  {"number": 2000},
            "MONTH", {"number": 12, "name": "December"},
            "DAY",   ("EVENTS", "name", "Christmas"),
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
