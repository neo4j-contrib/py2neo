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

__author__    = "Nigel Small <py2neo@nigelsmall.org>"
__copyright__ = "Copyright 2011-2012 Nigel Small"
__license__   = "Apache License, Version 2.0"

from py2neo import neo4j, geoff
import unittest


class ParseTest(unittest.TestCase):

    def test_parsing_empty_string(self):
        rules = geoff._parse('')
        self.assertEqual([], rules)

    def test_parsing_blank_lines(self):
        rules = geoff._parse('\n\n\n')
        self.assertEqual([], rules)

    def test_parsing_comment(self):
        rules = geoff._parse('# this is a comment')
        self.assertEqual([], rules)

    def test_parsing_single_node(self):
        rules = geoff._parse('(A)')
        self.assertEqual([(geoff.NODE, 'A', {})], rules)

    def test_parsing_single_node_with_data(self):
        rules = geoff._parse('(A) {"name": "Alice"}')
        self.assertEqual([(geoff.NODE, 'A', {'name': 'Alice'})], rules)

    def test_parsing_simple_graph(self):
        rules = geoff._parse(
            '(A) {"name": "Alice"}\n' \
            '(B) {"name": "Bob"}\n' \
            '(A)-[:KNOWS]->(B)\n'
        )
        self.assertEqual([
            (geoff.NODE, 'A', {'name': 'Alice'}),
            (geoff.NODE, 'B', {'name': 'Bob'}),
            (geoff.RELATIONSHIP, None, ('A', 'KNOWS', 'B', {})),
        ], rules)

    def test_parsing_graph_with_unknown_rules(self):
        rules = geoff._parse(
            '(A)<=|People| {"name": "Alice"}\n' \
            '(B)<=|People| {"name": "Bob"}\n' \
            '(A) {"name": "Alice Allison"}\n' \
            '(B) {"name": "Bob Robertson"}\n' \
            '(A)-[:KNOWS]->(B)\n'
        )
        self.assertEqual([
            (geoff.UNKNOWN, None, ('(A)<=|People|', {'name': 'Alice'})),
            (geoff.UNKNOWN, None, ('(B)<=|People|', {'name': 'Bob'})),
            (geoff.NODE, 'A', {'name': 'Alice Allison'}),
            (geoff.NODE, 'B', {'name': 'Bob Robertson'}),
            (geoff.RELATIONSHIP, None, ('A', 'KNOWS', 'B', {})),
        ], rules)


class SubgraphCreationTest(unittest.TestCase):

    def test_empty_subgraph_creation(self):
        s = geoff.Subgraph()
        self.assertEqual(0, len(s.nodes))
        self.assertEqual(0, len(s.relationships))

    def test_simple_subgraph_creation(self):
        s = geoff.Subgraph({"name": "Alice"}, {"name": "Bob"}, (0, "KNOWS", 1))
        self.assertEqual(2, len(s.nodes))
        self.assertEqual(1, len(s.relationships))

    def test_subgraph_creation_from_text(self):
        s = geoff.Subgraph(
            '(A) {"name": "Alice"}',
            '(B) {"name": "Bob"}',
            '(A)-[:KNOWS]->(B)'
        )
        self.assertEqual('(A) {"name": "Alice"}\n(B) {"name": "Bob"}\n(A)-[0:KNOWS]->(B) {}', s.dumps())

    def test_subgraph_creation_from_text_with_alternate_ordering(self):
        s = geoff.Subgraph(
            '(D) {"name": "Dave"}',
            '(B) {"name": "Bob"}',
            '(C) {"name": "Carol"}',
            '(C)-[:KNOWS]->(D)',
            '(A) {"name": "Alice"}',
            '(A)-[:KNOWS]->(B)',
        )
        self.assertEqual('(D) {"name": "Dave"}\n(B) {"name": "Bob"}\n' \
                         '(C) {"name": "Carol"}\n(C)-[0:KNOWS]->(D) {}\n' \
                         '(A) {"name": "Alice"}\n(A)-[1:KNOWS]->(B) {}', s.dumps())

    def test_subgraph_creation_from_db(self):
        graph_db = neo4j.GraphDatabaseService()
        a, b, ab = graph_db.create(
            {"name": "Alice"}, {"name": "Bob"}, (0, "KNOWS", 1)
        )
        s = geoff.Subgraph(a, b, ab)
        self.assertEqual('(0) {"name": "Alice"}\n(1) {"name": "Bob"}\n(0)-[0:KNOWS]->(1) {}', s.dumps())

class DumpTest(unittest.TestCase):

    def setUp(self):
        self.graph_db = neo4j.GraphDatabaseService()

    def test_node_dump(self):
        a, = self.graph_db.create(
            {"name": "Alice"}
        )
        out = geoff.Subgraph(a).dumps()
        self.assertEqual('(0) {"name": "Alice"}', out)

    def test_subgraph_dump(self):
        a, b, ab = self.graph_db.create(
            {"name": "Alice"}, {"name": "Bob"}, (0, "KNOWS", 1)
        )
        out = geoff.Subgraph(a, b, ab).dumps()
        self.assertEqual('(0) {"name": "Alice"}\n' \
                         '(1) {"name": "Bob"}\n' \
                         '(0)-[0:KNOWS]->(1) {}', out)


class InsertTest(unittest.TestCase):

    def setUp(self):
        self.graph_db = neo4j.GraphDatabaseService()

    def test_insert_from_abstract(self):
        s = geoff.Subgraph(
            {"name": "Alice"}, {"name": "Bob"}, (0, "KNOWS", 1)
        )
        params = s.insert_into(self.graph_db)
        self.assertIn("(0)", params)
        self.assertIn("(1)", params)
        self.assertIn("[0]", params)

    def test_insert_from_text(self):
        s = geoff.Subgraph(
            '(A) {"name": "Alice"}',
            '(B) {"name": "Bob"}',
            '(A)-[:KNOWS]->(B)'
        )
        params = s.insert_into(self.graph_db)
        self.assertIn("(A)", params)
        self.assertIn("(B)", params)
        self.assertIn("[0]", params)


class MergeTest(unittest.TestCase):

    def setUp(self):
        self.graph_db = neo4j.GraphDatabaseService()

    def test_merge_from_abstract(self):
        s = geoff.Subgraph(
            {"name": "Alice"}, {"name": "Bob"}, (0, "KNOWS", 1)
        )
        params = s.merge_into(self.graph_db)
        self.assertIn("(0)", params)
        self.assertIn("(1)", params)
        self.assertIn("[0]", params)

    def test_merge_from_text(self):
        s = geoff.Subgraph(
            '(A) {"name": "Alice"}',
            '(B) {"name": "Bob"}',
            '(A)-[:KNOWS]->(B)'
        )
        params = s.merge_into(self.graph_db)
        self.assertIn("(A)", params)
        self.assertIn("(B)", params)
        self.assertIn("[0]", params)


if __name__ == '__main__':
    unittest.main()

