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

__author__    = "Nigel Small <nasmall@gmail.com>"
__copyright__ = "Copyright 2011-2012 Nigel Small"
__license__   = "Apache License, Version 2.0"

from py2neo import neo4j, geoff
import unittest


def parse(source):
    return geoff._Parser(source).parse()

class ParseTest(unittest.TestCase):

    def test_can_parse_empty_string(self):
        nodes, rels, entries = parse('')
        assert nodes == {}
        assert rels == []
        assert entries == {}

    def test_can_parse_linear_whitespace(self):
        nodes, rels, entries = parse('  \t   \t ')
        assert nodes == {}
        assert rels == []
        assert entries == {}

    def test_can_parse_blank_lines(self):
        nodes, rels, entries = parse('\n\n\n')
        assert nodes == {}
        assert rels == []
        assert entries == {}

    def test_can_parse_comment(self):
        nodes, rels, entries = parse('/* this is a comment */')
        assert nodes == {}
        assert rels == []
        assert entries == {}

    def test_can_parse_single_node(self):
        nodes, rels, entries = parse('(A)')
        assert nodes == {"A": geoff.AbstractNode("A", {})}
        assert rels == []
        assert entries == {}

    def test_parsing_single_node_with_data(self):
        nodes, rels, entries = parse('(A) {"name": "Alice"}')
        self.assertEqual([(geoff.NODE, 'A', {'name': 'Alice'})], rules)

    def test_parsing_simple_graph(self):
        nodes, rels, entries = parse(
            '(A) {"name": "Alice"}\n'
            '(B) {"name": "Bob"}\n'
            '(A)-[:KNOWS]->(B)\n'
        )
        self.assertEqual([
            (geoff.NODE, 'A', {'name': 'Alice'}),
            (geoff.NODE, 'B', {'name': 'Bob'}),
            (geoff.RELATIONSHIP, None, ('A', 'KNOWS', 'B', {})),
            ], rules)

    def test_parsing_graph_with_unknown_rules(self):
        nodes, rels, entries = parse(
            '(A)<=|People| {"name": "Alice"}\n'
            '(B)<=|People| {"name": "Bob"}\n'
            '(A) {"name": "Alice Allison"}\n'
            '(B) {"name": "Bob Robertson"}\n'
            '(A)-[:KNOWS]->(B)\n'
        )
        self.assertEqual([
            (geoff.UNKNOWN, None, ('(A)<=|People|', {'name': 'Alice'})),
            (geoff.UNKNOWN, None, ('(B)<=|People|', {'name': 'Bob'})),
            (geoff.NODE, 'A', {'name': 'Alice Allison'}),
            (geoff.NODE, 'B', {'name': 'Bob Robertson'}),
            (geoff.RELATIONSHIP, None, ('A', 'KNOWS', 'B', {})),
            ], rules)


if __name__ == '__main__':
    unittest.main()
