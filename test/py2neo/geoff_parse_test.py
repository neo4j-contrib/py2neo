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

    def test_can_parse_node_with_no_data(self):
        nodes, rels, entries = parse('(A)')
        assert nodes == {"A": geoff.AbstractNode("A", {})}
        assert rels == []
        assert entries == {}

    def test_can_parse_node_with_infix_data(self):
        nodes, rels, entries = parse('(A {"name": "Alice"})')
        assert nodes == {"A": geoff.AbstractNode("A", {"name": "Alice"})}
        assert rels == []
        assert entries == {}

    def test_can_parse_node_with_postfix_data(self):
        nodes, rels, entries = parse('(A) {"name": "Alice"}')
        assert nodes == {"A": geoff.AbstractNode("A", {"name": "Alice"})}
        assert rels == []
        assert entries == {}

    def test_can_parse_graph(self):
        nodes, rels, entries = parse('(A {"name": "Alice"}) '
                                     '(B {"name": "Bob"}) '
                                     '(A)-[:KNOWS]->(B) '
        )
        assert nodes == {
            "A": geoff.AbstractNode("A", {"name": "Alice"}),
            "B": geoff.AbstractNode("B", {"name": "Bob"}),
        }
        assert len(rels) == 1
        assert isinstance(rels[0], geoff.AbstractRelationship)
        assert rels[0].start_node == nodes["A"]
        assert rels[0].type == "KNOWS"
        assert rels[0].end_node == nodes["B"]
        assert rels[0].properties == {}
        assert entries == {}

    def test_can_parse_one_liner_graph(self):
        nodes, rels, entries = parse('(A {"name": "Alice"})-[:KNOWS]->(B {"name": "Bob"})')
        assert nodes == {
            "A": geoff.AbstractNode("A", {"name": "Alice"}),
            "B": geoff.AbstractNode("B", {"name": "Bob"}),
            }
        assert len(rels) == 1
        assert isinstance(rels[0], geoff.AbstractRelationship)
        assert rels[0].start_node == nodes["A"]
        assert rels[0].type == "KNOWS"
        assert rels[0].end_node == nodes["B"]
        assert rels[0].properties == {}
        assert entries == {}


if __name__ == '__main__':
    unittest.main()
