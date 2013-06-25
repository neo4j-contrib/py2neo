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

import unittest

from py2neo import geoff, neo4j


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

    def test_can_parse_comment_with_other_elements(self):
        nodes, rels, entries = parse('(A) /* this is a comment */ (B)')
        assert nodes == {
            "A": geoff.AbstractNode("A", {}),
            "B": geoff.AbstractNode("B", {}),
        }
        assert rels == []
        assert entries == {}

    def test_can_parse_node(self):
        nodes, rels, entries = parse('(A)')
        assert nodes == {"A": geoff.AbstractNode("A", {})}
        assert rels == []
        assert entries == {}

    def test_can_parse_node_with_data(self):
        nodes, rels, entries = parse('(A {"name": "Alice"})')
        assert nodes == {"A": geoff.AbstractNode("A", {"name": "Alice"})}
        assert rels == []
        assert entries == {}

    def test_can_parse_node_with_many_data_types(self):
        nodes, rels, entries = parse(
            '(A {"name":"Alice","age":34,"awesome":true,"lazy":false,'
            '"children":null,"fib":[1,1,2,3,5,8,13,21],"empty":[],'
            '"rainbow":["red","orange","yellow","green","blue","indigo","violet"]})'
        )
        assert nodes == {"A": geoff.AbstractNode("A", {
            "name": "Alice",
            "age": 34,
            "awesome": True,
            "lazy": False,
            "children": None,
            "fib": [1,1,2,3,5,8,13,21],
            "empty":[],
            "rainbow": ["red","orange","yellow","green","blue","indigo","violet"],
        })}
        assert rels == []
        assert entries == {}

    def test_can_parse_node_with_non_json_data(self):
        nodes, rels, entries = parse('(A {name: "Alice"})')
        assert nodes == {"A": geoff.AbstractNode("A", {"name": "Alice"})}
        assert rels == []
        assert entries == {}

    def test_can_parse_anonymous_node(self):
        nodes, rels, entries = parse('()')
        assert len(nodes) == 1
        for key, value in nodes.items():
            assert value == geoff.AbstractNode(None, {})
        assert rels == []
        assert entries == {}

    def test_can_parse_anonymous_node_with_data(self):
        nodes, rels, entries = parse('({"name": "Alice"})')
        assert len(nodes) == 1
        for key, value in nodes.items():
            assert value == geoff.AbstractNode(None, {"name": "Alice"})
        assert rels == []
        assert entries == {}

    def test_can_parse_anonymous_node_with_non_json_data(self):
        nodes, rels, entries = parse('({name: "Alice"})')
        assert len(nodes) == 1
        for key, value in nodes.items():
            assert value == geoff.AbstractNode(None, {"name": "Alice"})
        assert rels == []
        assert entries == {}

    def test_can_parse_node_plus_forward_path(self):
        nodes, rels, entries = parse('(A)-[:KNOWS]->(B)')
        assert nodes == {
            "A": geoff.AbstractNode("A", {}),
            "B": geoff.AbstractNode("B", {}),
        }
        assert len(rels) == 1
        assert isinstance(rels[0], geoff.AbstractRelationship)
        assert rels[0].start_node == nodes["A"]
        assert rels[0].type == "KNOWS"
        assert rels[0].end_node == nodes["B"]
        assert rels[0].properties == {}
        assert entries == {}

    def test_can_parse_node_plus_reverse_path(self):
        nodes, rels, entries = parse('(A)<-[:KNOWS]-(B)')
        assert nodes == {
            "A": geoff.AbstractNode("A", {}),
            "B": geoff.AbstractNode("B", {}),
        }
        assert len(rels) == 1
        assert isinstance(rels[0], geoff.AbstractRelationship)
        assert rels[0].start_node == nodes["B"]
        assert rels[0].type == "KNOWS"
        assert rels[0].end_node == nodes["A"]
        assert rels[0].properties == {}
        assert entries == {}

    def test_can_parse_longer_path(self):
        nodes, rels, entries = parse(
            '(A)-[:KNOWS]->'
            '(B)-[:KNOWS]->'
            '(C)<-[:KNOWS]-'
            '(D)'
        )
        assert nodes == {
            "A": geoff.AbstractNode("A", {}),
            "B": geoff.AbstractNode("B", {}),
            "C": geoff.AbstractNode("C", {}),
            "D": geoff.AbstractNode("D", {}),
        }
        assert rels == [
            geoff.AbstractRelationship(nodes["A"], "KNOWS", {}, nodes["B"]),
            geoff.AbstractRelationship(nodes["B"], "KNOWS", {}, nodes["C"]),
            geoff.AbstractRelationship(nodes["D"], "KNOWS", {}, nodes["C"]),
        ]
        assert entries == {}

    def test_can_parse_longer_path_and_data(self):
        nodes, rels, entries = parse(
            '(A {"name":"Alice","age":34})-[:KNOWS {since:1999}]->'
            '(B {"name":"Bob"})-[:KNOWS {friends:true}]->'
            '(C {"name":"Carol"})<-[:KNOWS]-'
            '(D {"name":"Dave"})'
        )
        assert nodes == {
            "A": geoff.AbstractNode("A", {"name": "Alice", "age": 34}),
            "B": geoff.AbstractNode("B", {"name": "Bob"}),
            "C": geoff.AbstractNode("C", {"name": "Carol"}),
            "D": geoff.AbstractNode("D", {"name": "Dave"}),
        }
        assert rels == [
            geoff.AbstractRelationship(nodes["A"], "KNOWS", {"since": 1999}, nodes["B"]),
            geoff.AbstractRelationship(nodes["B"], "KNOWS", {"friends": True}, nodes["C"]),
            geoff.AbstractRelationship(nodes["D"], "KNOWS", {}, nodes["C"]),
        ]
        assert entries == {}

    def test_can_parse_forward_index_entry(self):
        nodes, rels, entries = parse(
            '|People {"email":"alice@example.com"}|=>(A)'
        )
        assert nodes == {
            "A": geoff.AbstractNode("A", {}),
        }
        assert rels == []
        assert entries == {
            ('People', 'email', 'alice@example.com', 'A'):
                geoff.AbstractIndexEntry("People", "email", "alice@example.com", nodes["A"]),
        }

    def test_can_parse_reverse_index_entry(self):
        nodes, rels, entries = parse(
            '(A)<=|People {"email":"alice@example.com"}|'
        )
        assert nodes == {
            "A": geoff.AbstractNode("A", {}),
        }
        assert rels == []
        assert entries == {
            ('People', 'email', 'alice@example.com', 'A'):
                geoff.AbstractIndexEntry("People", "email", "alice@example.com", nodes["A"]),
        }


class LegacyParseTest(unittest.TestCase):

    def test_can_parse_node(self):
        nodes, rels, entries = parse('(A) {"name": "Alice"}')
        assert nodes == {
            "A": geoff.AbstractNode("A", {"name": "Alice"}),
        }
        assert rels == []
        assert entries == {}

    def test_can_parse_relationship(self):
        nodes, rels, entries = parse('(A)-[:KNOWS]->(B) {"since": 1999}')
        assert nodes == {
            "A": geoff.AbstractNode("A", {}),
            "B": geoff.AbstractNode("B", {}),
        }
        assert rels == [
            geoff.AbstractRelationship(nodes["A"], "KNOWS", {"since": 1999}, nodes["B"]),
        ]
        assert entries == {}

    def test_can_parse_index_entry(self):
        nodes, rels, entries = parse('(A)<=|People| {"email": "alice@example.com"}')
        assert nodes == {
            "A": geoff.AbstractNode("A", {}),
        }
        assert rels == []
        assert entries == {
            ('People', 'email', 'alice@example.com', 'A'):
                geoff.AbstractIndexEntry("People", "email", "alice@example.com", nodes["A"]),
        }


class MultiElementParseTest(unittest.TestCase):

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


class InsertTestCase(unittest.TestCase):

    def setUp(self):
        self.graph_db = neo4j.GraphDatabaseService()
        self.graph_db.clear()

    def test_stuff(self):
        source = r"""
        |People {"email":"bob@example.com"}|=>(b)
        |People {"email":"ernie@example.com"}|=>(e)
        |People {"email":"ernie@example.com"}|=>(e)
        (a {name:"Alice"})  (b) {"name":"Bob Robertson"}
        (a {age:43})-[:KNOWS]->(b)-[:KNOWS]->(c)<-[:LOVES {amount:"lots"}]-(d)
        (f {name:"Lonely Frank"})

        /* Alice and Bob got married twice */
        (a)-[:MARRIED {date:"1970-01-01"}]->(b)
        (a)-[:MARRIED {date:"2001-09-11"}]->(b)
        """
        s = geoff.Subgraph(source)
        print(s.nodes)
        print(s.relationships)
        print(s.index_entries)
        print(s._indexed_nodes)
        print(s._related_nodes)
        print(s._odd_nodes)
        for name, node in s.insert_into(self.graph_db).items():
            print(name, node)


if __name__ == '__main__':
    unittest.main()
