#/usr/bin/env python
# -*- coding: utf-8 -*-

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


from __future__ import unicode_literals

try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO
import sys
import unittest

from py2neo import geoff, neo4j


PLANETS_GEOFF = StringIO("""\
(node_1)
(node_2 {"day":88,"day unit":"days","density":6.03,"density unit":"gm/cm","distance":57.91,"distance unit":"km","mass":0.054,"mass unit":"(Earth=1)","name":"Mercury","radius":2340,"radius unit":"km","year":0.24085})
(node_3 {"day":230,"day unit":"days","density":5.11,"density unit":"gm/cm","distance":108.21,"distance unit":"km","mass":0.814,"mass unit":"(Earth=1)","name":"Venus","radius":6100,"radius unit":"km","year":0.61521})
(node_4 {"day":24,"day unit":"hours","density":5.517,"density unit":"gm/cm","distance":149.6,"distance unit":"km","mass":1.0,"mass unit":"(Earth=1)","name":"Earth","radius":6371,"radius unit":"km","year":1.000039})
(node_5)
(node_6 {"name":"Moon","orbit":27.322,"orbit unit":"days"})
(node_7 {"day":24.5,"day unit":"hours","density":4.16,"density unit":"gm/cm","distance":227.9,"distance unit":"km","mass":0.107,"mass unit":"(Earth=1)","name":"Mars","radius":3324,"radius unit":"km","year":1.88089})
(node_8)
(node_9 {"name":"Phobos","orbit":0.319,"orbit unit":"days"})
(node_10 {"name":"Deimos","orbit":1.262,"orbit unit":"days"})
(node_11 {"day":9.8,"day unit":"hours","density":1.34,"density unit":"gm/cm","distance":778.3,"distance unit":"km","mass":317.4,"mass unit":"(Earth=1)","name":"Jupiter","radius":69750,"radius unit":"km","year":11.8653})
(node_12)
(node_13 {"name":"Ganymede","orbit":7.155,"orbit unit":"days"})
(node_14 {"name":"Callisto","orbit":16.689,"orbit unit":"days"})
(node_15 {"day":10.2,"day unit":"hours","density":0.68,"density unit":"gm/cm","distance":1428,"distance unit":"km","mass":95.0,"mass unit":"(Earth=1)","name":"Saturn","radius":58170,"radius unit":"km","year":29.6501})
(node_16)
(node_17 {"name":"Titan","orbit":15.945,"orbit unit":"days"})
(node_18 {"name":"Rhea","orbit":4.518,"orbit unit":"days"})
(node_19 {"day":10.7,"day unit":"hours","density":1.55,"density unit":"gm/cm","distance":2872,"distance unit":"km","mass":14.5,"mass unit":"(Earth=1)","name":"Uranus","radius":23750,"radius unit":"km","year":83.7445})
(node_20)
(node_21 {"name":"Ariel","orbit":2.52,"orbit unit":"days"})
(node_22 {"name":"Miranda","orbit":1.414,"orbit unit":"days"})
(node_23 {"day":12.7,"day unit":"hours","density":2.23,"density unit":"gm/cm","distance":4498,"distance unit":"km","mass":17.6,"mass unit":"(Earth=1)","name":"Neptune","radius":22400,"radius unit":"km","year":165.951})
(node_24)
(node_25 {"name":"Triton","orbit":5.877,"orbit unit":"days"})
(node_1)-[:planet]->(node_2)
(node_1)-[:planet]->(node_3)
(node_1)-[:planet]->(node_4)
(node_4)-[:satellites]->(node_5)
(node_5)-[:satellite]->(node_6)
(node_1)-[:planet]->(node_7)
(node_7)-[:satellites]->(node_8)
(node_8)-[:satellite]->(node_9)
(node_8)-[:satellite]->(node_10)
(node_1)-[:planet]->(node_11)
(node_11)-[:satellites]->(node_12)
(node_12)-[:satellite]->(node_13)
(node_12)-[:satellite]->(node_14)
(node_1)-[:planet]->(node_15)
(node_15)-[:satellites]->(node_16)
(node_16)-[:satellite]->(node_17)
(node_16)-[:satellite]->(node_18)
(node_1)-[:planet]->(node_19)
(node_19)-[:satellites]->(node_20)
(node_20)-[:satellite]->(node_21)
(node_20)-[:satellite]->(node_22)
(node_1)-[:planet]->(node_23)
(node_23)-[:satellites]->(node_24)
(node_24)-[:satellite]->(node_25)""")

PLANETS_XML = StringIO("""\
<?xml version="1.0" encoding="UTF-8"?>
<planets>

  <planet>
    <name>Mercury</name>
    <distance unit="km">57.91</distance>
    <radius unit="km">2340</radius>
    <year>0.24085</year>
    <day unit="days">88</day>
    <mass unit="(Earth=1)">0.054</mass>
    <density unit="gm/cm">6.03</density>
  </planet>

  <planet>
    <name>Venus</name>
    <distance unit="km">108.21</distance>
    <radius unit="km">6100</radius>
    <year>0.61521</year>
    <day unit="days">230</day>
    <mass unit="(Earth=1)">0.814</mass>
    <density unit="gm/cm">5.11</density>
  </planet>

  <planet>
    <name>Earth</name>
    <distance unit="km">149.60</distance>
    <radius unit="km">6371</radius>
    <year>1.000039</year>
    <day unit="hours">24</day>
    <mass unit="(Earth=1)">1.00</mass>
    <density unit="gm/cm">5.517</density>
    <satellites>
      <satellite>
        <name>Moon</name>
        <orbit unit="days">27.322</orbit>
      </satellite>
    </satellites>
  </planet>

  <planet>
    <name>Mars</name>
    <distance unit="km">227.9</distance>
    <radius unit="km">3324</radius>
    <year>1.88089</year>
    <day unit="hours">24.5</day>
    <mass unit="(Earth=1)">0.107</mass>
    <density unit="gm/cm">4.16</density>
    <satellites>
      <satellite>
        <name>Phobos</name>
        <orbit unit="days">0.319</orbit>
      </satellite>
      <satellite>
        <name>Deimos</name>
        <orbit unit="days">1.262</orbit>
      </satellite>
    </satellites>
  </planet>


  <planet>
    <name>Jupiter</name>
    <distance unit="km">778.3</distance>
    <radius unit="km">69750</radius>
    <year>11.8653</year>
    <day unit="hours">9.8</day>
    <mass unit="(Earth=1)">317.4</mass>
    <density unit="gm/cm">1.34</density>
    <satellites>
      <satellite>
        <name>Ganymede</name>
        <orbit unit="days">7.155</orbit>
      </satellite>
      <satellite>
        <name>Callisto</name>
        <orbit unit="days">16.689</orbit>
      </satellite>
    </satellites>
  </planet>

  <planet>
    <name>Saturn</name>
    <distance unit="km">1428</distance>
    <radius unit="km">58170</radius>
    <year>29.6501</year>
    <day unit="hours">10.2</day>
    <mass unit="(Earth=1)">95.0</mass>
    <density unit="gm/cm">0.68</density>
    <satellites>
      <satellite>
        <name>Titan</name>
        <orbit unit="days">15.945</orbit>
      </satellite>
      <satellite>
        <name>Rhea</name>
        <orbit unit="days">4.518</orbit>
      </satellite>
    </satellites>
  </planet>

  <planet>
    <name>Uranus</name>
    <distance unit="km">2872</distance>
    <radius unit="km">23750</radius>
    <year>83.7445</year>
    <day unit="hours">10.7</day>
    <mass unit="(Earth=1)">14.5</mass>
    <density unit="gm/cm">1.55</density>
    <satellites>
      <satellite>
        <name>Ariel</name>
        <orbit unit="days">2.520</orbit>
      </satellite>
      <satellite>
        <name>Miranda</name>
        <orbit unit="days">1.414</orbit>
      </satellite>
    </satellites>
  </planet>

  <planet>
    <name>Neptune</name>
    <distance unit="km">4498</distance>
    <radius unit="km">22400</radius>
    <year>165.951</year>
    <day unit="hours">12.7</day>
    <mass unit="(Earth=1)">17.6</mass>
    <density unit="gm/cm">2.23</density>
    <satellites>
      <satellite>
        <name>Triton</name>
        <orbit unit="days">5.877</orbit>
      </satellite>
    </satellites>
  </planet>

</planets>
""")


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


def test_can_insert_empty_subgraph():
    graph_db = neo4j.GraphDatabaseService()
    source = ''
    subgraph = geoff.Subgraph(source)
    out = subgraph.insert_into(graph_db)
    assert out == {}


def test_can_insert_single_node():
    graph_db = neo4j.GraphDatabaseService()
    source = '(a {"name": "Alice"})'
    subgraph = geoff.Subgraph(source)
    out = subgraph.insert_into(graph_db)
    assert isinstance(out["a"], neo4j.Node)
    assert out["a"].get_properties() == {"name": "Alice"}
    matches = list(out["a"].match_outgoing())
    assert len(matches) == 0


def test_can_insert_simple_graph():
    graph_db = neo4j.GraphDatabaseService()
    source = '(a {"name": "Alice"}) (b {"name": "Bob"}) (a)-[:KNOWS]->(b)'
    subgraph = geoff.Subgraph(source)
    out = subgraph.insert_into(graph_db)
    assert isinstance(out["a"], neo4j.Node)
    assert isinstance(out["b"], neo4j.Node)
    assert out["a"].get_properties() == {"name": "Alice"}
    assert out["b"].get_properties() == {"name": "Bob"}
    matches = list(out["a"].match_outgoing(end_node=out["b"]))
    assert len(matches) == 1
    assert matches[0].type == "KNOWS"


def test_can_insert_reasonably_complex_graph():
    graph_db = neo4j.GraphDatabaseService()
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
    subgraph = geoff.Subgraph(source)
    out = subgraph.insert_into(graph_db)
    assert len(out) == 6
    assert all(isinstance(node, neo4j.Node) for node in out.values())
    assert out["a"].get_properties() == {"name": "Alice", "age": 43}
    assert out["b"].get_properties() == {"name": "Bob Robertson"}
    assert out["c"].get_properties() == {}
    assert out["d"].get_properties() == {}
    assert out["e"].get_properties() == {}
    assert out["f"].get_properties() == {"name": "Lonely Frank"}


def test_can_merge_simple_graph():
    graph_db = neo4j.GraphDatabaseService()
    source = '(a {"name": "Alice"}) (b {"name": "Bob"}) (a)-[:KNOWS]->(b)'
    subgraph = geoff.Subgraph(source)
    out = subgraph.merge_into(graph_db)
    assert isinstance(out["a"], neo4j.Node)
    assert isinstance(out["b"], neo4j.Node)
    assert out["a"].get_properties() == {"name": "Alice"}
    assert out["b"].get_properties() == {"name": "Bob"}
    matches = list(out["a"].match_outgoing(end_node=out["b"]))
    assert len(matches) == 1
    assert matches[0].type == "KNOWS"


def test_can_insert_subgraph_from_geoff_file():
    graph_db = neo4j.GraphDatabaseService()
    planets = geoff.Subgraph.load(PLANETS_GEOFF)
    assert len(planets.nodes) == 25
    assert len(planets.relationships) == 24
    assert len(planets.index_entries) == 0
    out = planets.insert_into(graph_db)
    assert len(out) == 25
    assert all(isinstance(node, neo4j.Node) for node in out.values())


def test_can_insert_subgraph_from_xml_file():
    graph_db = neo4j.GraphDatabaseService()
    planets = geoff.Subgraph.load_xml(PLANETS_XML)
    if sys.version_info >= (2, 7):
        assert planets.source == PLANETS_GEOFF.getvalue()
    assert len(planets.nodes) == 25
    assert len(planets.relationships) == 24
    assert len(planets.index_entries) == 0
    out = planets.insert_into(graph_db)
    assert len(out) == 25
    assert all(isinstance(node, neo4j.Node) for node in out.values())


def test_can_identify_non_unique_paths():
    graph_db = neo4j.GraphDatabaseService()
    graph_db.clear()
    source = """\
    |People {"email":"alice@example.com"}|=>(a)
    |People {"email":"bob@example.com"}|=>(b)
    (a {"name": "Alice"})
    (b {"name": "Bob"})
    (a)-[:KNOWS]->(b)
    (a)-[:KNOWS]->(b)
    """
    geoff.Subgraph(source).insert_into(graph_db)
    source = """\
    |People {"email":"alice@example.com"}|=>(a)
    |People {"email":"bob@example.com"}|=>(b)
    (a)-[:KNOWS]->(b)
    """
    try:
        geoff.Subgraph(source).merge_into(graph_db)
    except geoff.ConstraintViolation:
        assert True
    else:
        assert False


if __name__ == '__main__':
    unittest.main()
