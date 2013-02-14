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

from py2neo import neo4j, geoff


class SubgraphParsingTestCase(unittest.TestCase):

    def test_can_parse_simple_traditional_geoff(self):
        s = geoff.Subgraph("""\
            (A) {"name": "Alice"}
            (B) {"name": "Bob"}
            (A)-[:KNOWS]->(B)
        """)
        assert s.nodes == {
            'A': geoff.AbstractNode('A', {'name': 'Alice'}),
            'B': geoff.AbstractNode('B', {'name': 'Bob'}),
        }
        assert s.relationships[0].type == "KNOWS"
        assert s.relationships[0].properties == {}
        assert s.relationships[0].start_node == s.nodes["A"]
        assert s.relationships[0].end_node == s.nodes["B"]
        assert s.index_entries == {}
        assert s.indexed_nodes == {}
        assert s.related_nodes == {
            'A': geoff.AbstractNode('A', {'name': 'Alice'}),
            'B': geoff.AbstractNode('B', {'name': 'Bob'}),
        }
        assert s.odd_nodes == {}

    def test_can_parse_simple_new_geoff(self):
        s = geoff.Subgraph("""\
            (A {"name": "Alice"})
            (B {"name": "Bob"})
            (A)-[:KNOWS]->(B)
        """)
        assert s.nodes == {
            'A': geoff.AbstractNode('A', {'name': 'Alice'}),
            'B': geoff.AbstractNode('B', {'name': 'Bob'}),
        }
        assert s.relationships[0].type == "KNOWS"
        assert s.relationships[0].properties == {}
        assert s.relationships[0].start_node == s.nodes["A"]
        assert s.relationships[0].end_node == s.nodes["B"]
        assert s.index_entries == {}
        assert s.indexed_nodes == {}
        assert s.related_nodes == {
            'A': geoff.AbstractNode('A', {'name': 'Alice'}),
            'B': geoff.AbstractNode('B', {'name': 'Bob'}),
        }
        assert s.odd_nodes == {}

    def test_can_parse_simple_new_geoff_one_liner(self):
        s = geoff.Subgraph("""\
            (A {"name": "Alice"})-[:KNOWS]->(B {"name": "Bob"})
        """)
        assert s.nodes == {
            'A': geoff.AbstractNode('A', {'name': 'Alice'}),
            'B': geoff.AbstractNode('B', {'name': 'Bob'}),
        }
        assert s.relationships[0].type == "KNOWS"
        assert s.relationships[0].properties == {}
        assert s.relationships[0].start_node == s.nodes["A"]
        assert s.relationships[0].end_node == s.nodes["B"]
        assert s.index_entries == {}
        assert s.indexed_nodes == {}
        assert s.related_nodes == {
            'A': geoff.AbstractNode('A', {'name': 'Alice'}),
            'B': geoff.AbstractNode('B', {'name': 'Bob'}),
        }
        assert s.odd_nodes == {}

    def test_can_parse_simple_new_geoff_with_comment(self):
        s = geoff.Subgraph("""\
            /* Alice knows Bob */
            (A {"name": "Alice"})-[:KNOWS]->(B {"name": "Bob"})
        """)
        assert s.nodes == {
            'A': geoff.AbstractNode('A', {'name': 'Alice'}),
            'B': geoff.AbstractNode('B', {'name': 'Bob'}),
        }
        assert s.relationships[0].type == "KNOWS"
        assert s.relationships[0].properties == {}
        assert s.relationships[0].start_node == s.nodes["A"]
        assert s.relationships[0].end_node == s.nodes["B"]
        assert s.index_entries == {}
        assert s.indexed_nodes == {}
        assert s.related_nodes == {
            'A': geoff.AbstractNode('A', {'name': 'Alice'}),
            'B': geoff.AbstractNode('B', {'name': 'Bob'}),
        }
        assert s.odd_nodes == {}

#    def test_can_parse_geoff_with_index_entries(self):
#        s = geoff2.Subgraph("""\
#            |People {"email": "alice@example.com"}|=>(A)
#            |People {"email": "bob@example.org"}|=>(B)
#            (A {"name": "Alice"})-[:KNOWS]->(B {"name": "Bob"})
#        """)
#        assert s.nodes == {
#            'A': geoff2.AbstractNode('A', {'name': 'Alice'}),
#            'B': geoff2.AbstractNode('B', {'name': 'Bob'}),
#        }
#        assert s.relationships[0].type == "KNOWS"
#        assert s.relationships[0].properties == {}
#        assert s.relationships[0].start_node == s.nodes["A"]
#        assert s.relationships[0].end_node == s.nodes["B"]
#        print(s.index_entries)
#        assert s.index_entries == {}
#        assert s.indexed_nodes == {}
#        assert s.related_nodes == {
#            'A': geoff2.AbstractNode('A', {'name': 'Alice'}),
#            'B': geoff2.AbstractNode('B', {'name': 'Bob'}),
#        }
#        assert s.indexed_related_nodes == {}
#        assert s.unindexed_unrelated_nodes == {}

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
        print(s.indexed_nodes)
        print(s.related_nodes)
        print(s.odd_nodes)
        for name, node in s.insert_into(neo4j.GraphDatabaseService()).items():
            print(name, node)

if __name__ == "__main__":
    unittest.main()
