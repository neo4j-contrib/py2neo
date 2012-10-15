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

import logging
import unittest

from py2neo import neo4j, subgraph

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    level=logging.DEBUG,
)


def default_graph_db():
    return neo4j.GraphDatabaseService("http://localhost:7474/db/data/")


class MergeTest(unittest.TestCase):

    def setUp(self):
        self.graph_db = default_graph_db()
        self.graph_db.clear()

    def test_can_merge_single_node(self):
        abstract = {
            "People": {
                "__nodes__": {
                    "alice": {"name": "Alice"}
                }
            }
        }
        nodes = subgraph.merge(abstract, self.graph_db)
        assert self.graph_db.get_node_count() == 1
        assert len(nodes) == 1
        assert "People" in nodes
        people = nodes["People"]
        assert len(people) == 1
        assert "alice" in people
        assert isinstance(people["alice"], neo4j.Node)

    def test_can_merge_subgraph(self):
        abstract = {
            "People": {
                "__nodes__": {
                    "alice": {"name": "Alice"},
                    "bob":   {"name": "Bob"}
                },
                "__rels__": [
                    ["alice", "KNOWS", "bob", {"since": 1999}]
                ]
            }
        }
        nodes = subgraph.merge(abstract, self.graph_db)
        assert self.graph_db.get_node_count() == 2
        assert len(nodes) == 1
        assert "People" in nodes
        people = nodes["People"]
        assert len(people) == 2
        assert "alice" in people
        assert "bob" in people
        alice, bob = people["alice"], people["bob"]
        assert isinstance(alice, neo4j.Node)
        assert isinstance(bob, neo4j.Node)
        assert alice.has_relationship_with(bob, neo4j.Direction.OUTGOING, "KNOWS")

    def test_merge_of_unique_node_is_idempotent(self):
        abstract = {
            "People": {
                "__uniquekey__": "email",
                "__nodes__": {
                    "alice": {"name": "Alice", "email": "alice@example.com"}
                }
            }
        }
        nodes = subgraph.merge(abstract, self.graph_db)
        alice = nodes["People"]["alice"]
        assert isinstance(alice, neo4j.Node)
        for i in range(10):
            nodes = subgraph.merge(abstract, self.graph_db)
            clone_of_alice = nodes["People"]["alice"]
            assert clone_of_alice == alice

    def test_can_merge_subgraph_with_single_unique_node(self):
        abstract = {
            "People": {
                "__uniquekey__": "email",
                "__nodes__": {
                    "alice": {"name": "Alice", "email": "alice@example.com"},
                    "cake":  {"flavour": "chocolate"}
                },
                "__rels__": [
                    ["alice", "LIKES", "cake"]
                ]
            }
        }
        nodes = subgraph.merge(abstract, self.graph_db)
        alice, cake = nodes["People"]["alice"], nodes["People"]["cake"]
        assert isinstance(alice, neo4j.Node)
        assert isinstance(cake, neo4j.Node)
        assert alice.has_relationship_with(cake, neo4j.Direction.OUTGOING, "LIKES")
        for i in range(10):
            nodes = subgraph.merge(abstract, self.graph_db)
            clone_of_alice, extra_cake = nodes["People"]["alice"], nodes["People"]["cake"]
            assert clone_of_alice == alice
            assert extra_cake == cake
            assert clone_of_alice.has_relationship_with(extra_cake, neo4j.Direction.OUTGOING, "LIKES")

    def test_can_merge_subgraph_with_multiple_unique_nodes(self):
        abstract = {
            "People": {
                "__uniquekey__": "email",
                "__nodes__": {
                    "alice": {"name": "Alice", "email": "alice@example.com"},
                    "bob":   {"name": "Bob", "email": "bob@example.com"},
                },
                "__rels__": [
                    ["alice", "KNOWS", "bob"]
                ]
            }
        }
        nodes = subgraph.merge(abstract, self.graph_db)
        alice, bob = nodes["People"]["alice"], nodes["People"]["bob"]
        assert isinstance(alice, neo4j.Node)
        assert isinstance(bob, neo4j.Node)
        assert alice.has_relationship_with(bob, neo4j.Direction.OUTGOING, "KNOWS")
        for i in range(10):
            nodes = subgraph.merge(abstract, self.graph_db)
            clone_of_alice, clone_of_bob = nodes["People"]["alice"], nodes["People"]["bob"]
            assert clone_of_alice == alice
            assert clone_of_bob == bob
            assert clone_of_alice.has_relationship_with(clone_of_bob, neo4j.Direction.OUTGOING, "KNOWS")

    def test_can_merge_subgraph_with_multiple_categories(self):
        abstract = {
            "People": {
                "__uniquekey__": "email",
                "__nodes__": {
                    "alice": {"name": "Alice", "email": "alice@example.com"}
                },
                "__rels__": [
                    ["alice", "LIKES", "Food:cake"],
                    ["alice", "DISLIKES", "Food:pie"]
                ]
            },
            "Food": {
                "__nodes__": {
                    "cake": {"flavour": "chocolate"},
                    "pie":  {"flavour": "steak & ale"}
                },
            }
        }
        nodes = subgraph.merge(abstract, self.graph_db)
        alice, cake, pie = nodes["People"]["alice"], nodes["Food"]["cake"], nodes["Food"]["pie"]
        assert isinstance(alice, neo4j.Node)
        assert isinstance(cake, neo4j.Node)
        assert isinstance(pie, neo4j.Node)
        assert alice.has_relationship_with(cake, neo4j.Direction.OUTGOING, "LIKES")
        assert alice.has_relationship_with(pie, neo4j.Direction.OUTGOING, "DISLIKES")
        for i in range(10):
            nodes = subgraph.merge(abstract, self.graph_db)
            clone_of_alice, extra_cake, more_pie = nodes["People"]["alice"], nodes["Food"]["cake"], nodes["Food"]["pie"]
            assert clone_of_alice == alice
            assert extra_cake == cake
            assert more_pie == pie
            assert clone_of_alice.has_relationship_with(extra_cake, neo4j.Direction.OUTGOING, "LIKES")
            assert clone_of_alice.has_relationship_with(more_pie, neo4j.Direction.OUTGOING, "DISLIKES")

    def test_can_merge_overlapping_subgraphs(self):
        abstract_cake = {
            "People": {
                "__uniquekey__": "email",
                "__nodes__": {
                    "alice": {"name": "Alice", "email": "alice@example.com"}
                },
                "__rels__": [
                    ["alice", "LIKES", "Food:cake"]
                ]
            },
            "Food": {
                "__nodes__": {
                    "cake": {"flavour": "chocolate"}
                },
            }
        }
        nodes = subgraph.merge(abstract_cake, self.graph_db)
        alice, cake = nodes["People"]["alice"], nodes["Food"]["cake"]
        assert isinstance(alice, neo4j.Node)
        assert isinstance(cake, neo4j.Node)
        assert alice.has_relationship_with(cake, neo4j.Direction.OUTGOING, "LIKES")
        abstract_pie = {
            "People": {
                "__uniquekey__": "email",
                "__nodes__": {
                    "alice": {"name": "Alice", "email": "alice@example.com"}
                },
                "__rels__": [
                    ["alice", "LIKES", "Food:pie"]
                ]
            },
            "Food": {
                "__nodes__": {
                    "pie":  {"flavour": "steak & ale"}
                },
            }
        }
        nodes = subgraph.merge(abstract_pie, self.graph_db)
        alice, pie = nodes["People"]["alice"], nodes["Food"]["pie"]
        assert isinstance(alice, neo4j.Node)
        assert isinstance(pie, neo4j.Node)
        assert alice.has_relationship_with(pie, neo4j.Direction.OUTGOING, "LIKES")
        likes = alice.get_relationships(neo4j.Direction.OUTGOING, "LIKES")
        assert len(likes) == 2
        for like in likes:
            assert like.end_node in [cake, pie]


if __name__ == '__main__':
    unittest.main()

