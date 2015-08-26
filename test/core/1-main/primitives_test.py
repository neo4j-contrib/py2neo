#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2015, Nigel Small
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


from unittest import TestCase, main

from py2neo.primitives import *


ALICE = Node("Person", "Employee", name="Alice")
BOB = Node("Person", "Employee", name="Bob")
CAROL = Node("Person", name="Carol")
DAVE = Node("Person", name="Dave")

ALICE_KNOWS_BOB = Relationship(ALICE, "KNOWS", BOB, since=1999)
ALICE_LIKES_CAROL = Relationship(ALICE, "LIKES", CAROL)
CAROL_DISLIKES_BOB = Relationship(CAROL, "DISLIKES", BOB)
CAROL_MARRIED_TO_DAVE = Relationship(CAROL, "MARRIED_TO", DAVE)
DAVE_WORKS_FOR_DAVE = Relationship(DAVE, "WORKS_FOR", DAVE)


class GraphViewTestCase(TestCase):

    def test_node(self):
        assert repr(ALICE).startswith("<Node")
        assert ALICE.labels == {"Person", "Employee"}
        assert ALICE.properties == {"name": "Alice"}
        assert ALICE.properties["name"] == "Alice"
        assert ALICE["name"] == "Alice"
        assert ALICE.order == 1
        assert ALICE.size == 0
        assert ALICE.labels == {"Person", "Employee"}
        assert ALICE.types == set()
        assert ALICE.property_keys == {"name"}
        assert set(ALICE.nodes) == {ALICE}

    def test_node_update(self):
        alice = Node()
        assert alice.labels == set()
        assert alice.property_keys == set()
        alice.labels.add("Person")
        alice["name"] = "Alice"
        alice["age"] = 33
        assert alice.labels == {"Person"}
        assert alice.property_keys == {"name", "age"}

    def test_relationship(self):
        assert repr(ALICE_KNOWS_BOB).startswith("<Relationship")
        assert ALICE_KNOWS_BOB.type == "KNOWS"
        assert ALICE_KNOWS_BOB.properties == {"since": 1999}
        assert ALICE_KNOWS_BOB.properties["since"] == 1999
        assert ALICE_KNOWS_BOB["since"] == 1999
        assert ALICE_KNOWS_BOB.order == 2
        assert ALICE_KNOWS_BOB.size == 1
        assert ALICE_KNOWS_BOB.labels == {"Person", "Employee"}
        assert ALICE_KNOWS_BOB.types == {"KNOWS"}
        assert ALICE_KNOWS_BOB.property_keys == {"name", "since"}
        assert ALICE_KNOWS_BOB.start_node == ALICE
        assert ALICE_KNOWS_BOB.end_node == BOB
        assert set(ALICE_KNOWS_BOB.nodes) == {ALICE, BOB}
        assert set(ALICE_KNOWS_BOB.relationships) == {ALICE_KNOWS_BOB}

    def test_relationship_loop(self):
        assert repr(DAVE_WORKS_FOR_DAVE).startswith("<Relationship")
        assert DAVE_WORKS_FOR_DAVE.order == 1
        assert DAVE_WORKS_FOR_DAVE.size == 1
        assert set(DAVE_WORKS_FOR_DAVE.nodes) == {DAVE}
        assert set(DAVE_WORKS_FOR_DAVE.relationships) == {DAVE_WORKS_FOR_DAVE}

    def test_graph_view(self):
        graph = (ALICE_KNOWS_BOB | ALICE_LIKES_CAROL | CAROL_DISLIKES_BOB |
                 CAROL_MARRIED_TO_DAVE | DAVE_WORKS_FOR_DAVE)
        assert repr(graph).startswith("<GraphView")
        assert graph.order == 4
        assert graph.size == 5
        assert graph.nodes == (ALICE | BOB | CAROL | DAVE).nodes
        assert graph.labels == {"Person", "Employee"}
        assert graph.types == {"KNOWS", "LIKES", "DISLIKES", "MARRIED_TO", "WORKS_FOR"}
        assert graph.property_keys == {"name", "since"}

    def test_graph_view_equality(self):
        graph_1 = (ALICE_KNOWS_BOB | ALICE_LIKES_CAROL | CAROL_DISLIKES_BOB |
                   CAROL_MARRIED_TO_DAVE | DAVE_WORKS_FOR_DAVE)
        graph_2 = (ALICE_KNOWS_BOB | ALICE_LIKES_CAROL | CAROL_DISLIKES_BOB |
                   CAROL_MARRIED_TO_DAVE | DAVE_WORKS_FOR_DAVE)
        assert graph_1 == graph_2

    def test_graph_union(self):
        graph_1 = (ALICE_KNOWS_BOB | ALICE_LIKES_CAROL | CAROL_DISLIKES_BOB)
        graph_2 = (CAROL_DISLIKES_BOB | CAROL_MARRIED_TO_DAVE | DAVE_WORKS_FOR_DAVE)
        graph = graph_1 | graph_2
        assert repr(graph).startswith("<GraphView")
        assert graph.order == 4
        assert graph.size == 5
        assert graph.nodes == (ALICE | BOB | CAROL | DAVE).nodes
        assert graph.labels == {"Person", "Employee"}
        assert graph.types == {"KNOWS", "LIKES", "DISLIKES", "MARRIED_TO", "WORKS_FOR"}
        assert graph.property_keys == {"name", "since"}

    def test_graph_intersection(self):
        graph_1 = (ALICE_KNOWS_BOB | ALICE_LIKES_CAROL | CAROL_DISLIKES_BOB)
        graph_2 = (CAROL_DISLIKES_BOB | CAROL_MARRIED_TO_DAVE | DAVE_WORKS_FOR_DAVE)
        graph = graph_1 & graph_2
        assert repr(graph).startswith("<GraphView")
        assert graph.order == 2
        assert graph.size == 1
        assert graph.nodes == (BOB | CAROL).nodes
        assert graph.labels == {"Person", "Employee"}
        assert graph.types == {"DISLIKES"}
        assert graph.property_keys == {"name"}

    def test_graph_difference(self):
        graph_1 = (ALICE_KNOWS_BOB | ALICE_LIKES_CAROL | CAROL_DISLIKES_BOB)
        graph_2 = (CAROL_DISLIKES_BOB | CAROL_MARRIED_TO_DAVE | DAVE_WORKS_FOR_DAVE)
        graph = graph_1 - graph_2
        assert repr(graph).startswith("<GraphView")
        assert graph.order == 3
        assert graph.size == 2
        assert graph.nodes == (ALICE | BOB | CAROL).nodes
        assert graph.labels == {"Person", "Employee"}
        assert graph.types == {"KNOWS", "LIKES"}
        assert graph.property_keys == {"name", "since"}

    def test_graph_symmetric_difference(self):
        graph_1 = (ALICE_KNOWS_BOB | ALICE_LIKES_CAROL | CAROL_DISLIKES_BOB)
        graph_2 = (CAROL_DISLIKES_BOB | CAROL_MARRIED_TO_DAVE | DAVE_WORKS_FOR_DAVE)
        graph = graph_1 ^ graph_2
        assert repr(graph).startswith("<GraphView")
        assert graph.order == 4
        assert graph.size == 4
        assert graph.nodes == (ALICE | BOB | CAROL | DAVE).nodes
        assert graph.relationships == (ALICE_KNOWS_BOB | ALICE_LIKES_CAROL |
                                       CAROL_MARRIED_TO_DAVE | DAVE_WORKS_FOR_DAVE)
        assert graph.labels == {"Person", "Employee"}
        assert graph.types == {"KNOWS", "LIKES", "MARRIED_TO", "WORKS_FOR"}
        assert graph.property_keys == {"name", "since"}


if __name__ == "__main__":
    main()
