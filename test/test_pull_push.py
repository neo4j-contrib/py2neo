#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2017, Nigel Small
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


from py2neo.types import Node, Relationship, Path
from py2neo.http import HTTP
from test.util import GraphTestCase


class PullTestCase(GraphTestCase):

    def test_cannot_pull_non_graphy_object(self):
        with self.assertRaises(TypeError):
            self.graph.pull("this is not a graphy object")

    def test_can_graph_pull_node(self):
        alice_1 = Node()
        alice_2 = Node("Person", name="Alice")
        self.graph.create(alice_2)
        assert set(alice_1.labels) == set()
        assert dict(alice_1) == {}
        alice_1.graph = alice_2.graph
        alice_1.identity = alice_2.identity
        self.graph.pull(alice_1)
        assert set(alice_1.labels) == set(alice_2.labels)
        assert dict(alice_1) == dict(alice_2)

    def test_can_pull_path(self):
        alice = Node(name="Alice")
        bob = Node(name="Bob")
        carol = Node(name="Carol")
        dave = Node(name="Dave")
        path = Path(alice, "LOVES", bob, Relationship(carol, "HATES", bob), carol, "KNOWS", dave)
        self.graph.create(path)
        assert path[0]["amount"] is None
        assert path[1]["amount"] is None
        assert path[2]["since"] is None
        statement = ("MATCH ()-[ab]->() WHERE id(ab)={ab} "
                     "MATCH ()-[bc]->() WHERE id(bc)={bc} "
                     "MATCH ()-[cd]->() WHERE id(cd)={cd} "
                     "SET ab.amount = 'lots', bc.amount = 'some', cd.since = 1999")
        id_0 = path[0].identity
        id_1 = path[1].identity
        id_2 = path[2].identity
        parameters = {"ab": id_0, "bc": id_1, "cd": id_2}
        self.graph.run(statement, parameters)
        self.graph.pull(path)
        assert path[0]["amount"] == "lots"
        assert path[1]["amount"] == "some"
        assert path[2]["since"] == 1999

    def test_node_label_pull_scenarios(self):
        label_sets = [set(), {"Foo"}, {"Foo", "Bar"}, {"Spam"}]
        for old_labels in label_sets:
            for new_labels in label_sets:
                node = Node(*old_labels)
                self.graph.create(node)
                node_id = node.identity
                assert set(node.labels) == old_labels
                if old_labels:
                    remove_clause = "REMOVE a:%s" % ":".join(old_labels)
                else:
                    remove_clause = ""
                if new_labels:
                    set_clause = "SET a:%s" % ":".join(new_labels)
                else:
                    set_clause = ""
                if remove_clause or set_clause:
                    self.graph.run("MATCH (a) WHERE id(a)={x} %s %s" %
                                   (remove_clause, set_clause), x=node_id)
                    self.graph.pull(node)
                    assert set(node.labels) == new_labels, \
                        "Failed to pull new labels %r over old labels %r" % \
                        (new_labels, old_labels)

    def test_node_property_pull_scenarios(self):
        property_sets = [{}, {"name": "Alice"}, {"name": "Alice", "age": 33}, {"name": "Bob"}]
        for old_props in property_sets:
            for new_props in property_sets:
                node = Node(**old_props)
                self.graph.create(node)
                node_id = node.identity
                assert dict(node) == old_props
                self.graph.run("MATCH (a) WHERE id(a)={x} SET a={y}", x=node_id, y=new_props)
                self.graph.pull(node)
                assert dict(node) == new_props,\
                    "Failed to pull new properties %r over old properties %r" % \
                    (new_props, old_props)

    def test_relationship_property_pull_scenarios(self):
        property_sets = [{}, {"name": "Alice"}, {"name": "Alice", "age": 33}, {"name": "Bob"}]
        for old_props in property_sets:
            for new_props in property_sets:
                a = Node()
                b = Node()
                relationship = Relationship(a, "TO", b, **old_props)
                self.graph.create(relationship)
                relationship_id = relationship.identity
                assert dict(relationship) == old_props
                self.graph.run("MATCH ()-[r]->() WHERE id(r)={x} SET r={y}",
                               x=relationship_id, y=new_props)
                self.graph.pull(relationship)
                assert dict(relationship) == new_props, \
                    "Failed to pull new properties %r over old properties %r" % \
                    (new_props, old_props)


class PushTestCase(GraphTestCase):

    def test_cannot_push_non_graphy_object(self):
        with self.assertRaises(TypeError):
            self.graph.push("this is not a graphy object")

    def test_can_graph_push_node(self):
        alice_1 = Node("Person", name="Alice")
        alice_2 = Node()
        self.graph.create(alice_2)
        assert set(alice_2.labels) == set()
        assert dict(alice_2) == {}
        alice_1.graph = alice_2.graph
        alice_1.identity = alice_2.identity
        self.graph.push(alice_1)
        self.graph.pull(alice_2)
        assert set(alice_1.labels) == set(alice_2.labels)
        assert dict(alice_1) == dict(alice_2)

    def test_can_push_relationship(self):
        a = Node()
        b = Node()
        ab = Relationship(a, "KNOWS", b)
        self.graph.create(ab)
        value = self.graph.evaluate("MATCH ()-[ab:KNOWS]->() WHERE id(ab)={i} "
                                    "RETURN ab.since", i=ab.identity)
        assert value is None
        ab["since"] = 1999
        self.graph.push(ab)
        value = self.graph.evaluate("MATCH ()-[ab:KNOWS]->() WHERE id(ab)={i} "
                                    "RETURN ab.since", i=ab.identity)
        assert value == 1999

    def test_can_push_path(self):
        alice = Node(name="Alice")
        bob = Node(name="Bob")
        carol = Node(name="Carol")
        dave = Node(name="Dave")
        path = Path(alice, "LOVES", bob, Relationship(carol, "HATES", bob), carol, "KNOWS", dave)
        self.graph.create(path)
        statement = ("MATCH ()-[ab]->() WHERE id(ab)={ab} "
                     "MATCH ()-[bc]->() WHERE id(bc)={bc} "
                     "MATCH ()-[cd]->() WHERE id(cd)={cd} "
                     "RETURN ab.amount, bc.amount, cd.since")
        parameters = {"ab": path[0].identity, "bc": path[1].identity, "cd": path[2].identity}
        path[0]["amount"] = "lots"
        path[1]["amount"] = "some"
        path[2]["since"] = 1999
        ab_amount, bc_amount, cd_since = self.graph.run(statement, parameters).next()
        assert ab_amount is None
        assert bc_amount is None
        assert cd_since is None
        self.graph.push(path)
        ab_amount, bc_amount, cd_since = self.graph.run(statement, parameters).next()
        assert ab_amount == "lots"
        assert bc_amount == "some"
        assert cd_since == 1999

    def assert_has_labels(self, node_id, expected):
        actual = self.graph.evaluate("MATCH (_) WHERE id(_) = {x} return labels(_)", x=node_id)
        assert set(actual) == set(expected)

    def test_should_push_no_labels_onto_no_labels(self):
        node = Node()
        self.graph.create(node)
        node_id = node.identity
        self.assert_has_labels(node_id, {})
        self.graph.push(node)
        self.assert_has_labels(node_id, {})

    def test_should_push_no_labels_onto_one_label(self):
        node = Node("A")
        self.graph.create(node)
        node_id = node.identity
        self.assert_has_labels(node_id, {"A"})
        node.clear_labels()
        self.graph.push(node)
        self.assert_has_labels(node_id, {})

    def test_should_push_one_label_onto_no_labels(self):
        node = Node()
        self.graph.create(node)
        node_id = node.identity
        self.assert_has_labels(node_id, {})
        node.add_label("A")
        self.graph.push(node)
        self.assert_has_labels(node_id, {"A"})

    def test_should_push_one_label_onto_same_label(self):
        node = Node("A")
        self.graph.create(node)
        node_id = node.identity
        self.assert_has_labels(node_id, {"A"})
        self.graph.push(node)
        self.assert_has_labels(node_id, {"A"})

    def test_should_push_one_additional_label(self):
        node = Node("A")
        self.graph.create(node)
        node_id = node.identity
        self.assert_has_labels(node_id, {"A"})
        node.add_label("B")
        self.graph.push(node)
        self.assert_has_labels(node_id, {"A", "B"})

    def test_should_push_one_label_onto_different_label(self):
        node = Node("A")
        self.graph.create(node)
        node_id = node.identity
        self.assert_has_labels(node_id, {"A"})
        node.clear_labels()
        node.add_label("B")
        self.graph.push(node)
        self.assert_has_labels(node_id, {"B"})

    def test_should_push_multiple_labels_with_overlap(self):
        node = Node("A", "B")
        self.graph.create(node)
        node_id = node.identity
        self.assert_has_labels(node_id, {"A", "B"})
        node.remove_label("A")
        node.add_label("C")
        self.graph.push(node)
        self.assert_has_labels(node_id, {"B", "C"})
