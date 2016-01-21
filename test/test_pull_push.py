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


from py2neo.types import Node, Relationship, Path
from test.util import Py2neoTestCase


class PullTestCase(Py2neoTestCase):

    def test_can_graph_pull_node(self):
        local = Node()
        remote = Node("Person", name="Alice")
        self.graph.create(remote)
        assert set(local.labels()) == set()
        assert dict(local) == {}
        local._set_resource(remote.resource.uri)
        self.graph.pull(local)
        assert set(local.labels()) == set(remote.labels())
        assert dict(local) == dict(remote)

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
        id_0 = path[0].resource._id
        id_1 = path[1].resource._id
        id_2 = path[2].resource._id
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
                node_id = node.resource._id
                assert set(node.labels()) == old_labels
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
                    assert set(node.labels()) == new_labels, \
                        "Failed to pull new labels %r over old labels %r" % \
                        (new_labels, old_labels)

    def test_node_property_pull_scenarios(self):
        property_sets = [{}, {"name": "Alice"}, {"name": "Alice", "age": 33}, {"name": "Bob"}]
        for old_props in property_sets:
            for new_props in property_sets:
                node = Node(**old_props)
                self.graph.create(node)
                node_id = node.resource._id
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
                relationship_id = relationship.resource._id
                assert dict(relationship) == old_props
                self.graph.run("MATCH ()-[r]->() WHERE id(r)={x} SET r={y}",
                               x=relationship_id, y=new_props)
                self.graph.pull(relationship)
                assert dict(relationship) == new_props, \
                    "Failed to pull new properties %r over old properties %r" % \
                    (new_props, old_props)


class PushTestCase(Py2neoTestCase):

    def test_can_graph_push_node(self):
        local = Node("Person", name="Alice")
        remote = Node()
        self.graph.create(remote)
        assert set(remote.labels()) == set()
        assert dict(remote) == {}
        local._set_resource(remote.resource.uri)
        self.graph.push(local)
        self.graph.pull(remote)
        assert set(local.labels()) == set(remote.labels())
        assert dict(local) == dict(remote)

    def test_can_push_relationship(self):
        a = Node()
        b = Node()
        ab = Relationship(a, "KNOWS", b)
        self.graph.create(ab)
        value = self.graph.evaluate("MATCH ()-[ab:KNOWS]->() WHERE id(ab)={i} "
                                    "RETURN ab.since", i=ab)
        assert value is None
        ab["since"] = 1999
        self.graph.push(ab)
        value = self.graph.evaluate("MATCH ()-[ab:KNOWS]->() WHERE id(ab)={i} "
                                    "RETURN ab.since", i=ab)
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
        parameters = {"ab": path[0], "bc": path[1], "cd": path[2]}
        path[0]["amount"] = "lots"
        path[1]["amount"] = "some"
        path[2]["since"] = 1999
        ab_amount, bc_amount, cd_since = self.graph.run(statement, parameters).select()
        assert ab_amount is None
        assert bc_amount is None
        assert cd_since is None
        self.graph.push(path)
        ab_amount, bc_amount, cd_since = self.graph.run(statement, parameters).select()
        assert ab_amount == "lots"
        assert bc_amount == "some"
        assert cd_since == 1999
