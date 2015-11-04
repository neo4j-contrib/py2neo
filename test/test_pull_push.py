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


from py2neo.core import Node, Rel, Rev, Path
from test.util import Py2neoTestCase


class PullPushTestCase(Py2neoTestCase):
        
    def test_can_pull_node(self):
        local = Node()
        remote = Node("Person", name="Alice")
        self.graph.create(remote)
        assert local.labels == set()
        assert local.properties == {}
        local.bind(remote.uri)
        self.graph.pull(local)
        assert local.labels == remote.labels
        assert local.properties == remote.properties

    def test_can_graph_pull_node(self):
        local = Node()
        remote = Node("Person", name="Alice")
        self.graph.create(remote)
        assert local.labels == set()
        assert local.properties == {}
        local.bind(remote.uri)
        self.graph.pull(local)
        assert local.labels == remote.labels
        assert local.properties == remote.properties
        
    def test_can_push_node(self):
        local = Node("Person", name="Alice")
        remote = Node()
        self.graph.create(remote)
        assert remote.labels == set()
        assert remote.properties == {}
        local.bind(remote.uri)
        self.graph.push(local)
        self.graph.pull(remote)
        assert local.labels == remote.labels
        assert local.properties == remote.properties

    def test_can_graph_push_node(self):
        local = Node("Person", name="Alice")
        remote = Node()
        self.graph.create(remote)
        assert remote.labels == set()
        assert remote.properties == {}
        local.bind(remote.uri)
        self.graph.push(local)
        self.graph.pull(remote)
        assert local.labels == remote.labels
        assert local.properties == remote.properties
        
    def test_can_push_rel(self):
        local = Rel("KNOWS", since=1999)
        remote = Rel("KNOWS")
        self.graph.create({}, {}, (0, remote, 1))
        assert remote.properties == {}
        local.bind(remote.uri)
        self.graph.push(local)
        self.graph.pull(remote)
        assert local.properties == remote.properties
        
    def test_can_push_relationship(self):
        a, b, ab = self.graph.create({}, {}, (0, "KNOWS", 1))
        value = self.cypher.execute_one("MATCH ()-[ab:KNOWS]->() WHERE id(ab)={i} "
                                        "RETURN ab.since", i=ab._id)
        assert value is None
        ab["since"] = 1999
        self.graph.push(ab)
        value = self.cypher.execute_one("MATCH ()-[ab:KNOWS]->() WHERE id(ab)={i} "
                                        "RETURN ab.since", i=ab._id)
        assert value == 1999

    def test_can_pull_path(self):
        alice = Node(name="Alice")
        bob = Node(name="Bob")
        carol = Node(name="Carol")
        dave = Node(name="Dave")
        path = Path(alice, "LOVES", bob, Rev("HATES"), carol, "KNOWS", dave)
        self.graph.create(path)
        assert path[0].properties["amount"] is None
        assert path[1].properties["amount"] is None
        assert path[2].properties["since"] is None
        assert path[0].rel.properties["amount"] is None
        assert path[1].rel.properties["amount"] is None
        assert path[2].rel.properties["since"] is None
        statement = ("MATCH ()-[ab]->() WHERE id(ab)={ab} "
                     "MATCH ()-[bc]->() WHERE id(bc)={bc} "
                     "MATCH ()-[cd]->() WHERE id(cd)={cd} "
                     "SET ab.amount = 'lots', bc.amount = 'some', cd.since = 1999")
        parameters = {"ab": path[0]._id, "bc": path[1]._id, "cd": path[2]._id}
        self.cypher.run(statement, parameters)
        self.graph.pull(path)
        assert path[0].properties["amount"] == "lots"
        assert path[1].properties["amount"] == "some"
        assert path[2].properties["since"] == 1999
        assert path[0].rel.properties["amount"] == "lots"
        assert path[1].rel.properties["amount"] == "some"
        assert path[2].rel.properties["since"] == 1999
        
    def test_can_push_path(self):
        alice = Node(name="Alice")
        bob = Node(name="Bob")
        carol = Node(name="Carol")
        dave = Node(name="Dave")
        path = Path(alice, "LOVES", bob, Rev("HATES"), carol, "KNOWS", dave)
        self.graph.create(path)
        statement = ("MATCH ()-[ab]->() WHERE id(ab)={ab} "
                     "MATCH ()-[bc]->() WHERE id(bc)={bc} "
                     "MATCH ()-[cd]->() WHERE id(cd)={cd} "
                     "RETURN ab.amount, bc.amount, cd.since")
        parameters = {"ab": path[0]._id, "bc": path[1]._id, "cd": path[2]._id}
        path[0].properties["amount"] = "lots"
        path[1].properties["amount"] = "some"
        path[2].properties["since"] = 1999
        results = self.cypher.execute(statement, parameters)
        ab_amount, bc_amount, cd_since = results[0]
        assert ab_amount is None
        assert bc_amount is None
        assert cd_since is None
        self.graph.push(path)
        results = self.cypher.execute(statement, parameters)
        ab_amount, bc_amount, cd_since = results[0]
        assert ab_amount == "lots"
        assert bc_amount == "some"
        assert cd_since == 1999
