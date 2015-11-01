#!/usr/bin/env python
# -*- encoding: utf-8 -*-

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


from py2neo import Node, Relationship
from py2neo.batch import PullBatch
from test.cases import DatabaseTestCase


class PullBatchTestCase(DatabaseTestCase):
    
    def setUp(self):
        self.batch = PullBatch(self.graph)
    
    def test_can_pull_node(self):
        uri = self.cypher.execute_one("CREATE (a {name:'Alice'}) RETURN a").uri
        alice = Node()
        alice.bind(uri)
        assert alice.properties["name"] is None
        self.batch.append(alice)
        self.batch.pull()
        assert alice.properties["name"] == "Alice"
        
    def test_can_pull_node_with_label(self):
        uri = self.cypher.execute_one("CREATE (a:Person {name:'Alice'}) RETURN a").uri
        alice = Node()
        alice.bind(uri)
        assert "Person" not in alice.labels
        assert alice.properties["name"] is None
        self.batch.append(alice)
        self.batch.pull()
        assert "Person" in alice.labels
        assert alice.properties["name"] == "Alice"
        
    def test_can_pull_relationship(self):
        uri = self.cypher.execute_one("CREATE ()-[ab:KNOWS {since:1999}]->() RETURN ab").uri
        ab = Relationship(None, "", None)
        ab.bind(uri)
        assert ab.type == ""
        assert ab.properties["since"] is None
        self.batch.append(ab)
        self.batch.pull()
        assert ab.type == "KNOWS"
        assert ab.properties["since"] == 1999
        
    def test_can_pull_rel(self):
        uri = self.cypher.execute_one("CREATE ()-[ab:KNOWS {since:1999}]->() RETURN ab").uri
        ab = Relationship(None, "", None).rel
        ab.bind(uri)
        assert ab.type == ""
        assert ab.properties["since"] is None
        self.batch.append(ab)
        self.batch.pull()
        assert ab.type == "KNOWS"
        assert ab.properties["since"] == 1999
        
    def test_can_pull_path(self):
        path = self.cypher.execute_one("CREATE p=()-[:KNOWS]->()-[:KNOWS]->() RETURN p")
        assert path.rels[0].properties["since"] is None
        statement = "MATCH ()-[ab]->() WHERE id(ab)={ab} SET ab.since=1999"
        self.cypher.execute(statement, {"ab": path.rels[0]._id})
        assert path.rels[0].properties["since"] is None
        self.batch.append(path)
        self.batch.pull()
        assert path.rels[0].properties["since"] == 1999
        
    def test_cannot_pull_none(self):
        try:
            self.batch.append(None)
        except TypeError:
            assert True
        else:
            assert False
