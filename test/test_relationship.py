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


from py2neo import Graph, Node, Relationship, ServiceRoot
from py2neo.packages.httpstream import ClientError
from test.util import Py2neoTestCase

import sys
PY2 = sys.version_info < (3,)


class DodgyClientError(ClientError):
    status_code = 499


class RelationshipTestCase(Py2neoTestCase):

    def test_relationship_repr(self):
        r = Relationship(Node(), "TO", Node())
        assert repr(r)

    def test_bound_relationship_repr(self):
        _, _, r = self.graph.create({}, {}, (0, "KNOWS", 1))
        assert repr(r)

    def test_bound_relationship_repr_with_no_type(self):
        _, _, r = self.graph.create({}, {}, (0, "KNOWS", 1))
        r._type = None
        assert repr(r)

    def test_can_get_all_relationship_types(self):
        types = self.graph.relationship_types
        assert isinstance(types, frozenset)
        
    def test_can_get_relationship_by_id_when_cached(self):
        _, _, relationship = self.graph.create({}, {}, (0, "KNOWS", 1))
        got = self.graph.relationship(relationship._id)
        assert got is relationship
        
    def test_can_get_relationship_by_id_when_not_cached(self):
        _, _, relationship = self.graph.create({}, {}, (0, "KNOWS", 1))
        Relationship.cache.clear()
        got = self.graph.relationship(relationship._id)
        assert got._id == relationship._id
        
    def test_relationship_cache_is_thread_local(self):
        import threading
        _, _, relationship = self.graph.create({}, {}, (0, "KNOWS", 1))
        assert relationship.uri in Relationship.cache
        other_relationship_cache_keys = []
    
        def check_cache():
            other_relationship_cache_keys.extend(Relationship.cache.keys())
    
        thread = threading.Thread(target=check_cache)
        thread.start()
        thread.join()
    
        assert relationship.uri in Relationship.cache
        assert relationship.uri not in other_relationship_cache_keys
        
    def test_cannot_get_relationship_by_id_when_id_does_not_exist(self):
        _, _, relationship = self.graph.create({}, {}, (0, "KNOWS", 1))
        rel_id = relationship._id
        self.graph.delete(relationship)
        Relationship.cache.clear()
        with self.assertRaises(IndexError):
            _ = self.graph.relationship(rel_id)

    def test_getting_no_relationships(self):
        alice, = self.graph.create({"name": "Alice"})
        rels = list(self.graph.match(alice))
        assert rels is not None
        assert isinstance(rels, list)
        assert len(rels) == 0

    def test_get_relationship(self):
        alice, bob, ab = self.graph.create({"name": "Alice"}, {"name": "Bob"}, (0, "KNOWS", 1))
        rel = self.graph.relationship(ab._id)
        assert rel == ab

    def test_type_of_bound_rel_is_immutable(self):
        a, b, ab = self.graph.create({}, {}, (0, "KNOWS", 1))
        with self.assertRaises(AttributeError):
            ab.rel.type = "LIKES"

    def test_service_root(self):
        a, b, ab = self.graph.create({}, {}, (0, "KNOWS", 1))
        assert ab.service_root == ServiceRoot("http://localhost:7474/")

    def test_graph(self):
        a, b, ab = self.graph.create({}, {}, (0, "KNOWS", 1))
        assert ab.graph == Graph("http://localhost:7474/db/data/")

    def test_only_one_relationship_in_a_relationship(self):
        rel = Relationship({}, "KNOWS", {})
        assert rel.size() == 1

    def test_relationship_str(self):
        alice = Node("Person", name="Alice")
        bob = Node("Person", name="Bob")
        relationship = Relationship(alice, "KNOWS", bob)
        assert str(relationship) == '(:Person {name:"Alice"})-[:KNOWS]->(:Person {name:"Bob"})'
        self.graph.create(relationship)
        assert str(relationship) == \
            '(:Person {name:"Alice"})-[r%s:KNOWS]->(:Person {name:"Bob"})' % relationship._id
