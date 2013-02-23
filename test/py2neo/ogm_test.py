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

from py2neo import neo4j, ogm
import unittest


class Person(object):

    def __init__(self, email=None, name=None, age=None):
        self.email = email
        self.name = name
        self.age = age


class ExampleCodeTestCase(unittest.TestCase):

    def setUp(self):
        neo4j.GraphDatabaseService().clear()

    def test_can_execute_example_code(self):

        from py2neo import neo4j, ogm

        class Person(object):

            def __init__(self, email=None, name=None, age=None):
                self.email = email
                self.name = name
                self.age = age

            def __str__(self):
                return self.name

        graph_db = neo4j.GraphDatabaseService()
        store = ogm.Store(graph_db)

        alice = Person("alice@example,com", "Alice", 34)
        store.save_unique(alice, "People", "email", alice.email)

        bob = Person("bob@example,org", "Bob", 66)
        carol = Person("carol@example,org", "Carol", 42)
        store.relate(alice, "LIKES", bob)
        store.relate(alice, "LIKES", carol)
        store.save(alice)

        friends = store.load_related(alice, "LIKES", Person)
        print("Alice likes {0}".format(" and ".join(str(f) for f in friends)))


class RelateTestCase(unittest.TestCase):
    pass


class SeparateTestCase(unittest.TestCase):
    pass


class LoadRelatedTestCase(unittest.TestCase):
    pass


class LoadTestCase(unittest.TestCase):
    pass


class LoadIndexedTestCase(unittest.TestCase):
    pass


class LoadUniqueTestCase(unittest.TestCase):

    def setUp(self):
        self.graph_db = neo4j.GraphDatabaseService()
        self.graph_db.clear()
        self.store = ogm.Store(self.graph_db)

    def test_can_load_simple_object(self):
        alice_node = self.graph_db.get_or_create_indexed_node("People", "email", "alice@example.com", {
            "email": "alice@example.com",
            "name": "Alice Allison",
            "age": 34,
        })
        alice = self.store.load_unique(Person, "People", "email", "alice@example.com")
        assert isinstance(alice, Person)
        assert hasattr(alice, "__node__")
        assert alice.__node__ == alice_node
        assert hasattr(alice, "__rel__")
        assert alice.__rel__ == {}
        assert alice.email == "alice@example.com"
        assert alice.name == "Alice Allison"
        assert alice.age == 34

    def test_can_load_object_with_relationships(self):
        alice_node = self.graph_db.get_or_create_indexed_node("People", "email", "alice@example.com", {
            "email": "alice@example.com",
            "name": "Alice Allison",
            "age": 34,
        })
        path = alice_node.create_path("LIKES", {"name": "Bob Robertson"})
        bob_node = path.nodes[1]
        alice = self.store.load_unique(Person, "People", "email", "alice@example.com")
        assert isinstance(alice, Person)
        assert hasattr(alice, "__node__")
        assert alice.__node__ == alice_node
        assert hasattr(alice, "__rel__")
        assert alice.__rel__ == {
            "LIKES": [({}, bob_node)],
        }
        assert alice.email == "alice@example.com"
        assert alice.name == "Alice Allison"
        assert alice.age == 34
        friends = self.store.load_related(alice, "LIKES", Person)
        assert isinstance(friends, list)
        assert len(friends) == 1
        friend = friends[0]
        assert isinstance(friend, Person)
        assert friend.__node__ == bob_node
        enemies = self.store.load_related(alice, "DISLIKES", Person)
        assert isinstance(enemies, list)
        assert len(enemies) == 0


class SaveTestCase(unittest.TestCase):
    pass


class SaveIndexedTestCase(unittest.TestCase):
    pass


class SaveUniqueTestCase(unittest.TestCase):

    def setUp(self):
        self.graph_db = neo4j.GraphDatabaseService()
        self.graph_db.clear()
        self.store = ogm.Store(self.graph_db)

    def test_can_save_simple_object(self):
        alice = Person("alice@example.com", "Alice Allison", 34)
        self.store.save_unique(alice, "People", "email", "alice@example.com")
        print(alice.__node__)

    def test_can_save_object_with_rels(self):
        alice = Person("alice@example.com", "Alice Allison", 34)
        bob_node, carol_node = self.graph_db.create(
            {"name": "Bob Robertson"},
            {"name": "Carol Carlsson"},
        )
        alice.__rel__ = {"KNOWS": [({}, bob_node)]}
        self.store.save_unique(alice, "People", "email", "alice@example.com")
        print(alice.__node__, bob_node, carol_node, alice.__node__.match())
        alice.__rel__ = {"KNOWS": [({}, bob_node), ({}, carol_node)]}
        self.store.save_unique(alice, "People", "email", "alice@example.com")
        print(alice.__node__, bob_node, carol_node, alice.__node__.match())


if __name__ == '__main__':
    unittest.main()
