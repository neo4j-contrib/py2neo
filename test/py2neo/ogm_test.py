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

__author__    = "Nigel Small <nasmall@gmail.com>"
__copyright__ = "Copyright 2011-2012 Nigel Small"
__license__   = "Apache License, Version 2.0"

from py2neo import neo4j
import unittest


class Person(object):
    email = None
    name = None
    age = None
    def related(self, rel_type, cls):
        # default makes sure does not return None
        return []

class SaveTestCase(unittest.TestCase):
    pass


class LoadTestCase(unittest.TestCase):

    def setUp(self):
        neo4j.GraphDatabaseService().clear()

    def test_can_load_simple_object(self):
        graph_db = neo4j.GraphDatabaseService()
        graph_db.get_or_create_indexed_node("People", "email", "alice@example.com", {
            "email": "alice@example.com",
            "name": "Alice Allison",
            "age": 34,
        })
        class Person(object):
            email = None
            name = None
            age = None
        alice = graph_db.load(Person, "People", "email", "alice@example.com")
        assert isinstance(alice, Person)
        assert alice.email == "alice@example.com"
        assert alice.name == "Alice Allison"
        assert alice.age == 34

    def test_can_load_object_with_relationships(self):
        graph_db = neo4j.GraphDatabaseService()
        alice = graph_db.get_or_create_indexed_node("People", "email", "alice@example.com", {
            "email": "alice@example.com",
            "name": "Alice Allison",
            "age": 34,
        })
        p = alice.create_path("KNOWS", {"name": "Bob Robertson"})
        alice = graph_db.load(Person, "People", "email", "alice@example.com")
        print(dir(alice))
        assert isinstance(alice, Person)
        assert alice.email == "alice@example.com"
        assert alice.name == "Alice Allison"
        assert alice.age == 34
        bob, = alice.related("KNOWS", Person)
        print(dir(bob))
        assert isinstance(bob, Person)
        assert bob.name == "Bob Robertson"
        no_friends = bob.related("KNOWS", Person)
        assert no_friends == []


class LoadAllTestCase(unittest.TestCase):
    pass


if __name__ == '__main__':
    unittest.main()
